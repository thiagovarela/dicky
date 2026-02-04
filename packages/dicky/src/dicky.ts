import type {
  AddToRegistry,
  ArgsOf,
  DLQEntry,
  DickyConfig,
  InvokeOptions,
  Invocation,
  InvocationId,
  JournalEntry,
  Metrics,
  ObjectDef,
  Registry,
  RegisteredService,
  ResolvedConfig,
  ReturnOf,
  SendOptions,
  ServiceDef,
} from "./types";
import type { AwakeableStore } from "./stores/awakeable";
import { AwakeableStoreImpl } from "./stores/awakeable";
import type { DLQStore } from "./stores/dlq";
import { DLQStoreImpl } from "./stores/dlq";
import type { JournalStore } from "./stores/journal";
import { JournalStoreImpl } from "./stores/journal";
import { LockManagerImpl } from "./stores/lock";
import { createRedisClient } from "./stores/redis";
import type { RedisClient } from "./stores/redis";
import { StateStoreImpl } from "./stores/state";
import type { StreamProducer } from "./stores/stream";
import { StreamProducerImpl } from "./stores/stream";
import { TimerStoreImpl } from "./stores/timer";
import type { Worker } from "./worker";
import { WorkerImpl } from "./worker";
import type { Logger } from "./workerLogger";
import { LuaScriptsImpl } from "./lua";
import { resolveConfig } from "./config";
import { keys } from "./utils";
import { TimeoutError } from "./errors";

export class Dicky<R extends Registry = {}> {
  private services = new Map<string, RegisteredService>();
  private config: ResolvedConfig;
  private started = false;
  private worker: Worker | null = null;
  private redis: RedisClient | null = null;
  private blockingRedis: RedisClient | null = null;
  private streamProducer: StreamProducer | null = null;
  private awakeableStore: AwakeableStore | null = null;
  private journal: JournalStore | null = null;
  private dlq: DLQStore | null = null;
  private logger: Logger;

  constructor(config: DickyConfig) {
    this.config = resolveConfig(config);
    this.logger = createLogger();
  }

  use<T extends ServiceDef | ObjectDef>(def: T): Dicky<AddToRegistry<R, T>> {
    if (this.started) {
      throw new Error("Cannot register services after start()");
    }
    if (this.services.has(def.name)) {
      throw new Error(`Service "${def.name}" already registered`);
    }

    if (def.__kind === "service") {
      this.services.set(def.name, {
        kind: "service",
        name: def.name,
        handlers: def.handlers,
      });
    } else {
      this.services.set(def.name, {
        kind: "object",
        name: def.name,
        handlers: def.handlers,
        initialState: def.initial,
      });
    }

    return this as unknown as Dicky<AddToRegistry<R, T>>;
  }

  getService(name: string): RegisteredService | undefined {
    return this.services.get(name);
  }

  async send<
    S extends (keyof R & string) | (string & {}),
    H extends S extends keyof R ? (keyof R[S]["handlers"] & string) | (string & {}) : string,
  >(
    service: S,
    handler: H,
    args: S extends keyof R
      ? H extends keyof R[S]["handlers"]
        ? ArgsOf<R[S]["handlers"][H]>
        : unknown
      : unknown,
    opts?: SendOptions,
  ): Promise<InvocationId> {
    this.ensureStarted();

    const serviceDef = this.services.get(service as string);
    if (!serviceDef) {
      throw new Error(`Unknown service: ${service}`);
    }

    if (!(handler in serviceDef.handlers)) {
      throw new Error(`Unknown handler: ${service}.${handler}`);
    }

    if (serviceDef.kind === "object" && !opts?.key) {
      throw new Error(`Object handler requires key: ${service}.${handler}`);
    }

    return this.streamProducer!.dispatch(service as string, handler as string, args, {
      ...(opts?.key ? { key: opts.key } : {}),
      ...(opts?.delay ? { delay: opts.delay } : {}),
      ...(opts?.maxRetries != null ? { maxRetries: opts.maxRetries } : {}),
      ...(opts?.idempotencyKey ? { idempotencyKey: opts.idempotencyKey } : {}),
    });
  }

  async invoke<
    S extends (keyof R & string) | (string & {}),
    H extends S extends keyof R ? (keyof R[S]["handlers"] & string) | (string & {}) : string,
  >(
    service: S,
    handler: H,
    args: S extends keyof R
      ? H extends keyof R[S]["handlers"]
        ? ArgsOf<R[S]["handlers"][H]>
        : unknown
      : unknown,
    opts?: InvokeOptions,
  ): Promise<
    S extends keyof R
      ? H extends keyof R[S]["handlers"]
        ? ReturnOf<R[S]["handlers"][H]>
        : unknown
      : unknown
  > {
    this.ensureStarted();

    const invocationId = await this.send(service, handler, args, opts);
    const result = await this.waitForCompletion(invocationId, `${service}.${handler}`, opts);
    return result as never;
  }

  async start(): Promise<void> {
    if (this.started) {
      throw new Error("Dicky already started");
    }

    const redis = await this.createRedisClient();
    const blockingRedis = await this.createRedisClient();

    const luaScripts = new LuaScriptsImpl();
    await luaScripts.load(redis);

    const streamProducer = new StreamProducerImpl(redis, this.config.redis.keyPrefix);
    const journal = new JournalStoreImpl(redis, this.config.redis.keyPrefix);
    const stateStore = new StateStoreImpl(redis, this.config.redis.keyPrefix);
    const timerStore = new TimerStoreImpl(redis, this.config.redis.keyPrefix, luaScripts);
    const awakeableStore = new AwakeableStoreImpl(
      redis,
      this.config.redis.keyPrefix,
      journal,
      streamProducer,
    );
    const lockManager = new LockManagerImpl(
      redis,
      luaScripts,
      this.config.redis.keyPrefix,
      this.config.worker.lockTtlMs,
      this.config.worker.lockRenewMs,
    );
    const dlq = new DLQStoreImpl(redis, this.config.redis.keyPrefix);

    this.redis = redis;
    this.blockingRedis = blockingRedis;
    this.streamProducer = streamProducer;
    this.awakeableStore = awakeableStore;
    this.journal = journal;
    this.dlq = dlq;

    if (this.config.worker.concurrency > 0) {
      this.worker = new WorkerImpl(
        redis,
        blockingRedis,
        this.services,
        journal,
        timerStore,
        lockManager,
        stateStore,
        awakeableStore,
        streamProducer,
        dlq,
        this.config,
        this.logger,
      );

      await this.worker.start();
    }

    this.started = true;
  }

  async stop(): Promise<void> {
    if (!this.started) {
      return;
    }

    await this.worker?.stop();

    if (this.blockingRedis && this.blockingRedis !== this.redis) {
      await this.blockingRedis.quit();
    }
    if (this.redis) {
      await this.redis.quit();
    }

    this.started = false;
    this.worker = null;
    this.redis = null;
    this.blockingRedis = null;
    this.streamProducer = null;
    this.awakeableStore = null;
    this.journal = null;
    this.dlq = null;
  }

  async resolveAwakeable(awakeableId: string, value?: unknown): Promise<void> {
    this.ensureStarted();
    await this.awakeableStore!.resolve(awakeableId, value);
  }

  async rejectAwakeable(awakeableId: string, error: string): Promise<void> {
    this.ensureStarted();
    await this.awakeableStore!.reject(awakeableId, error);
  }

  async getInvocation(id: InvocationId): Promise<Invocation | null> {
    this.ensureStarted();
    const record = await this.redis!.hgetall(keys(this.config.redis.keyPrefix).invocation(id));
    if (!record.id) {
      return null;
    }
    return parseInvocation(record);
  }

  async getJournal(id: InvocationId): Promise<JournalEntry[]> {
    this.ensureStarted();
    return this.journal!.getAll(id);
  }

  async cancel(id: InvocationId): Promise<void> {
    this.ensureStarted();
    const key = keys(this.config.redis.keyPrefix).invocation(id);
    await this.redis!.hset(key, "status", "cancelled");
    await this.redis!.hset(key, "updatedAt", String(Date.now()));
  }

  async getMetrics(service = "all"): Promise<Metrics> {
    this.ensureStarted();
    return {
      service,
      completed: 0,
      failed: 0,
      replayed: 0,
      totalSteps: 0,
      avgDurationMs: 0,
      pending: 0,
      active: 0,
    };
  }

  async listDLQ(service: string, opts?: { limit?: number }): Promise<DLQEntry[]> {
    this.ensureStarted();
    return this.dlq!.list(service, opts);
  }

  async retryDLQ(id: InvocationId): Promise<void> {
    this.ensureStarted();
    await this.dlq!.retry(id);
  }

  private async createRedisClient(): Promise<RedisClient> {
    const redisConfig = this.config.redis;

    if (redisConfig.client) {
      return redisConfig.client;
    }

    const options = {
      url: redisConfig.url,
      ...(redisConfig.driver ? { driver: redisConfig.driver } : {}),
      ...(redisConfig.factory ? { factory: redisConfig.factory } : {}),
    };

    return createRedisClient(options);
  }

  private ensureStarted(): void {
    if (!this.started || !this.streamProducer || !this.redis) {
      throw new Error("Dicky must be started before dispatching");
    }
  }

  private async waitForCompletion(
    invocationId: InvocationId,
    handlerName: string,
    opts?: InvokeOptions,
  ): Promise<unknown> {
    const timeoutMs = opts?.timeoutMs ?? 30_000;
    const completionKey = keys(this.config.redis.keyPrefix).completion(invocationId);
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      const raw = await this.redis!.get(completionKey);
      if (raw) {
        const payload = JSON.parse(raw) as {
          status: "completed" | "failed";
          result?: string;
          error?: string;
        };
        if (payload.status === "completed") {
          return payload.result != null ? JSON.parse(payload.result) : undefined;
        }
        throw new Error(payload.error ?? "Invocation failed");
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    throw new TimeoutError(invocationId, handlerName, timeoutMs);
  }
}

function parseInvocation(record: Record<string, string>): Invocation {
  const parsedResult = parseJson(record.result);
  return {
    id: record.id ?? "",
    service: record.service ?? "",
    handler: record.handler ?? "",
    args: record.args ? JSON.parse(record.args) : null,
    status: (record.status as Invocation["status"]) ?? "pending",
    attempt: Number.parseInt(record.attempt ?? "0", 10),
    maxRetries: Number.parseInt(record.maxRetries ?? "0", 10),
    createdAt: Number.parseInt(record.createdAt ?? "0", 10),
    updatedAt: Number.parseInt(record.updatedAt ?? "0", 10),
    ...(record.key ? { key: record.key } : {}),
    ...(record.completedAt ? { completedAt: Number.parseInt(record.completedAt, 10) } : {}),
    ...(parsedResult !== undefined ? { result: parsedResult } : {}),
    ...(record.error ? { error: record.error } : {}),
    ...(record.parentId ? { parentId: record.parentId } : {}),
    ...(record.parentStep ? { parentStep: Number.parseInt(record.parentStep, 10) } : {}),
  };
}

function parseJson(value?: string): unknown {
  if (value == null) {
    return undefined;
  }
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function createLogger(): Logger {
  return {
    error: (data, message) => console.error(message ?? "", data),
    warn: (data, message) => console.warn(message ?? "", data),
    info: (data, message) => console.info(message ?? "", data),
    debug: (data, message) => console.debug(message ?? "", data),
  };
}
