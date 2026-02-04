import type {
  AddToRegistry,
  ArgsOf,
  DLQEntry,
  DickyConfig,
  HandlerDef,
  InvokeOptions,
  Invocation,
  InvocationId,
  JournalEntry,
  Metrics,
  ObjectDef,
  Registry,
  RegisteredHandler,
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
import { createRedisClient, IoredisClient } from "./stores/redis";
import type { RedisClient } from "./stores/redis";
import { StateStoreImpl } from "./stores/state";
import type { StreamProducer } from "./stores/stream";
import { StreamProducerImpl } from "./stores/stream";
import { TimerStoreImpl } from "./stores/timer";
import type { Worker } from "./worker";
import { WorkerImpl } from "./worker";
import { createLogger } from "./workerLogger";
import type { Logger } from "./workerLogger";
import { LuaScriptsImpl } from "./lua";
import { RedisAdapterPool } from "./adapters/pool";
import { resolveConfig } from "./config";
import { keys, validateIdentifier } from "./utils";
import { TimeoutError } from "./errors";
import { parseInvocationRecord } from "./invocation";

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
  private completionRedis: RedisClient | null = null;
  private redisPool: RedisAdapterPool | null = null;
  private signalHandlers = new Map<NodeJS.Signals, () => void>();
  private stopping = false;

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

    validateIdentifier(def.name, "Service name");
    for (const handlerName of Object.keys(def.handlers)) {
      validateIdentifier(handlerName, "Handler name");
    }

    const normalizedHandlers = normalizeHandlers(def.handlers);

    if (def.__kind === "service") {
      this.services.set(def.name, {
        kind: "service",
        name: def.name,
        handlers: normalizedHandlers,
      });
    } else {
      this.services.set(def.name, {
        kind: "object",
        name: def.name,
        handlers: normalizedHandlers,
        initialState: def.initial,
      });
    }

    return this as unknown as Dicky<AddToRegistry<R, T>>;
  }

  getService(name: string): RegisteredService | undefined {
    return this.services.get(name);
  }

  /**
   * Dispatch a handler without awaiting its result.
   */
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
      maxRetries: opts?.maxRetries ?? this.config.retry.maxRetries,
      ...(opts?.idempotencyKey ? { idempotencyKey: opts.idempotencyKey } : {}),
    });
  }

  /**
   * Invoke a handler and wait for its completion payload.
   */
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
    const blockingRedis = await this.createBlockingRedisClient(redis);
    const completionRedis = await this.createCompletionRedisClient(redis);

    const luaScripts = new LuaScriptsImpl();
    await luaScripts.load(redis);

    const streamProducer = new StreamProducerImpl(
      redis,
      this.config.redis.keyPrefix,
      this.config.retry.maxRetries,
      this.config.retention.idempotencyTtlMs,
    );
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
    this.completionRedis = completionRedis;

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
    this.registerSignalHandlers();
  }

  async stop(): Promise<void> {
    if (!this.started || this.stopping) {
      return;
    }

    this.stopping = true;
    this.unregisterSignalHandlers();

    await this.worker?.stop();

    if (this.redisPool) {
      await this.redisPool.close();
      this.redisPool = null;
    } else {
      if (this.blockingRedis && this.blockingRedis !== this.redis) {
        await this.blockingRedis.quit();
      }
      if (
        this.completionRedis &&
        this.completionRedis !== this.redis &&
        this.completionRedis !== this.blockingRedis
      ) {
        await this.completionRedis.quit();
      }
      if (this.redis) {
        await this.redis.quit();
      }
    }

    this.started = false;
    this.worker = null;
    this.redis = null;
    this.blockingRedis = null;
    this.streamProducer = null;
    this.awakeableStore = null;
    this.journal = null;
    this.dlq = null;
    this.completionRedis = null;
    this.stopping = false;
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
    return parseInvocationRecord(record, {
      service: record.service ?? "",
      args: record.args ?? "null",
    });
  }

  async getJournal(id: InvocationId): Promise<JournalEntry[]> {
    this.ensureStarted();
    return this.journal!.getAll(id);
  }

  async cancel(id: InvocationId): Promise<void> {
    this.ensureStarted();
    const key = keys(this.config.redis.keyPrefix).invocation(id);
    const record = await this.redis!.hgetall(key);
    if (!record.id) {
      return;
    }

    await this.redis!.hmset(key, "status", "cancelled", "updatedAt", String(Date.now()));
    await this.redis!.pexpire(key, this.config.retention.invocationTtlMs);
    await this.redis!.pexpire(
      keys(this.config.redis.keyPrefix).journal(id),
      this.config.retention.journalTtlMs,
    );

    await this.adjustMetricsForCancel(record.service ?? "all", record.status);
  }

  async getMetrics(service = "all"): Promise<Metrics> {
    this.ensureStarted();
    const key = keys(this.config.redis.keyPrefix).metrics(service);
    const data = await this.redis!.hgetall(key);

    const completed = Number.parseInt(data.completed ?? "0", 10);
    const failed = Number.parseInt(data.failed ?? "0", 10);
    const replayed = Number.parseInt(data.replayed ?? "0", 10);
    const totalSteps = Number.parseInt(data.totalSteps ?? "0", 10);
    const totalDurationMs = Number.parseInt(data.totalDurationMs ?? "0", 10);
    const pending = Number.parseInt(data.pending ?? "0", 10);
    const active = Number.parseInt(data.active ?? "0", 10);
    const processingErrors = Number.parseInt(data.processingErrors ?? "0", 10);
    const totalCompleted = completed + failed;

    return {
      service,
      completed,
      failed,
      replayed,
      totalSteps,
      avgDurationMs: totalCompleted > 0 ? Math.round(totalDurationMs / totalCompleted) : 0,
      pending,
      active,
      processingErrors,
    };
  }

  /**
   * Perform a Redis health check and report latency.
   */
  async health(): Promise<{ status: "healthy" | "unhealthy"; latency: number }> {
    this.ensureStarted();
    const start = Date.now();
    try {
      await this.redis!.ping();
      return { status: "healthy", latency: Date.now() - start };
    } catch {
      return { status: "unhealthy", latency: Date.now() - start };
    }
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

    if (redisConfig.adapter) {
      const poolSize = redisConfig.pool?.size ?? 3;
      if (!this.redisPool) {
        this.redisPool = await RedisAdapterPool.create(
          redisConfig.adapter,
          redisConfig.url,
          poolSize,
        );
      }
      return this.redisPool.reserve();
    }

    const options = {
      url: redisConfig.url,
      ...(redisConfig.driver ? { driver: redisConfig.driver } : {}),
      ...(redisConfig.factory ? { factory: redisConfig.factory } : {}),
    };

    return createRedisClient(options);
  }

  private async createBlockingRedisClient(primary: RedisClient): Promise<RedisClient> {
    const redisConfig = this.config.redis;

    if (redisConfig.blockingClient) {
      return redisConfig.blockingClient;
    }

    if (redisConfig.blockingFactory) {
      return redisConfig.blockingFactory(redisConfig.url);
    }

    if (this.redisPool) {
      return this.redisPool.reserve();
    }

    if (primary instanceof IoredisClient) {
      const duplicate = primary.raw.duplicate();
      await duplicate.ping();
      return new IoredisClient(duplicate);
    }

    return primary;
  }

  private async createCompletionRedisClient(primary: RedisClient): Promise<RedisClient> {
    if (this.redisPool) {
      return this.redisPool.reserve();
    }

    if (primary instanceof IoredisClient) {
      const duplicate = primary.raw.duplicate();
      await duplicate.ping();
      return new IoredisClient(duplicate);
    }

    return primary;
  }

  private ensureStarted(): void {
    if (
      !this.started ||
      !this.streamProducer ||
      !this.redis ||
      !this.blockingRedis ||
      !this.completionRedis
    ) {
      throw new Error("Dicky must be started before dispatching");
    }
  }

  private async adjustMetricsForCancel(serviceName: string, status?: string): Promise<void> {
    if (!this.redis) {
      return;
    }

    const deltas: Array<[string, number]> = [];
    if (status === "pending" || status === "suspended") {
      deltas.push(["pending", -1]);
    }
    if (status === "running") {
      deltas.push(["active", -1]);
    }

    if (deltas.length === 0) {
      return;
    }

    for (const service of [serviceName, "all"]) {
      const key = keys(this.config.redis.keyPrefix).metrics(service);
      for (const [field, value] of deltas) {
        await this.redis.hincrby(key, field, value);
      }
    }
  }

  private registerSignalHandlers(): void {
    if (!this.config.shutdown.handleSignals) {
      return;
    }

    for (const signal of this.config.shutdown.signals) {
      if (this.signalHandlers.has(signal)) {
        continue;
      }
      const handler = () => {
        void this.stop();
      };
      this.signalHandlers.set(signal, handler);
      process.on(signal, handler);
    }
  }

  private unregisterSignalHandlers(): void {
    for (const [signal, handler] of this.signalHandlers) {
      process.off(signal, handler);
    }
    this.signalHandlers.clear();
  }

  private async waitForCompletion(
    invocationId: InvocationId,
    handlerName: string,
    opts?: InvokeOptions,
  ): Promise<unknown> {
    const timeoutMs = opts?.timeoutMs ?? 30_000;
    const completionKey = keys(this.config.redis.keyPrefix).completion(invocationId);
    const queueKey = keys(this.config.redis.keyPrefix).completionQueue(invocationId);
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      const raw = await this.redis!.get(completionKey);
      if (raw) {
        return parseCompletionPayload(raw);
      }

      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) {
        break;
      }
      const timeoutSeconds = Math.ceil(remainingMs / 1000);
      const popped = await this.completionRedis!.blpop(queueKey, timeoutSeconds);
      if (!popped) {
        continue;
      }
      const [, payload] = popped;
      return parseCompletionPayload(payload);
    }

    throw new TimeoutError(invocationId, handlerName, timeoutMs);
  }
}

function parseCompletionPayload(raw: string): unknown {
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

function normalizeHandlers(
  handlers: Record<string, HandlerDef>,
): Record<string, RegisteredHandler> {
  const normalized: Record<string, RegisteredHandler> = {};
  for (const [name, def] of Object.entries(handlers)) {
    if (typeof def === "function") {
      normalized[name] = { handler: def };
      continue;
    }
    normalized[name] = {
      handler: def.handler,
      ...(def.input ? { input: def.input } : {}),
      ...(def.output ? { output: def.output } : {}),
    };
  }
  return normalized;
}
