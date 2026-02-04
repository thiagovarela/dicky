import type { Logger } from "./workerLogger";
import { SuspendedError } from "./errors";
import type { Invocation, InvocationId, RegisteredService, ResolvedConfig } from "./types";
import { keys, newWorkerId } from "./utils";
import { DurableContextImpl } from "./context";
import type { AwakeableStore } from "./stores/awakeable";
import type { DLQStore } from "./stores/dlq";
import type { JournalStore } from "./stores/journal";
import type { LockGuard, LockManager } from "./stores/lock";
import type { RedisClient } from "./stores/redis";
import type { StateStore } from "./stores/state";
import type { StreamMessage, StreamProducer } from "./stores/stream";
import type { TimerStore } from "./stores/timer";

export interface Worker {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export class WorkerImpl implements Worker {
  private workerId: string;
  private running = false;
  private activeInvocations = new Set<string>();
  private abortController = new AbortController();
  private loopTasks: Array<Promise<void>> = [];

  constructor(
    private redis: RedisClient,
    private blockingRedis: RedisClient,
    private services: Map<string, RegisteredService>,
    private journal: JournalStore,
    private timerStore: TimerStore,
    private lockManager: LockManager,
    private stateStore: StateStore,
    private awakeableStore: AwakeableStore,
    private streamProducer: StreamProducer,
    private dlq: DLQStore,
    private config: ResolvedConfig,
    private logger: Logger,
  ) {
    this.workerId = newWorkerId();
  }

  async start(): Promise<void> {
    this.running = true;

    for (const [name] of this.services) {
      await this.ensureConsumerGroup(name);
    }

    this.loopTasks = [this.consumerLoop(), this.timerLoop(), this.reclaimLoop()];
  }

  async stop(): Promise<void> {
    this.running = false;
    this.abortController.abort();

    const timeoutAt = Date.now() + 10_000;
    while (this.activeInvocations.size > 0 && Date.now() < timeoutAt) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await Promise.allSettled(this.loopTasks);
    this.loopTasks = [];
  }

  private async consumerLoop(): Promise<void> {
    const concurrency = this.config.worker.concurrency;
    let activeCount = 0;

    while (this.running && !this.abortController.signal.aborted) {
      if (this.services.size === 0) {
        await new Promise((resolve) => setTimeout(resolve, this.config.worker.pollIntervalMs));
        continue;
      }

      if (activeCount >= concurrency) {
        await new Promise((resolve) => setTimeout(resolve, 50));
        continue;
      }

      for (const [serviceName] of this.services) {
        if (!this.running) {
          break;
        }

        try {
          const streamKey = keys(this.config.redis.keyPrefix).stream(serviceName);
          const groupName = keys(this.config.redis.keyPrefix).consumerGroup(serviceName);
          const consumerName = keys(this.config.redis.keyPrefix).consumerName(this.workerId);

          const results = await this.blockingRedis.xreadgroup(
            groupName,
            consumerName,
            "1",
            String(this.config.worker.pollIntervalMs),
            streamKey,
            ">",
          );

          if (!results || results.length === 0) {
            continue;
          }

          const [, messages] = results[0] ?? [];
          for (const [messageId, fields] of messages ?? []) {
            const msg = parseStreamMessage(messageId, fields);
            activeCount += 1;
            this.activeInvocations.add(msg.invocationId);

            this.processMessage(serviceName, msg)
              .catch((err) => this.logger.error({ err, msg }, "Failed to process message"))
              .finally(() => {
                activeCount -= 1;
                this.activeInvocations.delete(msg.invocationId);
              });
          }
        } catch (err) {
          if (!this.running) {
            break;
          }
          this.logger.error({ err }, "Consumer loop error");
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    }
  }

  private async timerLoop(): Promise<void> {
    const interval = this.config.worker.timerPollIntervalMs;

    while (this.running && !this.abortController.signal.aborted) {
      try {
        const expired = await this.timerStore.pollExpired(Date.now(), 10);

        for (const entry of expired) {
          switch (entry.type) {
            case "sleep": {
              if (entry.sequence == null) {
                break;
              }
              await this.journal.complete(entry.invocationId, entry.sequence, undefined);
              await this.streamProducer.reenqueue(entry.invocationId);
              break;
            }
            case "delayed": {
              await this.streamProducer.enqueueDelayed(entry.invocationId);
              break;
            }
            case "retry": {
              await this.streamProducer.reenqueueRetry(entry.invocationId);
              break;
            }
            default:
              break;
          }
        }
      } catch (err) {
        if (!this.running) {
          break;
        }
        this.logger.error({ err }, "Timer loop error");
      }

      await new Promise((resolve) => setTimeout(resolve, interval));
    }
  }

  private async reclaimLoop(): Promise<void> {
    const interval = Math.max(1, Math.floor(this.config.worker.ackTimeoutMs / 2));

    while (this.running && !this.abortController.signal.aborted) {
      try {
        for (const [serviceName] of this.services) {
          const streamKey = keys(this.config.redis.keyPrefix).stream(serviceName);
          const groupName = keys(this.config.redis.keyPrefix).consumerGroup(serviceName);
          const consumerName = keys(this.config.redis.keyPrefix).consumerName(this.workerId);

          const [nextId, reclaimed] = await this.redis.xautoclaim(
            streamKey,
            groupName,
            consumerName,
            String(this.config.worker.ackTimeoutMs),
            "0-0",
            "10",
          );

          void nextId;

          for (const [messageId, fields] of reclaimed ?? []) {
            const msg = parseStreamMessage(messageId, fields);
            await this.processMessage(serviceName, msg);
          }
        }
      } catch (err) {
        if (!this.running) {
          break;
        }
        this.logger.error({ err }, "Reclaim loop error");
      }

      await new Promise((resolve) => setTimeout(resolve, interval));
    }
  }

  private async processMessage(serviceName: string, msg: StreamMessage): Promise<void> {
    const service = this.services.get(serviceName);
    if (!service) {
      return;
    }

    const invocation = await this.loadOrCreateInvocation(msg, serviceName, service);

    let lockGuard: LockGuard | null = null;
    if (service.kind === "object" && invocation.key) {
      try {
        lockGuard = await this.lockManager.acquire(serviceName, invocation.key);
      } catch {
        return;
      }
    }

    try {
      await this.updateInvocationStatus(invocation.id, "running");

      let initialState: unknown = undefined;
      if (service.kind === "object" && invocation.key) {
        const stored = await this.stateStore.get(serviceName, invocation.key);
        initialState = stored != null ? JSON.parse(stored) : service.initialState;
      }

      const ctxInvocation = invocation.key
        ? { id: invocation.id, key: invocation.key, service: serviceName }
        : { id: invocation.id, service: serviceName };

      const ctx = new DurableContextImpl(
        ctxInvocation,
        this.journal,
        this.timerStore,
        this.stateStore,
        this.awakeableStore,
        this.streamProducer,
        service.kind === "object" ? { initialState: service.initialState } : undefined,
        initialState as never,
      );

      const handler = service.handlers[msg.handler];
      if (!handler) {
        this.logger.warn?.({ serviceName, handler: msg.handler }, "Unknown handler");
        await this.ack(serviceName, msg.messageId);
        return;
      }

      const args = safeJsonParse(msg.args, null);
      const handlerFn = handler as NonNullable<typeof handler>;
      const result = await handlerFn(ctx, args);

      const serializedResult = result !== undefined ? JSON.stringify(result) : undefined;
      await this.completeInvocation(invocation.id, serializedResult);
      await this.ack(serviceName, msg.messageId);
      await this.publishCompletion(invocation.id, "completed", serializedResult);

      if (invocation.parentId != null && invocation.parentStep != null) {
        await this.journal.complete(invocation.parentId, invocation.parentStep, serializedResult);
        await this.streamProducer.reenqueue(invocation.parentId);
      }
    } catch (err) {
      if (err instanceof SuspendedError) {
        await this.updateInvocationStatus(invocation.id, "suspended");
        await this.ack(serviceName, msg.messageId);
        return;
      }

      const attempt = Number.parseInt(msg.attempt, 10);
      const errorMsg = err instanceof Error ? err.message : String(err);

      if (attempt + 1 >= invocation.maxRetries) {
        await this.dlq.push(invocation, errorMsg);
        await this.failInvocation(invocation.id, errorMsg);
        await this.ack(serviceName, msg.messageId);
        await this.publishCompletion(invocation.id, "failed", undefined, errorMsg);

        if (invocation.parentId != null && invocation.parentStep != null) {
          await this.journal.fail(invocation.parentId, invocation.parentStep, errorMsg);
          await this.streamProducer.reenqueue(invocation.parentId);
        }
      } else {
        const delay = computeRetryDelay(attempt, this.config.retry);
        await this.timerStore.schedule(
          { invocationId: invocation.id, type: "retry" },
          Date.now() + delay,
        );
        await this.ack(serviceName, msg.messageId);
      }
    } finally {
      if (lockGuard) {
        await lockGuard.release();
      }
    }
  }

  private async ensureConsumerGroup(serviceName: string): Promise<void> {
    const streamKey = keys(this.config.redis.keyPrefix).stream(serviceName);
    const groupName = keys(this.config.redis.keyPrefix).consumerGroup(serviceName);
    try {
      await this.redis.xgroup(streamKey, "CREATE", groupName, "0", "MKSTREAM");
    } catch (err) {
      if (!(err instanceof Error) || !err.message.includes("BUSYGROUP")) {
        throw err;
      }
    }
  }

  private async ack(serviceName: string, messageId: string): Promise<void> {
    const streamKey = keys(this.config.redis.keyPrefix).stream(serviceName);
    const groupName = keys(this.config.redis.keyPrefix).consumerGroup(serviceName);
    await this.redis.xack(streamKey, groupName, messageId);
  }

  private async loadOrCreateInvocation(
    msg: StreamMessage,
    serviceName: string,
    service: RegisteredService,
  ): Promise<Invocation> {
    const invKey = keys(this.config.redis.keyPrefix).invocation(msg.invocationId);
    const record = await this.redis.hgetall(invKey);
    const now = Date.now();

    if (record.id) {
      return parseInvocationRecord(record, serviceName, msg.args);
    }

    const invocation: Invocation = {
      id: msg.invocationId,
      service: serviceName,
      handler: msg.handler,
      args: safeJsonParse(msg.args, null),
      status: "pending",
      attempt: Number.parseInt(msg.attempt, 10),
      maxRetries: this.config.retry.maxRetries,
      createdAt: now,
      updatedAt: now,
      ...(msg.key ? { key: msg.key } : {}),
    };

    await this.redis.hmset(
      invKey,
      "id",
      invocation.id,
      "service",
      invocation.service,
      "handler",
      invocation.handler,
      "args",
      msg.args,
      "status",
      invocation.status,
      "attempt",
      String(invocation.attempt),
      "maxRetries",
      String(invocation.maxRetries),
      "createdAt",
      String(invocation.createdAt),
      "updatedAt",
      String(invocation.updatedAt),
      ...(msg.key ? ["key", msg.key] : []),
      ...(service.kind === "object" && service.initialState !== undefined
        ? ["initialState", JSON.stringify(service.initialState)]
        : []),
    );

    return invocation;
  }

  private async updateInvocationStatus(
    id: InvocationId,
    status: Invocation["status"],
  ): Promise<void> {
    const invKey = keys(this.config.redis.keyPrefix).invocation(id);
    await this.redis.hset(invKey, "status", status);
    await this.redis.hset(invKey, "updatedAt", String(Date.now()));
  }

  private async completeInvocation(id: InvocationId, result?: string): Promise<void> {
    const invKey = keys(this.config.redis.keyPrefix).invocation(id);
    await this.redis.hset(invKey, "status", "completed");
    await this.redis.hset(invKey, "updatedAt", String(Date.now()));
    await this.redis.hset(invKey, "completedAt", String(Date.now()));
    if (result !== undefined) {
      await this.redis.hset(invKey, "result", result);
    }
  }

  private async failInvocation(id: InvocationId, error: string): Promise<void> {
    const invKey = keys(this.config.redis.keyPrefix).invocation(id);
    await this.redis.hset(invKey, "status", "failed");
    await this.redis.hset(invKey, "error", error);
    await this.redis.hset(invKey, "updatedAt", String(Date.now()));
    await this.redis.hset(invKey, "completedAt", String(Date.now()));
  }

  private async publishCompletion(
    invocationId: InvocationId,
    status: "completed" | "failed",
    result?: string,
    error?: string,
  ): Promise<void> {
    const key = keys(this.config.redis.keyPrefix).completion(invocationId);
    const payload = JSON.stringify({ status, result, error });
    await this.redis.set(key, payload);
  }
}

function parseStreamMessage(messageId: string, fields: string[]): StreamMessage {
  const parsed: Record<string, string> = {};
  for (let index = 0; index < fields.length; index += 2) {
    const key = fields[index];
    const value = fields[index + 1];
    if (key != null && value != null) {
      parsed[key] = value;
    }
  }

  return {
    messageId,
    invocationId: parsed.invocationId ?? "",
    handler: parsed.handler ?? "",
    args: parsed.args ?? "",
    attempt: parsed.attempt ?? "0",
    ...(parsed.key ? { key: parsed.key } : {}),
  };
}

function parseInvocationRecord(
  record: Record<string, string>,
  serviceName: string,
  fallbackArgs: string,
): Invocation {
  const argsRaw = record.args ?? fallbackArgs ?? "null";
  return {
    id: record.id ?? "",
    service: record.service ?? serviceName,
    handler: record.handler ?? "",
    args: safeJsonParse(argsRaw, null),
    status: (record.status as Invocation["status"]) ?? "pending",
    attempt: Number.parseInt(record.attempt ?? "0", 10),
    maxRetries: Number.parseInt(record.maxRetries ?? "0", 10),
    createdAt: Number.parseInt(record.createdAt ?? "0", 10),
    updatedAt: Number.parseInt(record.updatedAt ?? "0", 10),
    ...(record.key ? { key: record.key } : {}),
    ...(record.completedAt ? { completedAt: Number.parseInt(record.completedAt, 10) } : {}),
    ...(record.result ? { result: record.result } : {}),
    ...(record.error ? { error: record.error } : {}),
    ...(record.parentId ? { parentId: record.parentId } : {}),
    ...(record.parentStep ? { parentStep: Number.parseInt(record.parentStep, 10) } : {}),
  };
}

function safeJsonParse<T>(value: string, fallback: T): T {
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function computeRetryDelay(attempt: number, retry: ResolvedConfig["retry"]): number {
  const base = retry.initialDelayMs;
  const multiplier = retry.backoffMultiplier;
  const delay = base * Math.pow(multiplier, attempt);
  return Math.min(delay, retry.maxDelayMs);
}
