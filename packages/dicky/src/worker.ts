import type { Logger } from "./workerLogger";
import { SuspendedError } from "./errors";
import type { Invocation, InvocationId, RegisteredService, ResolvedConfig } from "./types";
import { parseInvocationRecord } from "./invocation";
import { flattenFields, keys, newWorkerId } from "./utils";
import { DurableContextImpl } from "./context";
import type { AwakeableStore } from "./stores/awakeable";
import type { DLQStore } from "./stores/dlq";
import type { JournalStore } from "./stores/journal";
import type { LockGuard, LockManager } from "./stores/lock";
import type { RedisClient } from "./stores/redis";
import type { StateStore } from "./stores/state";
import { parseStreamMessage } from "./stores/stream";
import type { StreamMessage, StreamProducer } from "./stores/stream";
import type { TimerStore } from "./stores/timer";

export interface Worker {
  start(): Promise<void>;
  stop(): Promise<void>;
}

type MetricField =
  | "completed"
  | "failed"
  | "replayed"
  | "totalSteps"
  | "totalDurationMs"
  | "pending"
  | "active"
  | "processingErrors";

export class WorkerImpl implements Worker {
  private workerId: string;
  private running = false;
  private stopping = false;
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
    this.stopping = false;
    this.running = true;

    for (const [name] of this.services) {
      await this.ensureConsumerGroup(name);
    }

    this.loopTasks = [this.consumerLoop(), this.timerLoop(), this.reclaimLoop()];
  }

  async stop(): Promise<void> {
    this.stopping = true;
    this.running = false;
    this.abortController.abort();

    const timeoutAt = Date.now() + 10_000;
    while (this.activeInvocations.size > 0 && Date.now() < timeoutAt) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await Promise.allSettled(this.loopTasks);
    this.loopTasks = [];
    this.stopping = false;
  }

  private async consumerLoop(): Promise<void> {
    const concurrency = this.config.worker.concurrency;
    let activeCount = 0;

    while (this.running && !this.abortController.signal.aborted) {
      if (this.stopping) {
        break;
      }
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

          if (!this.running || this.stopping) {
            break;
          }

          const [, messages] = results[0] ?? [];
          for (const [messageId, fields] of messages ?? []) {
            if (this.stopping) {
              break;
            }
            const msg = parseStreamMessage(messageId, fields);
            activeCount += 1;
            this.activeInvocations.add(msg.invocationId);

            this.processMessage(serviceName, msg)
              .catch(async (err) => {
                this.logger.error({ err, msg }, "Failed to process message");
                await this.incrementMetrics(serviceName, { processingErrors: 1 });
              })
              .finally(() => {
                activeCount -= 1;
                this.activeInvocations.delete(msg.invocationId);
              });
          }
        } catch (err) {
          if (!this.running) {
            break;
          }
          if (isNoGroupError(err)) {
            await this.ensureConsumerGroup(serviceName);
            continue;
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
      for (const [serviceName] of this.services) {
        try {
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
            await this.incrementMetrics(serviceName, { replayed: 1 });
            await this.processMessage(serviceName, msg);
          }
        } catch (err) {
          if (!this.running) {
            break;
          }
          if (isNoGroupError(err)) {
            await this.ensureConsumerGroup(serviceName);
            continue;
          }
          this.logger.error({ err }, "Reclaim loop error");
        }
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
      await this.setInvocationStatus(invocation, "running");

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
        initialState as never,
      );

      const handlerDef = service.handlers[msg.handler];
      if (!handlerDef) {
        this.logger.warn?.({ serviceName, handler: msg.handler }, "Unknown handler");
        await this.ack(serviceName, msg.messageId);
        return;
      }

      const args = safeJsonParse(msg.args, null);
      const parsedArgs = handlerDef.input ? handlerDef.input.parse(args) : args;
      const result = await handlerDef.handler(ctx, parsedArgs);
      const validatedResult = handlerDef.output ? handlerDef.output.parse(result) : result;

      const serializedResult = validatedResult !== undefined ? JSON.stringify(validatedResult) : undefined;
      await this.completeInvocation(invocation, serializedResult);
      await this.ack(serviceName, msg.messageId);
      await this.publishCompletion(invocation.id, "completed", serializedResult);

      if (invocation.parentId != null && invocation.parentStep != null) {
        await this.journal.complete(invocation.parentId, invocation.parentStep, serializedResult);
        await this.streamProducer.reenqueue(invocation.parentId);
      }
    } catch (err) {
      if (err instanceof SuspendedError) {
        await this.setInvocationStatus(invocation, "suspended");
        await this.ack(serviceName, msg.messageId);
        return;
      }

      const attempt = Number.parseInt(msg.attempt, 10);
      const errorMsg = err instanceof Error ? err.message : String(err);

      if (attempt >= invocation.maxRetries) {
        await this.dlq.push(invocation, errorMsg);
        await this.failInvocation(invocation, errorMsg);
        await this.ack(serviceName, msg.messageId);
        await this.publishCompletion(invocation.id, "failed", undefined, errorMsg);

        if (invocation.parentId != null && invocation.parentStep != null) {
          await this.journal.fail(invocation.parentId, invocation.parentStep, errorMsg);
          await this.streamProducer.reenqueue(invocation.parentId);
        }
      } else {
        const delay = computeRetryDelay(attempt, this.config.retry);
        await this.setInvocationStatus(invocation, "pending");
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
      return parseInvocationRecord(record, { service: serviceName, args: msg.args });
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

    await this.incrementMetrics(serviceName, { pending: 1 });

    return invocation;
  }

  private async setInvocationStatus(
    invocation: Invocation,
    status: Invocation["status"],
    extraFields: Record<string, string> = {},
  ): Promise<void> {
    const invKey = keys(this.config.redis.keyPrefix).invocation(invocation.id);
    const now = Date.now();
    await this.redis.hmset(
      invKey,
      ...flattenFields({
        status,
        updatedAt: String(now),
        ...extraFields,
      }),
    );
    await this.applyStatusMetrics(invocation, status);
    invocation.status = status;
  }

  private async applyStatusMetrics(
    invocation: Invocation,
    nextStatus: Invocation["status"],
  ): Promise<void> {
    const current = invocation.status;
    if (current === nextStatus) {
      return;
    }

    const deltas: Partial<Record<MetricField, number>> = {};
    const isTerminal =
      nextStatus === "completed" || nextStatus === "failed" || nextStatus === "cancelled";

    if (current === "pending" && nextStatus === "running") {
      deltas.pending = -1;
      deltas.active = 1;
    } else if (current === "suspended" && nextStatus === "running") {
      deltas.pending = -1;
      deltas.active = 1;
      deltas.replayed = 1;
    } else if (current === "running" && nextStatus === "suspended") {
      deltas.active = -1;
      deltas.pending = 1;
    } else if (current === "running" && nextStatus === "pending") {
      deltas.active = -1;
      deltas.pending = 1;
    } else if ((current === "pending" || current === "suspended") && isTerminal) {
      deltas.pending = -1;
    } else if (current === "running" && isTerminal) {
      deltas.active = -1;
    }

    if (Object.keys(deltas).length > 0) {
      await this.incrementMetrics(invocation.service, deltas);
    }
  }

  private async completeInvocation(invocation: Invocation, result?: string): Promise<void> {
    const completedAt = Date.now();
    const fields: Record<string, string> = {
      completedAt: String(completedAt),
      ...(result !== undefined ? { result } : {}),
    };
    await this.setInvocationStatus(invocation, "completed", fields);
    await this.recordCompletionMetrics(invocation, "completed", completedAt);
    await this.applyRetention(invocation);
  }

  private async failInvocation(invocation: Invocation, error: string): Promise<void> {
    const completedAt = Date.now();
    await this.setInvocationStatus(invocation, "failed", {
      completedAt: String(completedAt),
      error,
    });
    await this.recordCompletionMetrics(invocation, "failed", completedAt);
    await this.applyRetention(invocation);
  }

  private async recordCompletionMetrics(
    invocation: Invocation,
    status: "completed" | "failed",
    completedAt: number,
  ): Promise<void> {
    const steps = await this.journal.length(invocation.id);
    const durationMs = Math.max(0, completedAt - invocation.createdAt);
    await this.incrementMetrics(invocation.service, {
      totalSteps: steps,
      totalDurationMs: durationMs,
      ...(status === "completed" ? { completed: 1 } : { failed: 1 }),
    });
  }

  private async applyRetention(invocation: Invocation): Promise<void> {
    const invKey = keys(this.config.redis.keyPrefix).invocation(invocation.id);
    const journalKey = keys(this.config.redis.keyPrefix).journal(invocation.id);
    await this.redis.pexpire(invKey, this.config.retention.invocationTtlMs);
    await this.redis.pexpire(journalKey, this.config.retention.journalTtlMs);
  }

  private async publishCompletion(
    invocationId: InvocationId,
    status: "completed" | "failed",
    result?: string,
    error?: string,
  ): Promise<void> {
    const completionKey = keys(this.config.redis.keyPrefix).completion(invocationId);
    const queueKey = keys(this.config.redis.keyPrefix).completionQueue(invocationId);
    const payload = JSON.stringify({ status, result, error });
    await this.redis.set(completionKey, payload);
    await this.redis.pexpire(completionKey, this.config.retention.completionTtlMs);
    await this.redis.rpush(queueKey, payload);
    await this.redis.pexpire(queueKey, this.config.retention.completionTtlMs);
  }

  private async incrementMetrics(
    serviceName: string,
    deltas: Partial<Record<MetricField, number>>,
  ): Promise<void> {
    const entries = Object.entries(deltas).filter(
      (entry): entry is [string, number] => entry[1] !== undefined && entry[1] !== 0,
    );
    if (entries.length === 0) {
      return;
    }

    await Promise.all(
      [serviceName, "all"].map((service) => this.applyMetricDeltas(service, entries)),
    );
  }

  private async applyMetricDeltas(
    serviceName: string,
    entries: Array<[string, number]>,
  ): Promise<void> {
    const metricsKey = keys(this.config.redis.keyPrefix).metrics(serviceName);
    for (const [field, value] of entries) {
      await this.redis.hincrby(metricsKey, field, value);
    }
  }
}

function safeJsonParse<T>(value: string, fallback: T): T {
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function isNoGroupError(err: unknown): boolean {
  if (!(err instanceof Error)) {
    return false;
  }
  return err.message.includes("NOGROUP");
}

function computeRetryDelay(attempt: number, retry: ResolvedConfig["retry"]): number {
  const base = retry.initialDelayMs;
  const multiplier = retry.backoffMultiplier;
  const delay = base * Math.pow(multiplier, attempt);
  return Math.min(delay, retry.maxDelayMs);
}
