import { describe, expect, it } from "bun:test";
import { WorkerImpl } from "../worker";
import type { RegisteredService, ResolvedConfig } from "../types";
import { keys } from "../utils";
import type { StreamMessage, StreamProducer } from "../stores/stream";
import { MockRedisClient } from "../stores/redis";
import { JournalStoreImpl } from "../stores/journal";
import { TimerStoreImpl } from "../stores/timer";
import { StateStoreImpl } from "../stores/state";
import { AwakeableStoreImpl } from "../stores/awakeable";
import { LockManagerImpl } from "../stores/lock";
import { DLQStoreImpl } from "../stores/dlq";
import { MockLuaScripts } from "./stores/helpers";
import type { Logger } from "../workerLogger";

const prefix = "test:worker:";

class TestStreamProducer implements StreamProducer {
  reenqueueCalls: string[] = [];
  reenqueueRetryCalls: string[] = [];
  enqueueDelayedCalls: string[] = [];
  dispatchCalls: Array<{ service: string; handler: string }> = [];

  async enqueue(
    _serviceName: string,
    _invocationId: string,
    _handler: string,
    _args: string,
    _opts?: { key?: string; attempt?: number },
  ): Promise<string> {
    return "";
  }

  async reenqueue(invocationId: string): Promise<void> {
    this.reenqueueCalls.push(invocationId);
  }

  async reenqueueRetry(invocationId: string): Promise<void> {
    this.reenqueueRetryCalls.push(invocationId);
  }

  async enqueueDelayed(invocationId: string): Promise<void> {
    this.enqueueDelayedCalls.push(invocationId);
  }

  async dispatch(
    service: string,
    handler: string,
    _args: unknown,
    _opts?: {
      key?: string;
      delay?: string;
      parentId?: string;
      parentStep?: number;
      maxRetries?: number;
      idempotencyKey?: string;
    },
  ): Promise<string> {
    this.dispatchCalls.push({ service, handler });
    return "child";
  }
}

const logger: Logger = {
  error: () => undefined,
  warn: () => undefined,
  info: () => undefined,
  debug: () => undefined,
};

function createConfig(retryMaxRetries = 2): ResolvedConfig {
  return {
    redis: {
      host: "",
      port: 0,
      password: "",
      db: 0,
      keyPrefix: prefix,
      tls: false,
      url: "redis://localhost:6379/0",
    },
    worker: {
      concurrency: 1,
      pollIntervalMs: 1,
      lockTtlMs: 50,
      lockRenewMs: 10,
      ackTimeoutMs: 1,
      timerPollIntervalMs: 1,
    },
    retry: {
      maxRetries: retryMaxRetries,
      initialDelayMs: 1,
      maxDelayMs: 10,
      backoffMultiplier: 2,
    },
    retention: {
      completionTtlMs: 60 * 60 * 1000,
      invocationTtlMs: 24 * 60 * 60 * 1000,
      journalTtlMs: 24 * 60 * 60 * 1000,
      idempotencyTtlMs: 24 * 60 * 60 * 1000,
    },
    shutdown: {
      handleSignals: false,
      signals: ["SIGINT", "SIGTERM"],
    },
    log: { level: "info" },
  };
}

function createWorker(
  handler: (ctx: unknown, args: unknown) => Promise<unknown>,
  kind: RegisteredService["kind"] = "service",
  retryMaxRetries = 2,
) {
  const redis = new MockRedisClient();
  const journal = new JournalStoreImpl(redis, prefix);
  const timerStore = new TimerStoreImpl(redis, prefix, new MockLuaScripts());
  const stateStore = new StateStoreImpl(redis, prefix);
  const streamProducer = new TestStreamProducer();
  const awakeableStore = new AwakeableStoreImpl(redis, prefix, journal, streamProducer);
  const lockManager = new LockManagerImpl(redis, new MockLuaScripts(), prefix, 50, 10);
  const dlq = new DLQStoreImpl(redis, prefix);

  const services = new Map<string, RegisteredService>([
    [
      "svc",
      {
        kind,
        name: "svc",
        handlers: { handler: { handler } },
        ...(kind === "object" ? { initialState: { count: 0 } } : {}),
      },
    ],
  ]);

  const worker = new WorkerImpl(
    redis,
    redis,
    services,
    journal,
    timerStore,
    lockManager,
    stateStore,
    awakeableStore,
    streamProducer,
    dlq,
    createConfig(retryMaxRetries),
    logger,
  );

  return { worker, redis, streamProducer, journal, timerStore, dlq };
}

describe("Worker", () => {
  it("processes messages and completes invocations", async () => {
    const { worker, redis } = createWorker(async () => ({ ok: true }));

    const msg: StreamMessage = {
      messageId: "1-0",
      invocationId: "inv_1",
      handler: "handler",
      args: JSON.stringify({ ok: true }),
      attempt: "0",
    };

    const workerAny = worker as unknown as {
      processMessage: (serviceName: string, message: StreamMessage) => Promise<void>;
    };

    await workerAny.processMessage("svc", msg);

    const inv = await redis.hgetall(`${prefix}invocation:inv_1`);
    expect(inv.status).toBe("completed");

    const completion = await redis.get(keys(prefix).completion("inv_1"));
    expect(JSON.parse(completion ?? "{}").status).toBe("completed");
  });

  it("schedules retries on failure", async () => {
    const { worker, redis } = createWorker(async () => {
      throw new Error("boom");
    });

    const msg: StreamMessage = {
      messageId: "2-0",
      invocationId: "inv_retry",
      handler: "handler",
      args: JSON.stringify({}),
      attempt: "0",
    };

    const workerAny = worker as unknown as {
      processMessage: (serviceName: string, message: StreamMessage) => Promise<void>;
    };

    await workerAny.processMessage("svc", msg);

    const timerKey = keys(prefix).timers();
    const entries = await redis.zrangebyscore(timerKey, "-inf", "+inf");
    expect(entries).toHaveLength(1);
  });

  it("moves to DLQ after retries exhausted", async () => {
    const { worker, dlq } = createWorker(
      async () => {
        throw new Error("fail");
      },
      "service",
      0,
    );

    const msg: StreamMessage = {
      messageId: "3-0",
      invocationId: "inv_dlq",
      handler: "handler",
      args: JSON.stringify({}),
      attempt: "0",
    };

    const workerAny = worker as unknown as {
      processMessage: (serviceName: string, message: StreamMessage) => Promise<void>;
    };

    await workerAny.processMessage("svc", msg);

    const entries = await dlq.list("svc");
    expect(entries).toHaveLength(1);
    expect(entries[0]?.invocationId).toBe("inv_dlq");
  });

  it("skips processing on lock conflict", async () => {
    let called = false;
    const { worker, redis } = createWorker(async () => {
      called = true;
      return "ok";
    }, "object");

    const lockKey = keys(prefix).lock("svc", "key");
    await redis.set(lockKey, "token");

    const msg: StreamMessage = {
      messageId: "4-0",
      invocationId: "inv_lock",
      handler: "handler",
      args: JSON.stringify({}),
      attempt: "0",
      key: "key",
    };

    const workerAny = worker as unknown as {
      processMessage: (serviceName: string, message: StreamMessage) => Promise<void>;
    };

    await workerAny.processMessage("svc", msg);

    expect(called).toBe(false);
  });

  it("timer loop re-enqueues sleep entries", async () => {
    const { worker, streamProducer, timerStore } = createWorker(async () => "ok");
    await timerStore.schedule(
      { invocationId: "inv_sleep", sequence: 1, type: "sleep" },
      Date.now() - 1,
    );

    const workerAny = worker as unknown as {
      running: boolean;
      abortController: AbortController;
      timerLoop: () => Promise<void>;
    };

    workerAny.running = true;
    const loop = workerAny.timerLoop();
    await new Promise((resolve) => setTimeout(resolve, 10));
    workerAny.running = false;
    workerAny.abortController.abort();
    await loop;

    expect(streamProducer.reenqueueCalls).toContain("inv_sleep");
  });

  it("reclaim loop processes stale messages", async () => {
    let handled = false;
    const { worker, redis } = createWorker(async () => {
      handled = true;
      return "ok";
    });

    const streamKey = keys(prefix).stream("svc");
    const groupName = keys(prefix).consumerGroup("svc");
    await redis.xgroup(streamKey, "CREATE", groupName, "0", "MKSTREAM");
    await redis.xadd(
      streamKey,
      "*",
      "invocationId",
      "inv_reclaim",
      "handler",
      "handler",
      "args",
      "{}",
      "attempt",
      "0",
    );
    await redis.xreadgroup(groupName, "other", "1", "0", streamKey, ">");

    const workerAny = worker as unknown as {
      running: boolean;
      abortController: AbortController;
      reclaimLoop: () => Promise<void>;
    };

    workerAny.running = true;
    const loop = workerAny.reclaimLoop();
    await new Promise((resolve) => setTimeout(resolve, 10));
    workerAny.running = false;
    workerAny.abortController.abort();
    await loop;

    expect(handled).toBe(true);
  });

  it("computeRetryDelay adds jitter to prevent thundering herd", async () => {
    const { computeRetryDelay } = await import("../worker");
    const retryConfig = {
      initialDelayMs: 1000,
      maxDelayMs: 30000,
      backoffMultiplier: 2,
    };

    // Test that jitter produces different values for the same input
    const delays = Array.from({ length: 100 }, () => computeRetryDelay(1, retryConfig));
    const unique = new Set(delays);
    expect(unique.size).toBeGreaterThan(1); // Should have different values due to jitter

    // Test that delays are within expected range (base * multiplier ± 25%)
    const expectedBase = retryConfig.initialDelayMs * retryConfig.backoffMultiplier; // 2000ms
    const minExpected = expectedBase * 0.75; // 1500ms
    const maxExpected = expectedBase * 1.25; // 2500ms

    for (const delay of delays) {
      expect(delay).toBeGreaterThanOrEqual(minExpected);
      expect(delay).toBeLessThanOrEqual(maxExpected);
    }

    // Test max delay capping with jitter
    const highAttempt = 10; // Would normally exceed maxDelayMs
    const cappedDelays = Array.from({ length: 10 }, () =>
      computeRetryDelay(highAttempt, retryConfig),
    );
    for (const delay of cappedDelays) {
      expect(delay).toBeLessThanOrEqual(retryConfig.maxDelayMs * 1.25);
    }
  });
});
