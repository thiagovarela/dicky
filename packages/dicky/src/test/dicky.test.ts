import { describe, expect, it } from "bun:test";
import { Dicky } from "../dicky";
import { service } from "../define";
import { MockRedisClient } from "../stores/redis";
import { keys } from "../utils";
import type { DickyConfig } from "../types";
import { TimeoutError } from "../errors";

const prefix = "test:dicky:";

function createConfig(redis: MockRedisClient, overrides?: Partial<DickyConfig>): DickyConfig {
  return {
    redis: {
      host: "localhost",
      port: 6379,
      keyPrefix: prefix,
      client: redis,
    },
    worker: {
      concurrency: 1,
      pollIntervalMs: 1,
      lockTtlMs: 50,
      lockRenewMs: 10,
      ackTimeoutMs: 50,
      timerPollIntervalMs: 1,
    },
    retry: {
      maxRetries: 1,
      initialDelayMs: 1,
      maxDelayMs: 10,
      backoffMultiplier: 2,
    },
    ...overrides,
  };
}

function createDicky(overrides?: Partial<DickyConfig>) {
  const redis = new MockRedisClient();
  const config = createConfig(redis, overrides);
  const dicky = new Dicky(config);
  return { dicky, redis };
}

describe("Dicky", () => {
  it("registers services with use", () => {
    const { dicky } = createDicky();
    dicky.use(
      service("users", {
        getProfile: async (_ctx, { id }: { id: string }) => ({ id }),
      }),
    );

    expect(dicky.getService("users")).toBeDefined();
  });

  it("throws on registration after start", async () => {
    const { dicky } = createDicky();
    dicky.use(service("test", { handler: async () => {} }));
    await dicky.start();

    expect(() => dicky.use(service("other", { handler: async () => {} }))).toThrow();
    await dicky.stop();
  });

  it("sends invocations", async () => {
    const { dicky, redis } = createDicky();
    dicky.use(service("test", { echo: async (_ctx, { msg }: { msg: string }) => msg }));
    await dicky.start();

    const id = await dicky.send("test", "echo", { msg: "hello" });
    expect(id).toMatch(/^inv_/);

    const record = await redis.hgetall(keys(prefix).invocation(id));
    expect(record.service).toBe("test");

    await dicky.stop();
  });

  it("throws for unknown services", async () => {
    const { dicky } = createDicky();
    await dicky.start();

    await expect(dicky.send("unknown", "handler", {})).rejects.toThrow("Unknown service");

    await dicky.stop();
  });

  it("invokes handlers and waits for completion", async () => {
    const { dicky, redis } = createDicky();
    dicky.use(
      service("test", {
        echo: async (_ctx, { msg }: { msg: string }) => ({ echoed: msg }),
      }),
    );
    await dicky.start();

    const invocationId = "inv_test";
    const completionKey = keys(prefix).completion(invocationId);
    await redis.set(
      completionKey,
      JSON.stringify({ status: "completed", result: '{"echoed":"hi"}' }),
    );

    const dickyAny = dicky as unknown as {
      send: (...args: unknown[]) => Promise<string>;
    };
    dickyAny.send = async () => invocationId;

    const result = await dicky.invoke("test", "echo", { msg: "hi" });
    expect(result).toEqual({ echoed: "hi" });

    await dicky.stop();
  });

  it("times out when completion is missing", async () => {
    const { dicky } = createDicky({ worker: { concurrency: 0 } });
    dicky.use(service("test", { handler: async () => "ok" }));
    await dicky.start();

    await expect(dicky.invoke("test", "handler", {}, { timeoutMs: 50 })).rejects.toBeInstanceOf(
      TimeoutError,
    );

    await dicky.stop();
  });

  it("lists DLQ entries", async () => {
    const { dicky } = createDicky();
    dicky.use(service("test", { handler: async () => "ok" }));
    await dicky.start();

    const entries = await dicky.listDLQ("test");
    expect(Array.isArray(entries)).toBe(true);

    await dicky.stop();
  });
});
