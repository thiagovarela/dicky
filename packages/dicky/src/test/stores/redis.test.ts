import { describe, expect, it } from "bun:test";
import { MockRedisClient, createRedisClient } from "../../stores/redis";
import { integrationEnabled, setupTestRedis, teardownTestRedis } from "../setup";

describe("MockRedisClient", () => {
  it("tracks calls and respects delays", async () => {
    const redis = new MockRedisClient({ delayMs: 10 });
    const start = Date.now();
    await redis.set("key", "value");
    const elapsed = Date.now() - start;

    expect(redis.calls.length).toBe(1);
    expect(elapsed).toBeGreaterThanOrEqual(10);
  });

  it("supports error injection", async () => {
    const redis = new MockRedisClient({
      errorInjector: (command) => (command === "get" ? new Error("boom") : null),
    });

    await redis.set("key", "value");
    await expect(redis.get("key")).rejects.toThrow("boom");
  });

  it("handles stream operations", async () => {
    const redis = new MockRedisClient();
    await redis.xgroup("stream", "CREATE", "group", "0", "MKSTREAM");
    const id = await redis.xadd("stream", "*", "field", "value");

    const result = await redis.xreadgroup("group", "consumer", "1", "0", "stream", ">");
    expect(result).not.toBeNull();
    expect(result?.[0]?.[0]).toBe("stream");
    expect(result?.[0]?.[1]?.[0]?.[0]).toBe(id);
  });

  it("supports runtime client selection", async () => {
    const custom = new MockRedisClient();
    const client = await createRedisClient({
      url: "redis://example",
      factory: async () => custom,
    });

    expect(client).toBe(custom);
  });
});

(integrationEnabled ? describe : describe.skip)("IoredisClient", () => {
  it("connects and pings", async () => {
    const prefix = "test:redis:";
    const client = await setupTestRedis({ prefix });
    if (!client) {
      throw new Error("Redis not available");
    }

    const redis = await createRedisClient(client);
    expect(await redis.ping()).toBe("PONG");

    await teardownTestRedis(client, prefix);
  });
});
