import { describe, expect, it } from "bun:test";
import { DLQStoreImpl } from "../../stores/dlq";
import { MockRedisClient } from "../../stores/redis";
import type { Invocation } from "../../types";

const prefix = "test:dlq:";

describe("DLQStore", () => {
  it("pushes and lists entries", async () => {
    const redis = new MockRedisClient();
    const store = new DLQStoreImpl(redis, prefix);

    const invocation: Invocation = {
      id: "inv_1",
      service: "orders",
      handler: "process",
      args: { id: 1 },
      status: "failed",
      attempt: 2,
      maxRetries: 3,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };

    await store.push(invocation, "boom");
    const entries = await store.list("orders");

    expect(entries).toHaveLength(1);
    expect(entries[0]?.error).toBe("boom");
  });

  it("retries entries", async () => {
    const redis = new MockRedisClient();
    const store = new DLQStoreImpl(redis, prefix);

    await redis.hmset(
      `${prefix}invocation:inv_2`,
      "id",
      "inv_2",
      "service",
      "orders",
      "handler",
      "process",
      "args",
      "{}",
      "status",
      "failed",
      "attempt",
      "0",
      "maxRetries",
      "3",
      "createdAt",
      "0",
      "updatedAt",
      "0",
    );

    await store.push(
      {
        id: "inv_2",
        service: "orders",
        handler: "process",
        args: {},
        status: "failed",
        attempt: 0,
        maxRetries: 3,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      },
      "fail",
    );

    await redis.xgroup(`${prefix}stream:orders`, "CREATE", "group", "0", "MKSTREAM");
    await store.retry("inv_2");

    const result = await redis.xreadgroup(
      "group",
      "worker",
      "1",
      "0",
      `${prefix}stream:orders`,
      ">",
    );
    expect(result?.[0]?.[1]?.[0]?.[1]).toContain("inv_2");

    const entries = await store.list("orders");
    expect(entries).toHaveLength(0);
  });

  it("resets attempt counter on retry", async () => {
    const redis = new MockRedisClient();
    const store = new DLQStoreImpl(redis, prefix);

    // Create an invocation that has exhausted retries (attempt >= maxRetries)
    await redis.hmset(
      `${prefix}invocation:inv_retry_reset`,
      "id",
      "inv_retry_reset",
      "service",
      "orders",
      "handler",
      "process",
      "args",
      '{"test": true}',
      "status",
      "failed",
      "attempt",
      "3", // Exhausted retries (>= maxRetries=2)
      "maxRetries",
      "2",
      "createdAt",
      String(Date.now()),
      "updatedAt",
      String(Date.now()),
    );

    // Add to DLQ
    await store.push(
      {
        id: "inv_retry_reset",
        service: "orders",
        handler: "process",
        args: { test: true },
        status: "failed",
        attempt: 3,
        maxRetries: 2,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      },
      "Exhausted retries",
    );

    // Retry from DLQ
    await redis.xgroup(`${prefix}stream:orders`, "CREATE", "group", "0", "MKSTREAM");
    await store.retry("inv_retry_reset");

    // Verify invocation hash has attempt reset to 0
    const invocation = await redis.hgetall(`${prefix}invocation:inv_retry_reset`);
    expect(invocation.attempt).toBe("0");
    expect(invocation.status).toBe("pending");

    // Verify stream message has attempt reset to 0
    const result = await redis.xreadgroup("group", "worker", "1", "0", `${prefix}stream:orders`, ">");
    const fields = result?.[0]?.[1]?.[0]?.[1] as string[];
    const attemptIndex = fields.indexOf("attempt");
    expect(attemptIndex).toBeGreaterThan(-1);
    expect(fields[attemptIndex + 1]).toBe("0");

    // Verify removed from DLQ
    const dlqEntries = await store.list("orders");
    expect(dlqEntries).toHaveLength(0);
  });
});
