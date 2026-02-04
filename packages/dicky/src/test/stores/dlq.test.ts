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
});
