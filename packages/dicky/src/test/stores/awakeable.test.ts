import { describe, expect, it } from "bun:test";
import { AwakeableStoreImpl } from "../../stores/awakeable.js";
import { JournalStoreImpl } from "../../stores/journal.js";
import { MockRedisClient } from "../../stores/redis.js";
import type { JournalEntry } from "../../types.js";
import { MockLuaScripts } from "./helpers.js";

class TestStreamProducer {
  readonly reenqueued: string[] = [];

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
    this.reenqueued.push(invocationId);
  }

  async reenqueueRetry(_invocationId: string): Promise<void> {
    return undefined;
  }

  async enqueueDelayed(_invocationId: string): Promise<void> {
    return undefined;
  }

  async dispatch(
    _service: string,
    _handler: string,
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
    return "";
  }
}

const prefix = "test:awakeable:";

describe("AwakeableStore", () => {
  it("creates pending awakeables", async () => {
    const redis = new MockRedisClient();
    const scripts = new MockLuaScripts();
    const journal = new JournalStoreImpl(redis, prefix, scripts);
    const producer = new TestStreamProducer();
    const store = new AwakeableStoreImpl(redis, prefix, journal, producer);

    await store.create({ invocationId: "inv_1", sequence: 0, status: "pending" }, "awake_1");

    const entry = await redis.hgetall(`${prefix}awakeable:awake_1`);
    expect(entry.status).toBe("pending");
  });

  it("resolves awakeables and completes journal", async () => {
    const redis = new MockRedisClient();
    const scripts = new MockLuaScripts();
    const journal = new JournalStoreImpl(redis, prefix, scripts);
    const producer = new TestStreamProducer();
    const store = new AwakeableStoreImpl(redis, prefix, journal, producer);

    const entry: JournalEntry = {
      invocationId: "inv_2",
      sequence: 1,
      type: "awakeable",
      name: "wait",
      status: "pending",
      createdAt: Date.now(),
    };

    await journal.write(entry);
    await store.create({ invocationId: "inv_2", sequence: 1, status: "pending" }, "awake_2");
    await store.resolve("awake_2", { ok: true });

    const journalEntry = await journal.get("inv_2", 1);
    expect(journalEntry?.status).toBe("completed");
    expect(producer.reenqueued).toEqual(["inv_2"]);
  });

  it("rejects awakeables", async () => {
    const redis = new MockRedisClient();
    const scripts = new MockLuaScripts();
    const journal = new JournalStoreImpl(redis, prefix, scripts);
    const producer = new TestStreamProducer();
    const store = new AwakeableStoreImpl(redis, prefix, journal, producer);

    await journal.write({
      invocationId: "inv_3",
      sequence: 2,
      type: "awakeable",
      name: "wait",
      status: "pending",
      createdAt: Date.now(),
    });

    await store.create({ invocationId: "inv_3", sequence: 2, status: "pending" }, "awake_3");
    await store.reject("awake_3", "nope");

    const journalEntry = await journal.get("inv_3", 2);
    expect(journalEntry?.status).toBe("failed");
    await expect(store.resolve("awake_3", "later")).rejects.toThrow("already rejected");
  });
});
