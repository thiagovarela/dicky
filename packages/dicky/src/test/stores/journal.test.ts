import { describe, expect, it } from "bun:test";
import { JournalStoreImpl } from "../../stores/journal.js";
import { MockRedisClient } from "../../stores/redis.js";
import type { JournalEntry } from "../../types.js";
import { MockLuaScripts } from "./helpers.js";

const prefix = "test:journal:";

describe("JournalStore", () => {
  it("writes once and rejects duplicates", async () => {
    const redis = new MockRedisClient();
    const scripts = new MockLuaScripts();
    const store = new JournalStoreImpl(redis, prefix, scripts);

    const entry: JournalEntry = {
      invocationId: "inv_1",
      sequence: 0,
      type: "run",
      name: "step",
      status: "completed",
      createdAt: Date.now(),
      completedAt: Date.now(),
    };

    expect(await store.write(entry)).toBe(true);
    expect(await store.write(entry)).toBe(false);
  });

  it("completes pending entries", async () => {
    const redis = new MockRedisClient();
    const scripts = new MockLuaScripts();
    const store = new JournalStoreImpl(redis, prefix, scripts);

    const entry: JournalEntry = {
      invocationId: "inv_2",
      sequence: 1,
      type: "sleep",
      name: "wait",
      status: "pending",
      createdAt: Date.now(),
    };

    await store.write(entry);
    await store.complete("inv_2", 1, JSON.stringify({ ok: true }));

    const loaded = await store.get("inv_2", 1);
    expect(loaded?.status).toBe("completed");
    expect(loaded?.result).toBe(JSON.stringify({ ok: true }));
  });

  it("returns entries ordered by sequence", async () => {
    const redis = new MockRedisClient();
    const scripts = new MockLuaScripts();
    const store = new JournalStoreImpl(redis, prefix, scripts);

    await store.write({
      invocationId: "inv_3",
      sequence: 2,
      type: "run",
      name: "two",
      status: "completed",
      createdAt: Date.now(),
      completedAt: Date.now(),
    });

    await store.write({
      invocationId: "inv_3",
      sequence: 1,
      type: "run",
      name: "one",
      status: "completed",
      createdAt: Date.now(),
      completedAt: Date.now(),
    });

    const all = await store.getAll("inv_3");
    expect(all.map((entry) => entry.sequence)).toEqual([1, 2]);
  });
});
