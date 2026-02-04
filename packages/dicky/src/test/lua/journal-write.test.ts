import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { createRedisClient } from "../../stores/redis";
import type { RedisClient } from "../../stores/redis";
import { LuaScriptsImpl } from "../../lua";
import {
  clearRedis,
  integrationEnabled,
  redisUrl,
  startRedis,
  stopRedis,
} from "../integration/setup";

(integrationEnabled ? describe : describe.skip)("Lua: journal-write", () => {
  const prefix = "test:lua:journal:";
  let redis: RedisClient | null = null;
  let scripts: LuaScriptsImpl;

  beforeAll(async () => {
    await startRedis();
    redis = await createRedisClient({ url: redisUrl });
    scripts = new LuaScriptsImpl();
    await scripts.load(redis);
  });

  beforeEach(async () => {
    await clearRedis(prefix);
  });

  afterAll(async () => {
    if (redis) {
      await redis.quit();
    }
    await stopRedis();
  });

  it("creates new journal entry", async () => {
    const journalKey = `${prefix}inv-123`;
    const entry = { invocationId: "inv-123", sequence: 0, type: "run", status: "pending" };

    const result = await scripts.eval(
      redis,
      "journal-write",
      [journalKey],
      ["0", JSON.stringify(entry)],
    );
    expect(result).toBe(1);

    const stored = await redis.hget(journalKey, "0");
    expect(JSON.parse(stored ?? "{}")).toEqual(entry);
  });

  it("returns 0 for duplicate write", async () => {
    const journalKey = `${prefix}inv-123`;
    const entry = { invocationId: "inv-123", sequence: 0, type: "run", status: "pending" };

    await scripts.eval(redis, "journal-write", [journalKey], ["0", JSON.stringify(entry)]);
    const result = await scripts.eval(
      redis,
      "journal-write",
      [journalKey],
      ["0", JSON.stringify(entry)],
    );

    expect(result).toBe(0);
  });

  it("allows different sequences", async () => {
    const journalKey = `${prefix}inv-123`;

    await scripts.eval(redis, "journal-write", [journalKey], ["0", JSON.stringify({ seq: 0 })]);
    await scripts.eval(redis, "journal-write", [journalKey], ["1", JSON.stringify({ seq: 1 })]);
    await scripts.eval(redis, "journal-write", [journalKey], ["2", JSON.stringify({ seq: 2 })]);

    const count = await redis.hlen(journalKey);
    expect(count).toBe(3);
  });

  it("handles concurrent writes", async () => {
    const journalKey = `${prefix}inv-concurrent`;
    const entry = { invocationId: "inv-concurrent", sequence: 0, type: "run" };

    const results = await Promise.all(
      Array.from({ length: 10 }, () =>
        scripts.eval(redis, "journal-write", [journalKey], ["0", JSON.stringify(entry)]),
      ),
    );

    const successCount = results.filter((value) => value === 1).length;
    const failCount = results.filter((value) => value === 0).length;

    expect(successCount).toBe(1);
    expect(failCount).toBe(9);
  });
});
