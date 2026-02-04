import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LuaScriptsImpl } from "../lua";
import { keys } from "../utils";
import type { RedisClient } from "../stores/redis";
import { integrationEnabled, setupTestRedis, teardownTestRedis } from "./setup";

(integrationEnabled ? describe : describe.skip)("LuaScripts", () => {
  const prefix = "test:lua:";
  let redis: RedisClient | null = null;
  let scripts: LuaScriptsImpl;

  beforeAll(async () => {
    const client = await setupTestRedis({ prefix });
    if (!client) {
      throw new Error("Redis not available");
    }
    redis = client;
    scripts = new LuaScriptsImpl();
    await scripts.load(redis);
  });

  afterAll(async () => {
    if (redis) {
      await teardownTestRedis(redis, prefix);
    }
  });

  it("loads scripts and caches SHAs", () => {
    expect(scripts.list()).toHaveLength(3);
    expect(scripts.getSha("timer-poll")).toBeTruthy();
  });

  it("handles NOSCRIPT by reloading", async () => {
    const timerKey = keys(prefix).timers();
    await redis.zadd(timerKey, String(Date.now() - 10), JSON.stringify({ id: "t1" }));

    await redis.scriptFlush();
    const results = (await scripts.eval(
      redis,
      "timer-poll",
      [timerKey],
      [String(Date.now()), "10"],
    )) as string[];

    expect(results).toHaveLength(1);
  });

  it("returns expired timers", async () => {
    const timerKey = keys(prefix).timers();
    const now = Date.now();
    const pastEntry = { invocationId: "inv_1", type: "sleep" };
    const futureEntry = { invocationId: "inv_2", type: "sleep" };

    await redis.zadd(timerKey, String(now - 10), JSON.stringify(pastEntry));
    await redis.zadd(timerKey, String(now + 10000), JSON.stringify(futureEntry));

    const results = (await scripts.eval(
      redis,
      "timer-poll",
      [timerKey],
      [String(now), "10"],
    )) as string[];

    expect(results).toHaveLength(1);
    expect(JSON.parse(results[0] ?? "{}")).toEqual(pastEntry);

    const remaining = await redis.zrangebyscore(timerKey, "-inf", "+inf");
    expect(remaining).toHaveLength(1);
  });

  it("renews and releases locks", async () => {
    const lockKey = keys(prefix).lock("orders", "1");
    const token = "token";

    await redis.set(lockKey, token);

    const renewed = await scripts.eval(redis, "lock-renew", [lockKey], [token, "10000"]);
    expect(renewed).toBe(1);

    const released = await scripts.eval(redis, "lock-release", [lockKey], [token]);
    expect(released).toBe(1);

    expect(await redis.get(lockKey)).toBeNull();
  });
});
