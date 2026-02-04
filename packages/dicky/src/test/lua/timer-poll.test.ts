import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { createRedisClient } from "../../stores/redis";
import type { RedisClient } from "../../stores/redis";
import { LuaScriptsImpl } from "../../lua";
import { clearRedis, integrationEnabled, redisUrl, startRedis, stopRedis } from "../integration/setup";

(integrationEnabled ? describe : describe.skip)("Lua: timer-poll", () => {
  const prefix = "test:lua:timer:";
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

  it("returns only expired timers", async () => {
    const timerKey = `${prefix}timers`;
    const now = Date.now();

    await redis.zadd(timerKey, String(now - 1000), JSON.stringify({ invocationId: "inv-1" }));
    await redis.zadd(timerKey, String(now + 10000), JSON.stringify({ invocationId: "inv-2" }));

    const expired = (await scripts.eval(
      redis,
      "timer-poll",
      [timerKey],
      [String(now), "10"],
    )) as string[];

    expect(expired).toHaveLength(1);
  });

  it("removes expired timers", async () => {
    const timerKey = `${prefix}timers`;
    const now = Date.now();

    await redis.zadd(timerKey, String(now - 500), JSON.stringify({ invocationId: "inv-1" }));
    await redis.zadd(timerKey, String(now - 500), JSON.stringify({ invocationId: "inv-2" }));

    await scripts.eval(redis, "timer-poll", [timerKey], [String(now), "10"]);

    const remaining = await redis.zrangebyscore(timerKey, "-inf", "+inf");
    expect(remaining).toHaveLength(0);
  });

  it("respects limit parameter", async () => {
    const timerKey = `${prefix}timers`;
    const now = Date.now();

    for (let i = 0; i < 10; i += 1) {
      await redis.zadd(timerKey, String(now - 1000), JSON.stringify({ invocationId: `inv-${i}` }));
    }

    const expired = (await scripts.eval(
      redis,
      "timer-poll",
      [timerKey],
      [String(now), "5"],
    )) as string[];

    expect(expired).toHaveLength(5);
  });
});
