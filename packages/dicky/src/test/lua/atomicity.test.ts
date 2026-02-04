import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { createRedisClient } from "../../stores/redis";
import type { RedisClient } from "../../stores/redis";
import { LuaScriptsImpl } from "../../lua";
import { clearRedis, integrationEnabled, redisUrl, startRedis, stopRedis } from "../integration/setup";

(integrationEnabled ? describe : describe.skip)("Lua Scripts: Atomicity", () => {
  const prefix = "test:lua:atomic:";
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

  it("timer-poll removes exactly matching entries", async () => {
    const timerKey = `${prefix}timers`;
    const now = Date.now();

    for (let i = 0; i < 20; i += 1) {
      await redis.zadd(timerKey, String(now - 1000), JSON.stringify({ invocationId: `inv-${i}` }));
    }

    const polled = (await scripts.eval(
      redis,
      "timer-poll",
      [timerKey],
      [String(now), "5"],
    )) as string[];

    expect(polled).toHaveLength(5);

    const remaining = await redis.zrangebyscore(timerKey, "-inf", "+inf");
    expect(remaining).toHaveLength(15);
  });

  it("lock-release respects ownership", async () => {
    const lockKey = `${prefix}lock`;
    await redis.set(lockKey, "owner");

    const results = await Promise.all([
      scripts.eval(redis, "lock-release", [lockKey], ["owner"]),
      scripts.eval(redis, "lock-release", [lockKey], ["other"]),
    ]);

    const successes = results.filter((value) => value === 1).length;
    expect(successes).toBe(1);
  });
});
