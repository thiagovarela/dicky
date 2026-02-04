import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createRedisClient } from "../../stores/redis";
import type { RedisClient } from "../../stores/redis";
import { LuaScriptsImpl } from "../../lua";
import { clearRedis, redisUrl, startRedis, stopRedis } from "../integration/setup";
import { perfEnabled } from "../setup";

(perfEnabled ? describe : describe.skip)("Lua Scripts: Performance", () => {
  const prefix = "test:lua:perf:";
  let redis: RedisClient | null = null;
  let scripts: LuaScriptsImpl;

  beforeAll(async () => {
    await startRedis();
    redis = await createRedisClient({ url: redisUrl });
    scripts = new LuaScriptsImpl();
    await scripts.load(redis);
  });

  afterAll(async () => {
    if (redis) {
      await redis.quit();
    }
    await stopRedis();
  });

  it("timer-poll completes quickly", async () => {
    const timerKey = `${prefix}timers`;
    const now = Date.now();
    await clearRedis(prefix);

    for (let i = 0; i < 100; i += 1) {
      await redis.zadd(timerKey, String(now - 1000), JSON.stringify({ invocationId: `inv-${i}` }));
    }

    const durations: number[] = [];
    for (let i = 0; i < 20; i += 1) {
      const start = performance.now();
      await scripts.eval(redis, "timer-poll", [timerKey], [String(now), "5"]);
      durations.push(performance.now() - start);
    }

    const avg = durations.reduce((sum, value) => sum + value, 0) / durations.length;
    expect(avg).toBeLessThan(7);
  });
});
