import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { createRedisClient, IoredisClient } from "../../stores/redis";
import type { RedisClient } from "../../stores/redis";
import { LuaScriptsImpl } from "../../lua";
import {
  clearRedis,
  integrationEnabled,
  redisUrl,
  startRedis,
  stopRedis,
} from "../integration/setup";

(integrationEnabled ? describe : describe.skip)("Lua: lock-acquire", () => {
  const prefix = "test:lua:lock:";
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

  it("acquires unheld lock", async () => {
    const lockKey = `${prefix}counter:user-1`;
    const token = "token-abc123";

    const result = await scripts.eval(redis, "lock-acquire", [lockKey], [token, "30000"]);
    expect(result).toBe(1);

    const holder = await redis.get(lockKey);
    expect(holder).toBe(token);
  });

  it("fails to acquire held lock", async () => {
    const lockKey = `${prefix}counter:user-1`;

    await scripts.eval(redis, "lock-acquire", [lockKey], ["token-1", "30000"]);
    const result = await scripts.eval(redis, "lock-acquire", [lockKey], ["token-2", "30000"]);

    expect(result).toBe(0);
  });

  it("sets TTL on lock", async () => {
    const lockKey = `${prefix}counter:user-1`;

    await scripts.eval(redis, "lock-acquire", [lockKey], ["token", "5000"]);

    if (redis instanceof IoredisClient) {
      const ttl = await redis.raw.pttl(lockKey);
      expect(ttl).toBeGreaterThan(0);
    }
  });

  it("handles concurrent acquisition attempts", async () => {
    const lockKey = `${prefix}counter:user-concurrent`;
    const tokens = Array.from({ length: 10 }, (_, index) => `token-${index}`);

    const results = await Promise.all(
      tokens.map((token) => scripts.eval(redis, "lock-acquire", [lockKey], [token, "30000"])),
    );

    const successCount = results.filter((value) => value === 1).length;
    expect(successCount).toBe(1);
  });
});
