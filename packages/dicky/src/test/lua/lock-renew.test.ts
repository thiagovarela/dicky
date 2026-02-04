import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { createRedisClient, IoredisClient } from "../../stores/redis";
import type { RedisClient } from "../../stores/redis";
import { LuaScriptsImpl } from "../../lua";
import { clearRedis, redisUrl, startRedis, stopRedis } from "../integration/setup";

const integrationEnabled = process.env.DICKY_INTEGRATION === "1";

(integrationEnabled ? describe : describe.skip)("Lua: lock-renew", () => {
  const prefix = "test:lua:lock-renew:";
  let redis: RedisClient;
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
    await redis.quit();
    await stopRedis();
  });

  it("renews owned lock", async () => {
    const lockKey = `${prefix}counter:user-1`;
    const token = "token-abc123";

    await redis.set(lockKey, token);
    await redis.pexpire(lockKey, 1000);

    const result = await scripts.eval(redis, "lock-renew", [lockKey], [token, "3000"]);
    expect(result).toBe(1);

    if (redis instanceof IoredisClient) {
      const ttl = await redis.raw.pttl(lockKey);
      expect(ttl).toBeGreaterThan(0);
    }
  });

  it("fails to renew stolen lock", async () => {
    const lockKey = `${prefix}counter:user-1`;

    await redis.set(lockKey, "other");

    const result = await scripts.eval(redis, "lock-renew", [lockKey], ["token", "3000"]);
    expect(result).toBe(0);
  });
});
