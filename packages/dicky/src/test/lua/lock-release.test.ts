import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { createRedisClient } from "../../stores/redis";
import type { RedisClient } from "../../stores/redis";
import { LuaScriptsImpl } from "../../lua";
import { clearRedis, redisUrl, startRedis, stopRedis } from "../integration/setup";

const integrationEnabled = process.env.DICKY_INTEGRATION === "1";

(integrationEnabled ? describe : describe.skip)("Lua: lock-release", () => {
  const prefix = "test:lua:lock-release:";
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

  it("releases owned lock", async () => {
    const lockKey = `${prefix}counter:user-1`;
    const token = "token-abc123";

    await redis.set(lockKey, token);

    const result = await scripts.eval(redis, "lock-release", [lockKey], [token]);
    expect(result).toBe(1);

    const holder = await redis.get(lockKey);
    expect(holder).toBeNull();
  });

  it("fails to release unowned lock", async () => {
    const lockKey = `${prefix}counter:user-1`;

    await redis.set(lockKey, "other-token");

    const result = await scripts.eval(redis, "lock-release", [lockKey], ["token"]);
    expect(result).toBe(0);

    const holder = await redis.get(lockKey);
    expect(holder).toBe("other-token");
  });
});
