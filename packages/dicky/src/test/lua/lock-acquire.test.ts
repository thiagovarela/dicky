import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { createRedisClient, IoredisClient } from "../../stores/redis";
import type { RedisClient } from "../../stores/redis";
import { clearRedis, integrationEnabled, redisUrl, startRedis, stopRedis } from "../integration/setup";

(integrationEnabled ? describe : describe.skip)("Native: lock-acquire", () => {
  const prefix = "test:lua:lock:";
  let redis: RedisClient | null = null;

  beforeAll(async () => {
    await startRedis();
    redis = await createRedisClient({ url: redisUrl });
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

    const acquired = await redis.setIfNotExists(lockKey, token, 30_000);
    expect(acquired).toBe(true);

    const holder = await redis.get(lockKey);
    expect(holder).toBe(token);
  });

  it("fails to acquire held lock", async () => {
    const lockKey = `${prefix}counter:user-1`;

    await redis.setIfNotExists(lockKey, "token-1", 30_000);
    const acquired = await redis.setIfNotExists(lockKey, "token-2", 30_000);

    expect(acquired).toBe(false);
  });

  it("sets TTL on lock", async () => {
    const lockKey = `${prefix}counter:user-1`;

    await redis.setIfNotExists(lockKey, "token", 5000);

    if (redis instanceof IoredisClient) {
      const ttl = await redis.raw.pttl(lockKey);
      expect(ttl).toBeGreaterThan(0);
    }
  });
});
