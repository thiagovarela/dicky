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

  it("journal-write is atomic under concurrent access", async () => {
    const journalKey = `${prefix}journal`;
    const entry = { invocationId: "atomic", sequence: 0, type: "run" };

    const results = await Promise.all(
      Array.from({ length: 100 }, () =>
        scripts.eval(redis, "journal-write", [journalKey], ["0", JSON.stringify(entry)]),
      ),
    );

    const successes = results.filter((value) => value === 1).length;
    const failures = results.filter((value) => value === 0).length;

    expect(successes).toBe(1);
    expect(failures).toBe(99);

    const count = await redis.hlen(journalKey);
    expect(count).toBe(1);
  });

  it("timer-poll removes exactly matching entries", async () => {
    const timerKey = `${prefix}timers`;
    const now = Date.now();

    for (let i = 0; i < 50; i += 1) {
      await redis.zadd(timerKey, String(now - 1000), JSON.stringify({ invocationId: `inv-${i}` }));
    }

    const polled = (await scripts.eval(
      redis,
      "timer-poll",
      [timerKey],
      [String(now), "10"],
    )) as string[];

    expect(polled).toHaveLength(10);

    const remaining = await redis.zrangebyscore(timerKey, "-inf", "+inf");
    expect(remaining).toHaveLength(40);
  });

  it("lock acquisition/release with concurrent access", async () => {
    const lockKey = `${prefix}lock`;
    const tokens = Array.from({ length: 10 }, (_, index) => `token-${index}`);

    const acquireResults = await Promise.all(
      tokens.map((token) => scripts.eval(redis, "lock-acquire", [lockKey], [token, "30000"])),
    );

    const successes = acquireResults.filter((value) => value === 1).length;
    expect(successes).toBe(1);

    const ownerToken = tokens[acquireResults.findIndex((value) => value === 1)];
    await scripts.eval(redis, "lock-release", [lockKey], [ownerToken]);

    const newAcquire = await scripts.eval(redis, "lock-acquire", [lockKey], ["new-token", "30000"]);
    expect(newAcquire).toBe(1);
  });
});
