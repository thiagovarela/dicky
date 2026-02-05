import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { RedisClient as BunRedisClient } from "bun";
import { bunAdapter, createBunAdapter } from "../../adapters/bun";
import { LuaScriptsImpl } from "../../lua";
import type { RedisClient } from "../../stores/redis";
import { clearRedis, redisUrl, startRedis, stopRedis } from "../integration/setup";
import { integrationEnabled } from "../setup";

(integrationEnabled ? describe : describe.skip)("Bun Redis adapter", () => {
  const prefix = "test:adapter:bun:";
  let raw: BunRedisClient | null = null;
  let adapter: RedisClient | null = null;

  beforeAll(async () => {
    await startRedis();
    raw = new BunRedisClient(redisUrl);
    await raw.connect();
    adapter = bunAdapter(raw);
  });

  beforeEach(async () => {
    await clearRedis(prefix);
  });

  afterAll(async () => {
    if (adapter) {
      await adapter.quit();
    }
    await stopRedis();
  });

  it("supports basic commands", async () => {
    const key = `${prefix}string`;
    await adapter!.set(key, "value");
    expect(await adapter!.get(key)).toBe("value");

    const hashKey = `${prefix}hash`;
    expect(await adapter!.hsetnx(hashKey, "field", "one")).toBe(1);
    expect(await adapter!.hsetnx(hashKey, "field", "two")).toBe(0);

    const listKey = `${prefix}list`;
    await adapter!.rpush(listKey, "a");
    const popped = await adapter!.blpop(listKey, 1);
    expect(popped).toEqual([listKey, "a"]);
  });

  it("executes Lua scripts", async () => {
    const scripts = new LuaScriptsImpl();
    await scripts.load(adapter!);

    const lockKey = `${prefix}lock`;
    const acquired = await scripts.eval(adapter!, "lock-acquire", [lockKey], ["token", "5000"]);
    expect(acquired).toBe(1);

    const released = await scripts.eval(adapter!, "lock-release", [lockKey], ["token"]);
    expect(released).toBe(1);
  });

  it("creates a client from URL", async () => {
    const client = await createBunAdapter(redisUrl);
    const key = `${prefix}client`;
    await client.set(key, "ok");
    expect(await client.get(key)).toBe("ok");
    await client.quit();
  });
});
