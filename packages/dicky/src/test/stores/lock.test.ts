import { describe, expect, it } from "bun:test";
import { LockManagerImpl } from "../../stores/lock";
import { MockRedisClient } from "../../stores/redis";
import { MockLuaScripts } from "./helpers";
import { LockConflictError } from "../../errors";

const prefix = "test:lock:";

describe("LockManager", () => {
  it("acquires and releases locks", async () => {
    const redis = new MockRedisClient();
    const scripts = new MockLuaScripts();
    const manager = new LockManagerImpl(redis, scripts, prefix, 1000, 100);

    const guard = await manager.acquire("cart", "1");
    expect(guard.token).toBeTruthy();

    await guard.release();
    await expect(manager.acquire("cart", "1")).resolves.toBeDefined();
  });

  it("rejects conflicting locks", async () => {
    const redis = new MockRedisClient();
    const scripts = new MockLuaScripts();
    const manager = new LockManagerImpl(redis, scripts, prefix, 1000, 100);

    await manager.acquire("cart", "2");
    await expect(manager.acquire("cart", "2")).rejects.toBeInstanceOf(LockConflictError);
  });

  it("auto-renews locks", async () => {
    const redis = new MockRedisClient();
    const scripts = new MockLuaScripts();
    const manager = new LockManagerImpl(redis, scripts, prefix, 50, 10);

    const guard = await manager.acquire("cart", "3");
    await new Promise((resolve) => setTimeout(resolve, 80));

    await expect(manager.acquire("cart", "3")).rejects.toBeInstanceOf(LockConflictError);
    await guard.release();
    await expect(manager.acquire("cart", "3")).resolves.toBeDefined();
  });
});
