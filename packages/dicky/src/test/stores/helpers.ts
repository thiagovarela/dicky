import type { RedisClient } from "../../stores/redis.js";
import type { LuaScripts } from "../../stores/scripts.js";

interface LockState {
  token: string;
  expiresAt: number;
}

export class MockLuaScripts implements LuaScripts {
  private locks = new Map<string, LockState>();

  async eval(redis: RedisClient, script: string, keys: string[], args: string[]): Promise<unknown> {
    switch (script) {
      case "journal-write":
        return this.journalWrite(redis, keys[0], args[0], args[1]);
      case "timer-poll":
        return this.timerPoll(redis, keys[0], args[0], args[1]);
      case "lock-acquire":
        return this.lockAcquire(keys[0], args[0], args[1]);
      case "lock-renew":
        return this.lockRenew(keys[0], args[0], args[1]);
      case "lock-release":
        return this.lockRelease(keys[0], args[0]);
      default:
        throw new Error(`Unknown script ${script}`);
    }
  }

  private async journalWrite(
    redis: RedisClient,
    key?: string,
    sequence?: string,
    payload?: string,
  ): Promise<number> {
    if (!key || !sequence || payload == null) {
      return 0;
    }
    const existing = await redis.hget(key, sequence);
    if (existing) {
      return 0;
    }
    await redis.hset(key, sequence, payload);
    return 1;
  }

  private async timerPoll(
    redis: RedisClient,
    key?: string,
    now?: string,
    limit?: string,
  ): Promise<string[]> {
    if (!key) {
      return [];
    }
    const max = now ?? "0";
    const size = limit ?? "0";
    const entries = await redis.zrangebyscore(key, "-inf", max, undefined, "LIMIT", "0", size);
    if (entries.length > 0) {
      await redis.zrem(key, ...entries);
    }
    return entries;
  }

  private lockAcquire(lockKey?: string, token?: string, ttlMs?: string): number {
    if (!lockKey || !token || !ttlMs) {
      return 0;
    }
    const now = Date.now();
    const ttl = Number.parseInt(ttlMs, 10);
    const existing = this.locks.get(lockKey);
    if (existing && existing.expiresAt > now) {
      return 0;
    }
    this.locks.set(lockKey, { token, expiresAt: now + ttl });
    return 1;
  }

  private lockRenew(lockKey?: string, token?: string, ttlMs?: string): number {
    if (!lockKey || !token || !ttlMs) {
      return 0;
    }
    const existing = this.locks.get(lockKey);
    if (!existing || existing.token !== token) {
      return 0;
    }
    const ttl = Number.parseInt(ttlMs, 10);
    existing.expiresAt = Date.now() + ttl;
    return 1;
  }

  private lockRelease(lockKey?: string, token?: string): number {
    if (!lockKey || !token) {
      return 0;
    }
    const existing = this.locks.get(lockKey);
    if (!existing || existing.token !== token) {
      return 0;
    }
    this.locks.delete(lockKey);
    return 1;
  }
}
