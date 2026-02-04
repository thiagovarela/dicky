import type { RedisClient } from "../../stores/redis";
import type { LuaScripts } from "../../stores/scripts";

export class MockLuaScripts implements LuaScripts {
  async load(): Promise<void> {
    return undefined;
  }

  async eval(redis: RedisClient, script: string, keys: string[], args: string[]): Promise<unknown> {
    switch (script) {
      case "timer-poll":
        return this.timerPoll(redis, keys[0], args[0], args[1]);
      case "lock-renew":
        return this.lockRenew(redis, keys[0], args[0], args[1]);
      case "lock-release":
        return this.lockRelease(redis, keys[0], args[0]);
      default:
        throw new Error(`Unknown script ${script}`);
    }
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

  private async lockRenew(
    redis: RedisClient,
    lockKey?: string,
    token?: string,
    ttlMs?: string,
  ): Promise<number> {
    if (!lockKey || !token || !ttlMs) {
      return 0;
    }
    const current = await redis.get(lockKey);
    if (current !== token) {
      return 0;
    }
    await redis.pexpire(lockKey, Number.parseInt(ttlMs, 10));
    return 1;
  }

  private async lockRelease(redis: RedisClient, lockKey?: string, token?: string): Promise<number> {
    if (!lockKey || !token) {
      return 0;
    }
    const current = await redis.get(lockKey);
    if (current !== token) {
      return 0;
    }
    await redis.del(lockKey);
    return 1;
  }
}
