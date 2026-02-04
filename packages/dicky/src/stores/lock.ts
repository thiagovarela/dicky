import { LockConflictError } from "../errors.js";
import { keys, newLockToken } from "../utils.js";
import type { RedisClient } from "./redis.js";
import type { LuaScripts } from "./scripts.js";

export interface LockGuard {
  readonly objectName: string;
  readonly key: string;
  readonly token: string;
  release(): Promise<void>;
}

export interface LockManager {
  acquire(objectName: string, key: string): Promise<LockGuard>;
}

export class LockManagerImpl implements LockManager {
  constructor(
    private redis: RedisClient,
    private scripts: LuaScripts,
    private prefix: string,
    private ttlMs: number,
    private renewMs: number,
  ) {}

  async acquire(objectName: string, key: string): Promise<LockGuard> {
    const lockKey = keys(this.prefix).lock(objectName, key);
    const token = newLockToken();

    const acquired = await this.scripts.eval(
      this.redis,
      "lock-acquire",
      [lockKey],
      [token, String(this.ttlMs)],
    );

    if (normalizeLuaResult(acquired) !== 1) {
      throw new LockConflictError(objectName, key);
    }

    return new LockGuardImpl(
      this.redis,
      this.scripts,
      lockKey,
      token,
      this.ttlMs,
      this.renewMs,
      objectName,
      key,
    );
  }
}

class LockGuardImpl implements LockGuard {
  private renewTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private redis: RedisClient,
    private scripts: LuaScripts,
    private lockKey: string,
    readonly token: string,
    private ttlMs: number,
    renewMs: number,
    readonly objectName: string,
    readonly key: string,
  ) {
    this.renewTimer = setInterval(() => {
      this.scripts
        .eval(this.redis, "lock-renew", [this.lockKey], [this.token, String(this.ttlMs)])
        .catch(() => this.stopRenew());
    }, renewMs);
  }

  async release(): Promise<void> {
    this.stopRenew();
    await this.scripts.eval(this.redis, "lock-release", [this.lockKey], [this.token]);
  }

  private stopRenew(): void {
    if (this.renewTimer) {
      clearInterval(this.renewTimer);
      this.renewTimer = null;
    }
  }
}

function normalizeLuaResult(result: unknown): number {
  if (typeof result === "number") {
    return result;
  }
  if (typeof result === "string") {
    const parsed = Number.parseInt(result, 10);
    return Number.isNaN(parsed) ? 0 : parsed;
  }
  if (typeof result === "boolean") {
    return result ? 1 : 0;
  }
  return 0;
}
