import { keys } from "../utils";
import type { RedisClient } from "./redis";
import type { LuaScripts } from "./scripts";

export interface TimerEntry {
  invocationId: string;
  sequence?: number;
  type: "sleep" | "delayed" | "retry";
}

export interface TimerStore {
  schedule(entry: TimerEntry, wakeAtMs: number): Promise<void>;
  pollExpired(nowMs: number, limit: number): Promise<TimerEntry[]>;
}

export class TimerStoreImpl implements TimerStore {
  constructor(
    private redis: RedisClient,
    private prefix: string,
    private scripts: LuaScripts,
  ) {}

  async schedule(entry: TimerEntry, wakeAtMs: number): Promise<void> {
    const timerKey = keys(this.prefix).timers();
    await this.redis.zadd(timerKey, String(wakeAtMs), JSON.stringify(entry));
  }

  async pollExpired(nowMs: number, limit: number): Promise<TimerEntry[]> {
    const timerKey = keys(this.prefix).timers();
    const results = (await this.scripts.eval(
      this.redis,
      "timer-poll",
      [timerKey],
      [String(nowMs), String(limit)],
    )) as string[] | null;

    return (results ?? []).map((entry) => JSON.parse(entry) as TimerEntry);
  }
}
