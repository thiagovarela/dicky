import { keys } from "../utils.js";
import type { JournalEntry } from "../types.js";
import type { RedisClient } from "./redis.js";
import type { LuaScripts } from "./scripts.js";

export interface JournalStore {
  get(invocationId: string, sequence: number): Promise<JournalEntry | null>;
  write(entry: JournalEntry): Promise<boolean>;
  complete(invocationId: string, sequence: number, result?: string): Promise<void>;
  getAll(invocationId: string): Promise<JournalEntry[]>;
  length(invocationId: string): Promise<number>;
}

export class JournalStoreImpl implements JournalStore {
  constructor(
    private redis: RedisClient,
    private prefix: string,
    private scripts: LuaScripts,
  ) {}

  async get(invocationId: string, sequence: number): Promise<JournalEntry | null> {
    const key = keys(this.prefix).journal(invocationId);
    const raw = await this.redis.hget(key, String(sequence));
    return raw ? (JSON.parse(raw) as JournalEntry) : null;
  }

  async write(entry: JournalEntry): Promise<boolean> {
    const key = keys(this.prefix).journal(entry.invocationId);
    const result = await this.scripts.eval(
      this.redis,
      "journal-write",
      [key],
      [String(entry.sequence), JSON.stringify(entry)],
    );
    return normalizeLuaResult(result) === 1;
  }

  async complete(invocationId: string, sequence: number, result?: string): Promise<void> {
    const key = keys(this.prefix).journal(invocationId);
    const raw = await this.redis.hget(key, String(sequence));
    if (!raw) {
      return;
    }
    const entry = JSON.parse(raw) as JournalEntry;
    entry.status = "completed";
    if (result !== undefined) {
      entry.result = result;
    } else {
      delete entry.result;
    }
    entry.completedAt = Date.now();
    await this.redis.hset(key, String(sequence), JSON.stringify(entry));
  }

  async getAll(invocationId: string): Promise<JournalEntry[]> {
    const key = keys(this.prefix).journal(invocationId);
    const all = await this.redis.hgetall(key);
    return Object.values(all)
      .map((value) => JSON.parse(value) as JournalEntry)
      .sort((a, b) => a.sequence - b.sequence);
  }

  async length(invocationId: string): Promise<number> {
    const key = keys(this.prefix).journal(invocationId);
    return this.redis.hlen(key);
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
