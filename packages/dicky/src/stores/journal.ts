import { keys } from "../utils";
import type { JournalEntry } from "../types";
import type { RedisClient } from "./redis";

export interface JournalStore {
  get(invocationId: string, sequence: number): Promise<JournalEntry | null>;
  write(entry: JournalEntry): Promise<boolean>;
  complete(invocationId: string, sequence: number, result?: string): Promise<void>;
  fail(invocationId: string, sequence: number, error: string): Promise<void>;
  getAll(invocationId: string): Promise<JournalEntry[]>;
  length(invocationId: string): Promise<number>;
}

export class JournalStoreImpl implements JournalStore {
  constructor(
    private redis: RedisClient,
    private prefix: string,
  ) {}

  async get(invocationId: string, sequence: number): Promise<JournalEntry | null> {
    const key = keys(this.prefix).journal(invocationId);
    const raw = await this.redis.hget(key, String(sequence));
    return raw ? (JSON.parse(raw) as JournalEntry) : null;
  }

  async write(entry: JournalEntry): Promise<boolean> {
    const key = keys(this.prefix).journal(entry.invocationId);
    const result = await this.redis.hsetnx(key, String(entry.sequence), JSON.stringify(entry));
    return result === 1;
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

  async fail(invocationId: string, sequence: number, error: string): Promise<void> {
    const key = keys(this.prefix).journal(invocationId);
    const raw = await this.redis.hget(key, String(sequence));
    if (!raw) {
      return;
    }
    const entry = JSON.parse(raw) as JournalEntry;
    entry.status = "failed";
    entry.error = error;
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
