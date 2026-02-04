import { AwakeableError } from "../errors";
import { flattenFields, keys } from "../utils";
import type { JournalEntry } from "../types";
import type { RedisClient } from "./redis";
import type { JournalStore } from "./journal";
import type { StreamProducer } from "./stream";

export interface AwakeableEntry {
  invocationId: string;
  sequence: number;
  status: "pending" | "resolved" | "rejected";
  result?: string;
  error?: string;
}

export interface AwakeableStore {
  create(entry: AwakeableEntry, awakeableId: string): Promise<void>;
  resolve(awakeableId: string, value?: unknown): Promise<void>;
  reject(awakeableId: string, error: string): Promise<void>;
}

export class AwakeableStoreImpl implements AwakeableStore {
  constructor(
    private redis: RedisClient,
    private prefix: string,
    private journal: JournalStore,
    private streamProducer: StreamProducer,
  ) {}

  async create(entry: AwakeableEntry, awakeableId: string): Promise<void> {
    const awakeableKey = keys(this.prefix).awakeable(awakeableId);
    await this.redis.hmset(
      awakeableKey,
      ...flattenFields({
        invocationId: entry.invocationId,
        sequence: String(entry.sequence),
        status: "pending",
      }),
    );
  }

  async resolve(awakeableId: string, value?: unknown): Promise<void> {
    const awakeableKey = keys(this.prefix).awakeable(awakeableId);
    const entry = await this.redis.hgetall(awakeableKey);
    if (!entry.invocationId) {
      throw new AwakeableError(awakeableId, "not found");
    }
    if (entry.status !== "pending") {
      throw new AwakeableError(awakeableId, `already ${entry.status}`);
    }

    const serialized = value !== undefined ? JSON.stringify(value) : undefined;
    await this.redis.hmset(
      awakeableKey,
      ...flattenFields({
        status: "resolved",
        ...(serialized != null ? { result: serialized } : {}),
      }),
    );

    await this.journal.complete(entry.invocationId, Number(entry.sequence), serialized);
    await this.streamProducer.reenqueue(entry.invocationId);
  }

  async reject(awakeableId: string, error: string): Promise<void> {
    const awakeableKey = keys(this.prefix).awakeable(awakeableId);
    const entry = await this.redis.hgetall(awakeableKey);
    if (!entry.invocationId) {
      throw new AwakeableError(awakeableId, "not found");
    }
    if (entry.status !== "pending") {
      throw new AwakeableError(awakeableId, `already ${entry.status}`);
    }

    await this.redis.hmset(awakeableKey, ...flattenFields({ status: "rejected", error }));

    const journalKey = keys(this.prefix).journal(entry.invocationId);
    const raw = await this.redis.hget(journalKey, String(entry.sequence));
    if (raw) {
      const journalEntry = JSON.parse(raw) as JournalEntry;
      journalEntry.status = "failed";
      journalEntry.error = error;
      journalEntry.completedAt = Date.now();
      await this.redis.hset(journalKey, String(entry.sequence), JSON.stringify(journalEntry));
    }

    await this.streamProducer.reenqueue(entry.invocationId);
  }
}
