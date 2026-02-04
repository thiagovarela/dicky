import { keys } from "../utils";
import type { DLQEntry, Invocation, InvocationId } from "../types";
import type { RedisClient } from "./redis";

export interface DLQStore {
  push(invocation: Invocation, error: string): Promise<void>;
  list(service: string, opts?: { limit?: number }): Promise<DLQEntry[]>;
  retry(id: InvocationId): Promise<void>;
  clear(service: string): Promise<void>;
}

export class DLQStoreImpl implements DLQStore {
  constructor(
    private redis: RedisClient,
    private prefix: string,
  ) {}

  async push(invocation: Invocation, error: string): Promise<void> {
    const failedAt = Date.now();
    const entry: DLQEntry = {
      invocationId: invocation.id,
      service: invocation.service,
      handler: invocation.handler,
      args: invocation.args,
      error,
      attempt: invocation.attempt,
      failedAt,
      streamMessageId: (invocation as { streamMessageId?: string }).streamMessageId ?? "",
    };

    const dlqKey = keys(this.prefix).dlq(invocation.service);
    const entryKey = this.entryKey(invocation.service, invocation.id);
    await this.redis.set(entryKey, JSON.stringify(entry));
    await this.redis.zadd(dlqKey, String(failedAt), invocation.id);
  }

  async list(service: string, opts?: { limit?: number }): Promise<DLQEntry[]> {
    const dlqKey = keys(this.prefix).dlq(service);
    const limit = opts?.limit ?? 100;
    const ids = await this.redis.zrangebyscore(
      dlqKey,
      "-inf",
      "+inf",
      undefined,
      "LIMIT",
      "0",
      String(limit),
    );

    const entries: DLQEntry[] = [];
    for (const id of ids) {
      const raw = await this.redis.get(this.entryKey(service, id));
      if (raw) {
        entries.push(JSON.parse(raw) as DLQEntry);
      }
    }

    return entries.sort((a, b) => a.failedAt - b.failedAt);
  }

  async retry(id: InvocationId): Promise<void> {
    const invKey = keys(this.prefix).invocation(id);
    const invocation = await this.redis.hgetall(invKey);
    if (!invocation.service) {
      return;
    }

    const streamKey = keys(this.prefix).stream(invocation.service);
    const handler = invocation.handler ?? "";
    const args = invocation.args ?? "";
    const attempt = invocation.attempt ?? "0";

    await this.redis.xadd(
      streamKey,
      "*",
      ...flattenFields({
        invocationId: id,
        handler,
        args,
        attempt,
        ...(invocation.key ? { key: invocation.key } : {}),
      }),
    );

    await this.redis.hset(invKey, "status", "pending");
    await this.redis.hset(invKey, "updatedAt", String(Date.now()));

    const dlqKey = keys(this.prefix).dlq(invocation.service);
    await this.redis.zrem(dlqKey, id);
    await this.redis.del(this.entryKey(invocation.service, id));
  }

  async clear(service: string): Promise<void> {
    const dlqKey = keys(this.prefix).dlq(service);
    const ids = await this.redis.zrangebyscore(dlqKey, "-inf", "+inf");
    await this.redis.zrem(dlqKey, ...ids);
    await this.redis.del(ids.map((id) => this.entryKey(service, id)));
  }

  private entryKey(service: string, id: string): string {
    return `${keys(this.prefix).dlq(service)}:entry:${id}`;
  }
}

function flattenFields(fields: Record<string, string>): string[] {
  return Object.entries(fields).flatMap(([key, value]) => [key, value]);
}
