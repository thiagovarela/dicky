import { RedisClient as BunRedisClient } from "bun";
import type { RedisClient, XReadGroupResult } from "../stores/redis";

export type BunRedisClientOptions = ConstructorParameters<typeof BunRedisClient>[1];

export class BunRedisClientAdapter implements RedisClient {
  constructor(private client: BunRedisClient) {}

  get raw(): BunRedisClient {
    return this.client;
  }

  async xadd(key: string, id: string, ...fields: string[]): Promise<string> {
    const result = await this.send("XADD", [key, id, ...fields]);
    if (typeof result !== "string") {
      throw new Error("XADD returned non-string result");
    }
    return result;
  }

  async xreadgroup(
    group: string,
    consumer: string,
    count: string,
    block: string,
    ...streams: string[]
  ): Promise<XReadGroupResult> {
    const result = await this.send("XREADGROUP", [
      "GROUP",
      group,
      consumer,
      "COUNT",
      count,
      "BLOCK",
      block,
      "STREAMS",
      ...streams,
    ]);
    return (result as XReadGroupResult) ?? null;
  }

  async xack(key: string, group: string, ...ids: string[]): Promise<number> {
    const result = await this.send("XACK", [key, group, ...ids]);
    return Number(result ?? 0);
  }

  async xgroup(key: string, ...args: string[]): Promise<void> {
    if (args.length === 0) {
      return;
    }
    const [action, ...rest] = args;
    if (!action) {
      return;
    }
    await this.send("XGROUP", [action, key, ...rest]);
  }

  async xautoclaim(
    key: string,
    group: string,
    consumer: string,
    minIdle: string,
    start: string,
    count: string,
  ): Promise<[string, Array<[string, string[]]>]> {
    const result = await this.send("XAUTOCLAIM", [
      key,
      group,
      consumer,
      minIdle,
      start,
      "COUNT",
      count,
    ]);
    return result as [string, Array<[string, string[]]>];
  }

  async hget(key: string, field: string): Promise<string | null> {
    const result = await this.send("HGET", [key, field]);
    return result as string | null;
  }

  async hset(key: string, field: string, value: string): Promise<number> {
    const result = await this.send("HSET", [key, field, value]);
    return Number(result ?? 0);
  }

  async hsetnx(key: string, field: string, value: string): Promise<number> {
    const result = await this.send("HSETNX", [key, field, value]);
    return Number(result ?? 0);
  }

  async hincrby(key: string, field: string, value: number): Promise<number> {
    const result = await this.send("HINCRBY", [key, field, String(value)]);
    return Number(result ?? 0);
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    const result = await this.send("HDEL", [key, ...fields]);
    return Number(result ?? 0);
  }

  async hmset(key: string, ...pairs: string[]): Promise<void> {
    if (pairs.length === 0) {
      return;
    }
    await this.send("HSET", [key, ...pairs]);
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    const result = await this.send("HGETALL", [key]);
    return (result as Record<string, string>) ?? {};
  }

  async hlen(key: string): Promise<number> {
    const result = await this.send("HLEN", [key]);
    return Number(result ?? 0);
  }

  async set(key: string, value: string): Promise<void> {
    await this.send("SET", [key, value]);
  }

  async setIfNotExists(key: string, value: string, ttlMs: number): Promise<boolean> {
    const result = await this.send("SET", [key, value, "PX", String(ttlMs), "NX"]);
    return result === "OK";
  }

  async pexpire(key: string, ttlMs: number): Promise<number> {
    const result = await this.send("PEXPIRE", [key, String(ttlMs)]);
    return Number(result ?? 0);
  }

  async get(key: string): Promise<string | null> {
    const result = await this.send("GET", [key]);
    return result as string | null;
  }

  async del(key: string | string[]): Promise<number> {
    const keys = Array.isArray(key) ? key : [key];
    const result = await this.send("DEL", keys);
    return Number(result ?? 0);
  }

  async rpush(key: string, ...values: string[]): Promise<number> {
    const result = await this.send("RPUSH", [key, ...values]);
    return Number(result ?? 0);
  }

  async blpop(key: string, timeoutSeconds: number): Promise<[string, string] | null> {
    const result = await this.send("BLPOP", [key, String(timeoutSeconds)]);
    return (result as [string, string] | null) ?? null;
  }

  async zadd(key: string, score: string, member: string): Promise<number> {
    const result = await this.send("ZADD", [key, score, member]);
    return Number(result ?? 0);
  }

  async zrangebyscore(
    key: string,
    min: string | number,
    max: string | number,
    withScores?: "WITHSCORES",
    limit?: string,
    offset?: string,
    count?: string,
  ): Promise<string[]> {
    const args: Array<string | number> = [key, min, max];

    if (withScores) {
      args.push(withScores);
    }

    if (limit && offset && count) {
      args.push(limit, offset, count);
    }

    const result = await this.send("ZRANGEBYSCORE", args);
    return (result as string[]) ?? [];
  }

  async zrem(key: string, ...members: string[]): Promise<number> {
    const result = await this.send("ZREM", [key, ...members]);
    return Number(result ?? 0);
  }

  async scriptLoad(script: string): Promise<string> {
    const result = await this.send("SCRIPT", ["LOAD", script]);
    return result as string;
  }

  async scriptFlush(): Promise<void> {
    await this.send("SCRIPT", ["FLUSH"]);
  }

  async evalsha(sha: string, numKeys: string, ...args: string[]): Promise<unknown> {
    return this.send("EVALSHA", [sha, numKeys, ...args]);
  }

  async ping(): Promise<string> {
    const result = await this.send("PING", []);
    return result as string;
  }

  async quit(): Promise<void> {
    this.client.close();
  }

  private async send(command: string, args: Array<string | number>): Promise<unknown> {
    return this.client.send(
      command,
      args.map((value) => String(value)),
    );
  }
}

export function bunAdapter(client: BunRedisClient): RedisClient {
  return new BunRedisClientAdapter(client);
}

export async function createBunAdapter(
  url?: string,
  options?: BunRedisClientOptions,
): Promise<RedisClient> {
  const client = new BunRedisClient(url, options);
  await client.connect();
  return new BunRedisClientAdapter(client);
}
