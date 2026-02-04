import Redis from "ioredis";

export type XReadGroupResult = Array<[string, Array<[string, string[]]>]> | null;

export interface RedisClient {
  // Stream operations
  xadd(key: string, id: string, ...fields: string[]): Promise<string>;
  xreadgroup(
    group: string,
    consumer: string,
    count: string,
    block: string,
    ...streams: string[]
  ): Promise<XReadGroupResult>;
  xack(key: string, group: string, ...ids: string[]): Promise<number>;
  xgroup(key: string, ...args: string[]): Promise<void>;
  xautoclaim(
    key: string,
    group: string,
    consumer: string,
    minIdle: string,
    start: string,
    count: string,
  ): Promise<[string, Array<[string, string[]]>]>;

  // Hash operations
  hget(key: string, field: string): Promise<string | null>;
  hset(key: string, field: string, value: string): Promise<number>;
  hmset(key: string, ...pairs: string[]): Promise<void>;
  hgetall(key: string): Promise<Record<string, string>>;
  hlen(key: string): Promise<number>;

  // Key operations
  set(key: string, value: string): Promise<void>;
  get(key: string): Promise<string | null>;
  del(key: string | string[]): Promise<number>;

  // Sorted set operations
  zadd(key: string, score: string, member: string): Promise<number>;
  zrangebyscore(
    key: string,
    min: string | number,
    max: string | number,
    withScores?: "WITHSCORES",
    limit?: string,
    offset?: string,
    count?: string,
  ): Promise<string[]>;
  zrem(key: string, ...members: string[]): Promise<number>;

  // Script operations
  scriptLoad(script: string): Promise<string>;
  evalsha(sha: string, numKeys: string, ...args: string[]): Promise<unknown>;

  // Admin
  ping(): Promise<string>;

  // Close connection
  quit(): Promise<void>;
}

export interface MockRedisOptions {
  delayMs?: number;
  errorInjector?: (command: string, args: unknown[]) => Error | null;
}

interface StreamEntry {
  id: string;
  fields: string[];
}

interface PendingEntry {
  entry: StreamEntry;
  consumer: string;
  lastDelivered: number;
  deliveries: number;
}

interface StreamGroup {
  lastIndex: number;
  pending: Map<string, PendingEntry>;
}

interface StreamData {
  entries: StreamEntry[];
  lastId: number;
  groups: Map<string, StreamGroup>;
}

type RedisValue =
  | { type: "string"; value: string }
  | { type: "hash"; value: Map<string, string> }
  | { type: "zset"; value: Map<string, number> }
  | { type: "stream"; value: StreamData };

export class MockRedisClient implements RedisClient {
  private store = new Map<string, RedisValue>();
  private delayMs: number;
  private errorInjector: ((command: string, args: unknown[]) => Error | null) | null;

  readonly calls: Array<{ command: string; args: unknown[] }> = [];

  constructor(options: MockRedisOptions = {}) {
    this.delayMs = options.delayMs ?? 0;
    this.errorInjector = options.errorInjector ?? null;
  }

  async clearPrefix(prefix: string): Promise<void> {
    for (const key of this.store.keys()) {
      if (key.startsWith(prefix)) {
        this.store.delete(key);
      }
    }
  }

  async xadd(key: string, id: string, ...fields: string[]): Promise<string> {
    return this.run("xadd", [key, id, ...fields], () => {
      const stream = this.getOrCreateStream(key);
      const entryId = id === "*" ? this.nextStreamId(stream) : id;
      stream.entries.push({ id: entryId, fields: [...fields] });
      return entryId;
    });
  }

  async xreadgroup(
    group: string,
    consumer: string,
    count: string,
    _block: string,
    ...streams: string[]
  ): Promise<XReadGroupResult> {
    return this.run("xreadgroup", [group, consumer, count, _block, ...streams], () => {
      if (streams.length === 0) {
        return null;
      }

      const [streamKeys, streamIds] = splitStreams(streams);
      const results: Array<[string, Array<[string, string[]]>]> = [];
      const countNumber = Number.parseInt(count, 10);

      streamKeys.forEach((streamKey, index) => {
        const stream = this.getStream(streamKey);
        if (!stream) {
          return;
        }

        const groupData = this.getOrCreateGroup(stream, group);
        const startId = streamIds[index] ?? ">";
        const entries = this.selectEntries(stream, groupData, startId, countNumber);

        if (entries.length === 0) {
          return;
        }

        for (const entry of entries) {
          const pending = groupData.pending.get(entry.id);
          groupData.pending.set(entry.id, {
            entry,
            consumer,
            deliveries: (pending?.deliveries ?? 0) + 1,
            lastDelivered: Date.now(),
          });
        }

        results.push([streamKey, entries.map((entry) => [entry.id, [...entry.fields]])]);
      });

      return results.length > 0 ? results : null;
    });
  }

  async xack(key: string, group: string, ...ids: string[]): Promise<number> {
    return this.run("xack", [key, group, ...ids], () => {
      const stream = this.getStream(key);
      if (!stream) {
        return 0;
      }
      const groupData = stream.groups.get(group);
      if (!groupData) {
        return 0;
      }

      let removed = 0;
      for (const id of ids) {
        if (groupData.pending.delete(id)) {
          removed += 1;
        }
      }
      return removed;
    });
  }

  async xgroup(key: string, ...args: string[]): Promise<void> {
    return this.run("xgroup", [key, ...args], () => {
      if (args.length === 0) {
        return;
      }

      const [action, groupName] = args;
      if (!action) {
        return;
      }
      if (action.toUpperCase() !== "CREATE") {
        return;
      }

      if (!groupName) {
        return;
      }

      const stream = this.getOrCreateStream(key);
      if (!stream.groups.has(groupName)) {
        stream.groups.set(groupName, { lastIndex: -1, pending: new Map() });
      }
    });
  }

  async xautoclaim(
    key: string,
    group: string,
    consumer: string,
    minIdle: string,
    start: string,
    count: string,
  ): Promise<[string, Array<[string, string[]]>]> {
    return this.run("xautoclaim", [key, group, consumer, minIdle, start, count], () => {
      const stream = this.getStream(key);
      if (!stream) {
        return [start, []];
      }
      const groupData = stream.groups.get(group);
      if (!groupData) {
        return [start, []];
      }

      const minIdleMs = Number.parseInt(minIdle, 10);
      const now = Date.now();
      const limit = Number.parseInt(count, 10);
      const reclaimed: Array<[string, string[]]> = [];

      for (const [id, pending] of groupData.pending.entries()) {
        if (reclaimed.length >= limit) {
          break;
        }
        if (pending.lastDelivered + minIdleMs > now) {
          continue;
        }
        if (start !== "0-0" && id < start) {
          continue;
        }
        groupData.pending.set(id, {
          ...pending,
          consumer,
          lastDelivered: now,
          deliveries: pending.deliveries + 1,
        });
        reclaimed.push([id, [...pending.entry.fields]]);
      }

      return [start, reclaimed];
    });
  }

  async hget(key: string, field: string): Promise<string | null> {
    return this.run("hget", [key, field], () => {
      const hash = this.getHash(key);
      if (!hash) {
        return null;
      }
      return hash.get(field) ?? null;
    });
  }

  async hset(key: string, field: string, value: string): Promise<number> {
    return this.run("hset", [key, field, value], () => {
      const hash = this.getOrCreateHash(key);
      const exists = hash.has(field);
      hash.set(field, value);
      return exists ? 0 : 1;
    });
  }

  async hmset(key: string, ...pairs: string[]): Promise<void> {
    return this.run("hmset", [key, ...pairs], () => {
      const hash = this.getOrCreateHash(key);
      for (let index = 0; index < pairs.length; index += 2) {
        const field = pairs[index];
        const value = pairs[index + 1];
        if (field == null || value == null) {
          continue;
        }
        hash.set(field, value);
      }
    });
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return this.run("hgetall", [key], () => {
      const hash = this.getHash(key);
      if (!hash) {
        return {};
      }
      return Object.fromEntries(hash.entries());
    });
  }

  async hlen(key: string): Promise<number> {
    return this.run("hlen", [key], () => {
      const hash = this.getHash(key);
      return hash?.size ?? 0;
    });
  }

  async set(key: string, value: string): Promise<void> {
    return this.run("set", [key, value], () => {
      this.store.set(key, { type: "string", value });
    });
  }

  async get(key: string): Promise<string | null> {
    return this.run("get", [key], () => {
      const entry = this.store.get(key);
      if (!entry || entry.type !== "string") {
        return null;
      }
      return entry.value;
    });
  }

  async del(key: string | string[]): Promise<number> {
    return this.run("del", [key], () => {
      const keys = Array.isArray(key) ? key : [key];
      let removed = 0;
      for (const entryKey of keys) {
        if (this.store.delete(entryKey)) {
          removed += 1;
        }
      }
      return removed;
    });
  }

  async zadd(key: string, score: string, member: string): Promise<number> {
    return this.run("zadd", [key, score, member], () => {
      const zset = this.getOrCreateZSet(key);
      const exists = zset.has(member);
      zset.set(member, Number.parseFloat(score));
      return exists ? 0 : 1;
    });
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
    return this.run("zrangebyscore", [key, min, max, withScores, limit, offset, count], () => {
      const zset = this.getZSet(key);
      if (!zset) {
        return [];
      }

      const minValue = parseScore(min, Number.NEGATIVE_INFINITY);
      const maxValue = parseScore(max, Number.POSITIVE_INFINITY);

      let items = [...zset.entries()]
        .filter(([, score]) => score >= minValue && score <= maxValue)
        .sort(([, a], [, b]) => a - b);

      if (limit && offset && count) {
        const start = Number.parseInt(offset, 10);
        const length = Number.parseInt(count, 10);
        items = items.slice(start, start + length);
      }

      if (withScores === "WITHSCORES") {
        return items.flatMap(([member, score]) => [member, score.toString()]);
      }

      return items.map(([member]) => member);
    });
  }

  async zrem(key: string, ...members: string[]): Promise<number> {
    return this.run("zrem", [key, ...members], () => {
      const zset = this.getZSet(key);
      if (!zset) {
        return 0;
      }
      let removed = 0;
      for (const member of members) {
        if (zset.delete(member)) {
          removed += 1;
        }
      }
      return removed;
    });
  }

  async scriptLoad(script: string): Promise<string> {
    return this.run("scriptLoad", [script], () => {
      const sha = `mock-${Math.random().toString(36).slice(2)}`;
      this.store.set(`script:${sha}`, { type: "string", value: script });
      return sha;
    });
  }

  async evalsha(sha: string, _numKeys: string, ...args: string[]): Promise<unknown> {
    return this.run("evalsha", [sha, _numKeys, ...args], () => {
      const script = this.store.get(`script:${sha}`);
      if (!script || script.type !== "string") {
        throw new Error(`NOSCRIPT ${sha}`);
      }
      return script.value;
    });
  }

  async ping(): Promise<string> {
    return this.run("ping", [], () => "PONG");
  }

  async quit(): Promise<void> {
    return this.run("quit", [], () => undefined);
  }

  private getStream(key: string): StreamData | undefined {
    const entry = this.store.get(key);
    if (entry && entry.type === "stream") {
      return entry.value;
    }
    return undefined;
  }

  private getOrCreateStream(key: string): StreamData {
    const existing = this.getStream(key);
    if (existing) {
      return existing;
    }
    const stream: StreamData = {
      entries: [],
      lastId: 0,
      groups: new Map(),
    };
    this.store.set(key, { type: "stream", value: stream });
    return stream;
  }

  private getHash(key: string): Map<string, string> | undefined {
    const entry = this.store.get(key);
    if (entry && entry.type === "hash") {
      return entry.value;
    }
    return undefined;
  }

  private getOrCreateHash(key: string): Map<string, string> {
    const existing = this.getHash(key);
    if (existing) {
      return existing;
    }
    const hash = new Map<string, string>();
    this.store.set(key, { type: "hash", value: hash });
    return hash;
  }

  private getZSet(key: string): Map<string, number> | undefined {
    const entry = this.store.get(key);
    if (entry && entry.type === "zset") {
      return entry.value;
    }
    return undefined;
  }

  private getOrCreateZSet(key: string): Map<string, number> {
    const existing = this.getZSet(key);
    if (existing) {
      return existing;
    }
    const zset = new Map<string, number>();
    this.store.set(key, { type: "zset", value: zset });
    return zset;
  }

  private nextStreamId(stream: StreamData): string {
    stream.lastId += 1;
    return `${stream.lastId}-0`;
  }

  private getOrCreateGroup(stream: StreamData, group: string): StreamGroup {
    let groupData = stream.groups.get(group);
    if (!groupData) {
      groupData = { lastIndex: -1, pending: new Map() };
      stream.groups.set(group, groupData);
    }
    return groupData;
  }

  private selectEntries(
    stream: StreamData,
    groupData: StreamGroup,
    startId: string,
    count: number,
  ): StreamEntry[] {
    let startIndex = 0;
    if (startId === ">") {
      startIndex = groupData.lastIndex + 1;
    } else {
      startIndex = stream.entries.findIndex((entry) => entry.id > startId);
      if (startIndex < 0) {
        return [];
      }
    }

    const entries = stream.entries.slice(startIndex, startIndex + count);
    if (entries.length > 0) {
      const lastEntry = entries[entries.length - 1];
      if (lastEntry) {
        const lastIndex = stream.entries.findIndex((entry) => entry.id === lastEntry.id);
        if (lastIndex >= 0) {
          groupData.lastIndex = lastIndex;
        }
      }
    }
    return entries;
  }

  private async run<T>(command: string, args: unknown[], fn: () => T): Promise<T> {
    this.calls.push({ command, args });
    if (this.errorInjector) {
      const error = this.errorInjector(command, args);
      if (error) {
        throw error;
      }
    }
    if (this.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.delayMs));
    }
    return fn();
  }
}

export class IoredisClient implements RedisClient {
  constructor(private redis: Redis) {}

  get raw(): Redis {
    return this.redis;
  }

  async xadd(key: string, id: string, ...fields: string[]): Promise<string> {
    const result = await this.redis.xadd(key, id, ...fields);
    if (result == null) {
      throw new Error("XADD returned null");
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
    const result = await this.redis.xreadgroup(
      "GROUP",
      group,
      consumer,
      "COUNT",
      count,
      "BLOCK",
      block,
      "STREAMS",
      ...streams,
    );
    return result as XReadGroupResult;
  }

  async xack(key: string, group: string, ...ids: string[]): Promise<number> {
    return this.redis.xack(key, group, ...ids);
  }

  async xgroup(key: string, ...args: string[]): Promise<void> {
    if (args.length === 0) {
      return;
    }
    const [action, ...rest] = args;
    if (!action) {
      return;
    }
    const xgroup = this.redis.xgroup as unknown as (...values: string[]) => Promise<unknown>;
    await xgroup(action, key, ...rest);
  }

  async xautoclaim(
    key: string,
    group: string,
    consumer: string,
    minIdle: string,
    start: string,
    count: string,
  ): Promise<[string, Array<[string, string[]]>]> {
    const result = await this.redis.xautoclaim(
      key,
      group,
      consumer,
      minIdle,
      start,
      "COUNT",
      count,
    );
    return result as [string, Array<[string, string[]]>];
  }

  async hget(key: string, field: string): Promise<string | null> {
    return this.redis.hget(key, field);
  }

  async hset(key: string, field: string, value: string): Promise<number> {
    return this.redis.hset(key, field, value);
  }

  async hmset(key: string, ...pairs: string[]): Promise<void> {
    if (pairs.length === 0) {
      return;
    }
    await this.redis.hset(key, ...pairs);
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return this.redis.hgetall(key);
  }

  async hlen(key: string): Promise<number> {
    return this.redis.hlen(key);
  }

  async set(key: string, value: string): Promise<void> {
    await this.redis.set(key, value);
  }

  async get(key: string): Promise<string | null> {
    return this.redis.get(key);
  }

  async del(key: string | string[]): Promise<number> {
    if (Array.isArray(key)) {
      return this.redis.del(...key);
    }
    return this.redis.del(key);
  }

  async zadd(key: string, score: string, member: string): Promise<number> {
    return this.redis.zadd(key, score, member);
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

    const zrange = this.redis.zrangebyscore as unknown as (
      ...values: Array<string | number>
    ) => Promise<string[]>;
    return zrange(...args);
  }

  async zrem(key: string, ...members: string[]): Promise<number> {
    return this.redis.zrem(key, ...members);
  }

  async scriptLoad(script: string): Promise<string> {
    const result = await this.redis.script("LOAD", script);
    return result as string;
  }

  async evalsha(sha: string, numKeys: string, ...args: string[]): Promise<unknown> {
    const evalsha = this.redis.evalsha as unknown as (
      sha: string,
      numKeys: number,
      ...values: Array<string | number>
    ) => Promise<unknown>;
    return evalsha(sha, Number.parseInt(numKeys, 10), ...args);
  }

  async ping(): Promise<string> {
    return this.redis.ping();
  }

  async quit(): Promise<void> {
    await this.redis.quit();
  }
}

export type RedisDriver = "ioredis";

export interface RedisClientOptions {
  url: string;
  driver?: RedisDriver;
  factory?: (url: string) => Promise<RedisClient>;
}

export function isRedisClient(value: unknown): value is RedisClient {
  return !!value && typeof value === "object" && typeof (value as RedisClient).ping === "function";
}

export async function createIoredisClient(url: string): Promise<IoredisClient> {
  const redis = new Redis(url);
  await redis.ping();
  return new IoredisClient(redis);
}

export async function createRedisClient(
  options: RedisClientOptions | RedisClient,
): Promise<RedisClient> {
  if (isRedisClient(options)) {
    return options;
  }

  if (options.factory) {
    return options.factory(options.url);
  }

  const driver = options.driver ?? "ioredis";

  switch (driver) {
    case "ioredis":
    default:
      return createIoredisClient(options.url);
  }
}

function splitStreams(streams: string[]): [string[], string[]] {
  if (streams.length % 2 !== 0) {
    return [streams, []];
  }
  const half = streams.length / 2;
  return [streams.slice(0, half), streams.slice(half)];
}

function parseScore(value: string | number, fallback: number): number {
  if (typeof value === "number") {
    return value;
  }
  if (value === "-inf") {
    return Number.NEGATIVE_INFINITY;
  }
  if (value === "+inf") {
    return Number.POSITIVE_INFINITY;
  }
  const parsed = Number.parseFloat(value);
  return Number.isNaN(parsed) ? fallback : parsed;
}
