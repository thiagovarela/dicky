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
  hsetnx(key: string, field: string, value: string): Promise<number>;
  hincrby(key: string, field: string, value: number): Promise<number>;
  hdel(key: string, ...fields: string[]): Promise<number>;
  hmset(key: string, ...pairs: string[]): Promise<void>;
  hgetall(key: string): Promise<Record<string, string>>;
  hlen(key: string): Promise<number>;

  // Key operations
  set(key: string, value: string): Promise<void>;
  setIfNotExists(key: string, value: string, ttlMs: number): Promise<boolean>;
  pexpire(key: string, ttlMs: number): Promise<number>;
  get(key: string): Promise<string | null>;
  del(key: string | string[]): Promise<number>;

  // List operations
  rpush(key: string, ...values: string[]): Promise<number>;
  blpop(key: string, timeoutSeconds: number): Promise<[string, string] | null>;

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
  scriptFlush(): Promise<void>;
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
  | { type: "string"; value: string; expiresAt?: number }
  | { type: "hash"; value: Map<string, string>; expiresAt?: number }
  | { type: "zset"; value: Map<string, number>; expiresAt?: number }
  | { type: "stream"; value: StreamData; expiresAt?: number }
  | { type: "list"; value: string[]; expiresAt?: number };

export class MockRedisClient implements RedisClient {
  private store = new Map<string, RedisValue>();
  private listWaiters = new Map<string, Array<(value: string) => void>>();
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
    block: string,
    ...streams: string[]
  ): Promise<XReadGroupResult> {
    const result = await this.run("xreadgroup", [group, consumer, count, block, ...streams], () => {
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

    if (!result) {
      const blockMs = Number.parseInt(block, 10);
      if (blockMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, blockMs));
      }
    }

    return result;
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

  async hsetnx(key: string, field: string, value: string): Promise<number> {
    return this.run("hsetnx", [key, field, value], () => {
      const hash = this.getOrCreateHash(key);
      if (hash.has(field)) {
        return 0;
      }
      hash.set(field, value);
      return 1;
    });
  }

  async hincrby(key: string, field: string, value: number): Promise<number> {
    return this.run("hincrby", [key, field, value], () => {
      const hash = this.getOrCreateHash(key);
      const current = Number.parseInt(hash.get(field) ?? "0", 10);
      const next = current + value;
      hash.set(field, String(next));
      return next;
    });
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    return this.run("hdel", [key, ...fields], () => {
      const hash = this.getHash(key);
      if (!hash) {
        return 0;
      }
      let removed = 0;
      for (const field of fields) {
        if (hash.delete(field)) {
          removed += 1;
        }
      }
      return removed;
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

  async setIfNotExists(key: string, value: string, ttlMs: number): Promise<boolean> {
    return this.run("setIfNotExists", [key, value, ttlMs], () => {
      const existing = this.getEntry(key);
      if (existing) {
        return false;
      }
      this.store.set(key, { type: "string", value, expiresAt: Date.now() + ttlMs });
      return true;
    });
  }

  async pexpire(key: string, ttlMs: number): Promise<number> {
    return this.run("pexpire", [key, ttlMs], () => {
      const entry = this.getEntry(key);
      if (!entry) {
        return 0;
      }
      entry.expiresAt = Date.now() + ttlMs;
      return 1;
    });
  }

  async get(key: string): Promise<string | null> {
    return this.run("get", [key], () => {
      const entry = this.getStringValue(key);
      return entry?.value ?? null;
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

  async rpush(key: string, ...values: string[]): Promise<number> {
    return this.run("rpush", [key, ...values], () => {
      let list = this.getList(key);
      if (!list) {
        list = this.getOrCreateList(key);
      }

      for (const value of values) {
        const waiters = this.listWaiters.get(key);
        const waiter = waiters?.shift();
        if (waiter) {
          waiter(value);
          if (waiters && waiters.length === 0) {
            this.listWaiters.delete(key);
          }
          continue;
        }
        list.push(value);
      }

      return list.length;
    });
  }

  async blpop(key: string, timeoutSeconds: number): Promise<[string, string] | null> {
    return this.run("blpop", [key, timeoutSeconds], async () => {
      const list = this.getList(key);
      if (list && list.length > 0) {
        const value = list.shift();
        return value != null ? [key, value] : null;
      }

      if (timeoutSeconds <= 0) {
        return null;
      }

      return new Promise<[string, string] | null>((resolve) => {
        let settled = false;
        const waiters = this.listWaiters.get(key) ?? [];
        const waiter = (value: string) => {
          if (settled) {
            return;
          }
          settled = true;
          clearTimeout(timer);
          resolve([key, value]);
        };
        waiters.push(waiter);
        this.listWaiters.set(key, waiters);

        const timer = setTimeout(() => {
          if (settled) {
            return;
          }
          settled = true;
          const current = this.listWaiters.get(key);
          if (current) {
            const index = current.indexOf(waiter);
            if (index >= 0) {
              current.splice(index, 1);
            }
            if (current.length === 0) {
              this.listWaiters.delete(key);
            }
          }
          resolve(null);
        }, timeoutSeconds * 1000);
      });
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

  async scriptFlush(): Promise<void> {
    return this.run("scriptFlush", [], () => {
      for (const key of this.store.keys()) {
        if (key.startsWith("script:")) {
          this.store.delete(key);
        }
      }
    });
  }

  async evalsha(sha: string, numKeys: string, ...args: string[]): Promise<unknown> {
    return this.run("evalsha", [sha, numKeys, ...args], () => {
      const script = this.store.get(`script:${sha}`);
      if (!script || script.type !== "string") {
        throw new Error(`NOSCRIPT ${sha}`);
      }

      const keyCount = Number.parseInt(numKeys, 10);
      const keys = args.slice(0, keyCount);
      const argv = args.slice(keyCount);
      const content = script.value;

      if (content.includes("ZRANGEBYSCORE") && content.includes("ZREM")) {
        return this.evalTimerPoll(keys[0], argv[0], argv[1]);
      }

      if (content.includes("PEXPIRE")) {
        return this.evalLockRenew(keys[0], argv[0], argv[1]);
      }

      if (content.includes("DEL") && content.includes("GET")) {
        return this.evalLockRelease(keys[0], argv[0]);
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

  private getEntry(key: string): RedisValue | undefined {
    const entry = this.store.get(key);
    if (!entry) {
      return undefined;
    }
    if (entry.expiresAt != null && entry.expiresAt <= Date.now()) {
      this.store.delete(key);
      return undefined;
    }
    return entry;
  }

  private getStream(key: string): StreamData | undefined {
    const entry = this.getEntry(key);
    if (entry && entry.type === "stream") {
      return entry.value;
    }
    return undefined;
  }

  private getStringValue(
    key: string,
  ): { type: "string"; value: string; expiresAt?: number } | undefined {
    const entry = this.getEntry(key);
    if (!entry || entry.type !== "string") {
      return undefined;
    }
    return entry;
  }

  private evalTimerPoll(key?: string, now?: string, limit?: string): string[] {
    if (!key) {
      return [];
    }
    const zset = this.getZSet(key);
    if (!zset) {
      return [];
    }

    const max = Number.parseFloat(now ?? "0");
    const count = Number.parseInt(limit ?? "0", 10);
    const entries = [...zset.entries()]
      .filter(([, score]) => score <= max)
      .sort(([, a], [, b]) => a - b)
      .slice(0, count);

    const results = entries.map(([member]) => member);
    for (const [member] of entries) {
      zset.delete(member);
    }
    return results;
  }

  private evalLockRenew(key?: string, token?: string, ttlMs?: string): number {
    if (!key || !token || !ttlMs) {
      return 0;
    }
    const entry = this.getStringValue(key);
    if (!entry || entry.value !== token) {
      return 0;
    }
    entry.expiresAt = Date.now() + Number.parseInt(ttlMs, 10);
    return 1;
  }

  private evalLockRelease(key?: string, token?: string): number {
    if (!key || !token) {
      return 0;
    }
    const entry = this.getStringValue(key);
    if (!entry || entry.value !== token) {
      return 0;
    }
    this.store.delete(key);
    return 1;
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
    const entry = this.getEntry(key);
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
    const entry = this.getEntry(key);
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

  private getList(key: string): string[] | undefined {
    const entry = this.getEntry(key);
    if (entry && entry.type === "list") {
      return entry.value;
    }
    return undefined;
  }

  private getOrCreateList(key: string): string[] {
    const existing = this.getList(key);
    if (existing) {
      return existing;
    }
    const list: string[] = [];
    this.store.set(key, { type: "list", value: list });
    return list;
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
      await new Promise((resolve) => setTimeout(resolve, this.delayMs + 1));
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
    const xgroup = this.redis.xgroup.bind(this.redis) as unknown as (
      ...values: string[]
    ) => Promise<unknown>;
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

  async hsetnx(key: string, field: string, value: string): Promise<number> {
    return this.redis.hsetnx(key, field, value);
  }

  async hincrby(key: string, field: string, value: number): Promise<number> {
    return this.redis.hincrby(key, field, value);
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    return this.redis.hdel(key, ...fields);
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

  async setIfNotExists(key: string, value: string, ttlMs: number): Promise<boolean> {
    const result = await this.redis.set(key, value, "PX", ttlMs, "NX");
    return result === "OK";
  }

  async pexpire(key: string, ttlMs: number): Promise<number> {
    return this.redis.pexpire(key, ttlMs);
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

  async rpush(key: string, ...values: string[]): Promise<number> {
    return this.redis.rpush(key, ...values);
  }

  async blpop(key: string, timeoutSeconds: number): Promise<[string, string] | null> {
    const result = await this.redis.blpop(key, timeoutSeconds);
    if (!result) {
      return null;
    }
    const [listKey, value] = result as [string, string];
    return [listKey, value];
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

    const zrange = this.redis.zrangebyscore.bind(this.redis) as unknown as (
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

  async scriptFlush(): Promise<void> {
    await this.redis.script("FLUSH");
  }

  async evalsha(sha: string, numKeys: string, ...args: string[]): Promise<unknown> {
    const evalsha = this.redis.evalsha.bind(this.redis) as unknown as (
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
