import type { RedisAdapter } from "../types";
import type { RedisClient } from "../stores/redis";

export interface RedisPoolOptions {
  size: number;
}

export class RedisAdapterPool {
  private clients: RedisClient[] = [];
  private reserved = new Set<RedisClient>();
  private cursor = 0;

  private constructor(
    private adapter: RedisAdapter,
    private url: string,
    private size: number,
  ) {}

  static async create(adapter: RedisAdapter, url: string, size: number): Promise<RedisAdapterPool> {
    const pool = new RedisAdapterPool(adapter, url, size);
    await pool.init();
    return pool;
  }

  async reserve(): Promise<RedisClient> {
    const available = this.clients.find((client) => !this.reserved.has(client));
    if (available) {
      this.reserved.add(available);
      return available;
    }

    const client = await this.createClient();
    this.clients.push(client);
    this.reserved.add(client);
    return client;
  }

  get(): RedisClient {
    const available = this.clients.filter((client) => !this.reserved.has(client));
    if (available.length === 0) {
      throw new Error("No available Redis clients in pool");
    }
    const client = available[this.cursor % available.length];
    this.cursor += 1;
    return client!;
  }

  async close(): Promise<void> {
    await Promise.all(this.clients.map((client) => client.quit()));
    this.clients = [];
    this.reserved.clear();
  }

  private async init(): Promise<void> {
    const target = Math.max(1, this.size);
    for (let i = 0; i < target; i += 1) {
      this.clients.push(await this.createClient());
    }
  }

  private async createClient(): Promise<RedisClient> {
    switch (this.adapter) {
      case "bun": {
        const { createBunAdapter } = await import("./bun");
        return createBunAdapter(this.url);
      }
      case "ioredis":
      default: {
        const { createIoredisAdapter } = await import("./ioredis");
        return createIoredisAdapter(this.url);
      }
    }
  }
}
