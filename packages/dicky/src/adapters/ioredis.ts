import Redis, { type RedisOptions } from "ioredis";
import type { RedisClient } from "../stores/redis";
import { IoredisClient, createIoredisClient } from "../stores/redis";

export type IoredisOptions = RedisOptions;

export function ioredisAdapter(client: Redis): RedisClient {
  return new IoredisClient(client);
}

export async function createIoredisAdapter(url: string): Promise<RedisClient> {
  return createIoredisClient(url);
}
