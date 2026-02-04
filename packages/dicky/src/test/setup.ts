import { createRedisClient, IoredisClient, MockRedisClient } from "../stores/redis";
import type { RedisClient, RedisClientOptions } from "../stores/redis";

export const integrationEnabled = true;
export const perfEnabled = true;

export interface TestSetupOptions {
  prefix: string;
  redisUrl?: string;
  driver?: RedisClientOptions["driver"];
}

export async function setupTestRedis(options: TestSetupOptions): Promise<RedisClient | null> {
  const redisUrl = options.redisUrl ?? process.env.REDIS_URL ?? "redis://localhost:6379";

  if (!options.redisUrl && !process.env.REDIS_URL && !isDockerAvailable()) {
    throw new Error(
      "Docker is required for Redis-backed tests. Set REDIS_URL or start Docker.",
    );
  }

  if (!options.redisUrl && !process.env.REDIS_URL) {
    await ensureDockerRedis();
  }

  try {
    const clientOptions = {
      url: redisUrl,
      ...(options.driver ? { driver: options.driver } : {}),
    };
    const client = await createRedisClient(clientOptions);
    await clearPrefix(client, options.prefix);
    return client;
  } catch (error) {
    console.warn("Failed to connect to Redis for tests:", error);
    return null;
  }
}

export async function teardownTestRedis(client: RedisClient, prefix: string): Promise<void> {
  await clearPrefix(client, prefix);
  await client.quit();
}

async function ensureDockerRedis(): Promise<void> {
  const available = await ensureDockerDaemon();
  if (!available) {
    return;
  }

  try {
    const { exitCode } = Bun.spawnSync({
      cmd: ["docker", "compose", "-f", "docker-compose.yml", "ps", "-q", "redis"],
      stdout: "pipe",
      stderr: "pipe",
    });

    if (exitCode !== 0) {
      return;
    }

    const { stdout } = Bun.spawnSync({
      cmd: ["docker", "compose", "-f", "docker-compose.yml", "up", "-d", "redis"],
      stdout: "pipe",
      stderr: "pipe",
    });

    void stdout;
  } catch (error) {
    console.warn("Docker not available, skipping auto-start:", error);
  }
}


export function isDockerAvailable(): boolean {
  try {
    const result = Bun.spawnSync({
      cmd: ["docker", "info"],
      stdout: "pipe",
      stderr: "pipe",
    });
    return result.exitCode === 0;
  } catch {
    return false;
  }
}

export async function ensureDockerDaemon(): Promise<boolean> {
  return isDockerAvailable();
}

async function clearPrefix(client: RedisClient, prefix: string): Promise<void> {
  if (client instanceof MockRedisClient) {
    await client.clearPrefix(prefix);
    return;
  }

  if (client instanceof IoredisClient) {
    await clearPrefixWithScan(client, prefix);
  }
}

async function clearPrefixWithScan(client: IoredisClient, prefix: string): Promise<void> {
  let cursor = "0";
  do {
    const [next, keys] = await client.raw.scan(cursor, "MATCH", `${prefix}*`, "COUNT", "100");
    cursor = next;
    if (keys.length > 0) {
      await client.raw.del(...keys);
    }
  } while (cursor !== "0");
}
