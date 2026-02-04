import { buildRedisUrl } from "../../config";
import type { DickyConfig, InvocationStatus } from "../../types";
import { createRedisClient, IoredisClient } from "../../stores/redis";
import type { Dicky } from "../../dicky";
import { ensureDockerDaemon, isDockerAvailable } from "../setup";

export { integrationEnabled } from "../setup";

export const testConfig = {
  redis: {
    host: "localhost",
    port: 6379,
    keyPrefix: "test:",
  },
  keyPrefix: "test:",
};

export const redisUrl = process.env.REDIS_URL ?? buildRedisUrl(testConfig.redis);

export async function startRedis(): Promise<void> {
  if (process.env.REDIS_URL) {
    await waitForRedis();
    return;
  }

  if (!isDockerAvailable()) {
    const started = await ensureDockerDaemon();
    if (!started) {
      throw new Error(
        "Docker is required for integration tests. Set REDIS_URL or install/start Docker.",
      );
    }
  }

  const { exitCode } = Bun.spawnSync({
    cmd: ["docker", "compose", "-f", "docker-compose.yml", "ps", "-q", "redis"],
    stdout: "pipe",
    stderr: "pipe",
  });

  if (exitCode === 0) {
    Bun.spawnSync({
      cmd: ["docker", "compose", "-f", "docker-compose.yml", "up", "-d", "redis"],
      stdout: "pipe",
      stderr: "pipe",
    });
  }

  await waitForRedis();
}

export async function stopRedis(): Promise<void> {
  if (process.env.REDIS_URL) {
    return;
  }

  Bun.spawnSync({
    cmd: ["docker", "compose", "-f", "docker-compose.yml", "down", "--remove-orphans"],
    stdout: "pipe",
    stderr: "pipe",
  });
}

export async function clearRedis(prefix: string): Promise<void> {
  const client = await createRedisClient({ url: redisUrl });
  if (client instanceof IoredisClient) {
    await clearPrefixWithScan(client, prefix);
  }
  await client.quit();
}

export async function waitForInvocation(
  dicky: Dicky,
  id: string,
  expectedStatus: InvocationStatus,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const inv = await dicky.getInvocation(id);
    if (inv?.status === expectedStatus) {
      return;
    }
    await delay(50);
  }
  throw new Error(`Timeout waiting for invocation ${id} to be ${expectedStatus}`);
}

export async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export function buildIntegrationConfig(overrides?: Partial<DickyConfig>): DickyConfig {
  const config: DickyConfig = {
    redis: {
      ...testConfig.redis,
      ...(overrides?.redis ?? {}),
    },
    worker: {
      concurrency: 2,
      pollIntervalMs: 50,
      lockTtlMs: 1_000,
      lockRenewMs: 200,
      ackTimeoutMs: 1_000,
      timerPollIntervalMs: 50,
      ...(overrides?.worker ?? {}),
    },
    retry: {
      maxRetries: 2,
      initialDelayMs: 50,
      maxDelayMs: 200,
      backoffMultiplier: 2,
      ...(overrides?.retry ?? {}),
    },
  };

  if (overrides?.log) {
    config.log = overrides.log;
  }

  return config;
}

async function waitForRedis(): Promise<void> {
  const timeoutAt = Date.now() + 10_000;
  while (Date.now() < timeoutAt) {
    try {
      const client = await createRedisClient({ url: redisUrl });
      await client.ping();
      await client.quit();
      return;
    } catch {
      await delay(250);
    }
  }

  throw new Error("Redis did not start in time");
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
