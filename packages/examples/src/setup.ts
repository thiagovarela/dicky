import { randomUUID } from "node:crypto";
import type { Dicky, DickyConfig, Registry } from "@dicky/dicky";

export function createConfig(prefix: string): DickyConfig {
  const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
  const uniquePrefix = `${prefix}-${randomUUID()}`;

  return {
    redis: {
      host: "localhost",
      port: 6379,
      url: redisUrl,
      keyPrefix: `${uniquePrefix}:`,
    },
    worker: {
      concurrency: 2,
      pollIntervalMs: 50,
      lockTtlMs: 1_000,
      lockRenewMs: 200,
      ackTimeoutMs: 1_000,
      timerPollIntervalMs: 50,
    },
    retry: {
      maxRetries: 2,
      initialDelayMs: 50,
      maxDelayMs: 200,
      backoffMultiplier: 2,
    },
  };
}

export async function withCleanup<R extends Registry>(
  dicky: Dicky<R>,
  fn: () => Promise<void>,
): Promise<void> {
  await dicky.start();
  try {
    await fn();
  } finally {
    await dicky.stop();
  }
}

export async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
