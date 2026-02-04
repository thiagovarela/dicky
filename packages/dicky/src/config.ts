import type { DickyConfig, RedisConfig, ResolvedConfig } from "./types";

const DEFAULT_WORKER = {
  concurrency: 10,
  pollIntervalMs: 100,
  lockTtlMs: 30_000,
  lockRenewMs: 10_000,
  ackTimeoutMs: 60_000,
  timerPollIntervalMs: 1_000,
};

const DEFAULT_RETRY = {
  maxRetries: 3,
  initialDelayMs: 1_000,
  maxDelayMs: 30_000,
  backoffMultiplier: 2,
};

export function resolveConfig(config: DickyConfig): ResolvedConfig {
  const redis = config.redis;
  const keyPrefix = redis.keyPrefix ?? "dicky:";

  const resolved: ResolvedConfig = {
    redis: {
      ...redis,
      keyPrefix,
      url: buildRedisUrl(redis),
    },
    worker: {
      concurrency: config.worker?.concurrency ?? DEFAULT_WORKER.concurrency,
      pollIntervalMs: config.worker?.pollIntervalMs ?? DEFAULT_WORKER.pollIntervalMs,
      lockTtlMs: config.worker?.lockTtlMs ?? DEFAULT_WORKER.lockTtlMs,
      lockRenewMs: config.worker?.lockRenewMs ?? DEFAULT_WORKER.lockRenewMs,
      ackTimeoutMs: config.worker?.ackTimeoutMs ?? DEFAULT_WORKER.ackTimeoutMs,
      timerPollIntervalMs: config.worker?.timerPollIntervalMs ?? DEFAULT_WORKER.timerPollIntervalMs,
    },
    retry: {
      maxRetries: config.retry?.maxRetries ?? DEFAULT_RETRY.maxRetries,
      initialDelayMs: config.retry?.initialDelayMs ?? DEFAULT_RETRY.initialDelayMs,
      maxDelayMs: config.retry?.maxDelayMs ?? DEFAULT_RETRY.maxDelayMs,
      backoffMultiplier: config.retry?.backoffMultiplier ?? DEFAULT_RETRY.backoffMultiplier,
    },
  };

  if (config.log) {
    resolved.log = config.log;
  }

  return resolved;
}

export function buildRedisUrl(config: RedisConfig): string {
  if (config.url) {
    return config.url;
  }
  const host = config.host ?? "localhost";
  const port = config.port ?? 6379;
  const db = config.db ?? 0;
  const protocol = config.tls ? "rediss" : "redis";
  const auth = config.password ? `:${encodeURIComponent(config.password)}@` : "";
  return `${protocol}://${auth}${host}:${port}/${db}`;
}
