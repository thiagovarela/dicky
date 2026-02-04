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

const DEFAULT_RETENTION = {
  completionTtlMs: 60 * 60 * 1000,
  invocationTtlMs: 24 * 60 * 60 * 1000,
  journalTtlMs: 24 * 60 * 60 * 1000,
  idempotencyTtlMs: 24 * 60 * 60 * 1000,
};

const DEFAULT_SHUTDOWN = {
  handleSignals: true,
  signals: ["SIGINT", "SIGTERM"] as Array<NodeJS.Signals>,
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
    retention: {
      completionTtlMs: config.retention?.completionTtlMs ?? DEFAULT_RETENTION.completionTtlMs,
      invocationTtlMs: config.retention?.invocationTtlMs ?? DEFAULT_RETENTION.invocationTtlMs,
      journalTtlMs: config.retention?.journalTtlMs ?? DEFAULT_RETENTION.journalTtlMs,
      idempotencyTtlMs:
        config.retention?.idempotencyTtlMs ?? DEFAULT_RETENTION.idempotencyTtlMs,
    },
    shutdown: {
      handleSignals: config.shutdown?.handleSignals ?? DEFAULT_SHUTDOWN.handleSignals,
      signals: config.shutdown?.signals ?? DEFAULT_SHUTDOWN.signals,
    },
  };

  if (config.log) {
    resolved.log = config.log;
  }

  validateConfig(resolved);
  return resolved;
}

function validateConfig(config: ResolvedConfig): void {
  ensureNonNegative(config.worker.concurrency, "worker.concurrency");
  ensurePositive(config.worker.pollIntervalMs, "worker.pollIntervalMs");
  ensurePositive(config.worker.lockTtlMs, "worker.lockTtlMs");
  ensurePositive(config.worker.lockRenewMs, "worker.lockRenewMs");
  ensurePositive(config.worker.ackTimeoutMs, "worker.ackTimeoutMs");
  ensurePositive(config.worker.timerPollIntervalMs, "worker.timerPollIntervalMs");

  ensureNonNegative(config.retry.maxRetries, "retry.maxRetries");
  ensureNonNegative(config.retry.initialDelayMs, "retry.initialDelayMs");
  ensureNonNegative(config.retry.maxDelayMs, "retry.maxDelayMs");
  ensurePositive(config.retry.backoffMultiplier, "retry.backoffMultiplier");

  ensurePositive(config.retention.completionTtlMs, "retention.completionTtlMs");
  ensurePositive(config.retention.invocationTtlMs, "retention.invocationTtlMs");
  ensurePositive(config.retention.journalTtlMs, "retention.journalTtlMs");
  ensurePositive(config.retention.idempotencyTtlMs, "retention.idempotencyTtlMs");
}

function ensurePositive(value: number, field: string): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`Invalid ${field}: must be greater than 0`);
  }
}

function ensureNonNegative(value: number, field: string): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid ${field}: must be 0 or greater`);
  }
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
