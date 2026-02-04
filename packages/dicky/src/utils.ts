import { nanoid } from "nanoid";

/**
 * Build Redis key helpers using a shared prefix.
 */
export function keys(prefix: string) {
  return {
    stream: (service: string) => `${prefix}stream:${service}`,
    consumerGroup: (service: string) => `${prefix}cg:${service}`,
    consumerName: (workerId: string) => `${prefix}worker:${workerId}`,
    journal: (invocationId: string) => `${prefix}journal:${invocationId}`,
    invocation: (invocationId: string) => `${prefix}invocation:${invocationId}`,
    state: (objectName: string, key: string) => `${prefix}state:${objectName}:${key}`,
    lock: (objectName: string, key: string) => `${prefix}lock:${objectName}:${key}`,
    timers: () => `${prefix}timers`,
    awakeable: (awakeableId: string) => `${prefix}awakeable:${awakeableId}`,
    dlq: (service: string) => `${prefix}dlq:${service}`,
    completion: (invocationId: string) => `${prefix}completion:${invocationId}`,
    idempotency: (key: string) => `${prefix}idempotent:${key}`,
    metrics: (service: string) => `${prefix}metrics:${service}`,
  };
}

/**
 * Parse a duration string (e.g. 500ms, 5s, 30m, 2h, 1d) into milliseconds.
 */
export function parseDuration(duration: string): number {
  const regex = /^(\d+)(ms|s|m|h|d)$/;
  const match = duration.match(regex);
  if (!match) {
    throw new Error(`Invalid duration format: ${duration}`);
  }

  const value = Number.parseInt(match[1], 10);
  const unit = match[2] as "ms" | "s" | "m" | "h" | "d";

  const multipliers = {
    ms: 1,
    s: 1000,
    m: 60 * 1000,
    h: 60 * 60 * 1000,
    d: 24 * 60 * 60 * 1000,
  };

  return value * multipliers[unit];
}

/**
 * Generate a new invocation ID with the `inv_` prefix.
 */
export function newInvocationId(): string {
  return `inv_${nanoid()}`;
}

/**
 * Generate a new awakeable ID with the `awake_` prefix.
 */
export function newAwakeableId(): string {
  return `awake_${nanoid()}`;
}

/**
 * Generate a lock token for virtual object locks.
 */
export function newLockToken(): string {
  return nanoid();
}

/**
 * Generate a new worker ID with the `worker-` prefix.
 */
export function newWorkerId(): string {
  return `worker-${nanoid()}`;
}
