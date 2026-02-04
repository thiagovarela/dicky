import { keys, newInvocationId, parseDuration } from "../utils";
import type { RedisClient } from "./redis";
import type { TimerEntry } from "./timer";

export interface StreamMessage {
  messageId: string;
  invocationId: string;
  handler: string;
  key?: string;
  args: string;
  attempt: string;
}

export interface StreamProducer {
  enqueue(
    serviceName: string,
    invocationId: string,
    handler: string,
    args: string,
    opts?: { key?: string; attempt?: number },
  ): Promise<string>;
  reenqueue(invocationId: string): Promise<void>;
  reenqueueRetry(invocationId: string): Promise<void>;
  enqueueDelayed(invocationId: string): Promise<void>;
  dispatch(
    service: string,
    handler: string,
    args: unknown,
    opts?: {
      key?: string;
      delay?: string;
      parentId?: string;
      parentStep?: number;
      maxRetries?: number;
      idempotencyKey?: string;
    },
  ): Promise<string>;
}

export interface StreamConsumer {
  read(
    services: string[],
    group: string,
    consumer: string,
    count: number,
    blockMs: number,
  ): Promise<Map<string, StreamMessage[]>>;
  ack(stream: string, group: string, ...messageIds: string[]): Promise<number>;
}

export class StreamProducerImpl implements StreamProducer {
  constructor(
    private redis: RedisClient,
    private prefix: string,
  ) {}

  async enqueue(
    serviceName: string,
    invocationId: string,
    handler: string,
    args: string,
    opts?: { key?: string; attempt?: number },
  ): Promise<string> {
    const streamKey = keys(this.prefix).stream(serviceName);
    const fields: Record<string, string> = {
      invocationId,
      handler,
      args,
      attempt: String(opts?.attempt ?? 0),
    };
    if (opts?.key) {
      fields.key = opts.key;
    }

    return this.redis.xadd(streamKey, "*", ...flattenFields(fields));
  }

  async reenqueue(invocationId: string): Promise<void> {
    const invKey = keys(this.prefix).invocation(invocationId);
    const inv = await this.redis.hgetall(invKey);
    if (!inv.service || !inv.handler || !inv.args) {
      return;
    }
    const attempt = Number.parseInt(inv.attempt ?? "0", 10);
    const opts = inv.key ? { key: inv.key, attempt } : { attempt };

    await this.enqueue(inv.service, invocationId, inv.handler, inv.args, opts);
  }

  async reenqueueRetry(invocationId: string): Promise<void> {
    const invKey = keys(this.prefix).invocation(invocationId);
    const inv = await this.redis.hgetall(invKey);
    if (!inv.service || !inv.handler || !inv.args) {
      return;
    }

    const attempt = Number.parseInt(inv.attempt ?? "0", 10) + 1;
    await this.redis.hset(invKey, "attempt", String(attempt));

    const opts = inv.key ? { key: inv.key, attempt } : { attempt };
    await this.enqueue(inv.service, invocationId, inv.handler, inv.args, opts);
  }

  async enqueueDelayed(invocationId: string): Promise<void> {
    await this.reenqueue(invocationId);
  }

  async dispatch(
    service: string,
    handler: string,
    args: unknown,
    opts?: {
      key?: string;
      delay?: string;
      parentId?: string;
      parentStep?: number;
      maxRetries?: number;
      idempotencyKey?: string;
    },
  ): Promise<string> {
    if (opts?.idempotencyKey) {
      const existing = await this.redis.get(keys(this.prefix).idempotency(opts.idempotencyKey));
      if (existing) {
        return existing;
      }
    }

    const invocationId = newInvocationId();
    const serializedArgs = JSON.stringify(args ?? null);
    const invKey = keys(this.prefix).invocation(invocationId);

    await this.redis.hmset(
      invKey,
      ...flattenFields({
        id: invocationId,
        service,
        handler,
        args: serializedArgs,
        status: "pending",
        attempt: "0",
        maxRetries: String(opts?.maxRetries ?? 3),
        createdAt: String(Date.now()),
        updatedAt: String(Date.now()),
        ...(opts?.key ? { key: opts.key } : {}),
        ...(opts?.parentId ? { parentId: opts.parentId } : {}),
        ...(opts?.parentStep != null ? { parentStep: String(opts.parentStep) } : {}),
      }),
    );

    if (opts?.idempotencyKey) {
      await this.redis.set(keys(this.prefix).idempotency(opts.idempotencyKey), invocationId);
    }

    if (opts?.delay) {
      const delayMs = parseDuration(opts.delay);
      const timerKey = keys(this.prefix).timers();
      const entry: TimerEntry = { invocationId, type: "delayed" };
      await this.redis.zadd(timerKey, String(Date.now() + delayMs), JSON.stringify(entry));
      return invocationId;
    }

    const enqueueOpts = opts?.key ? { key: opts.key } : undefined;
    await this.enqueue(service, invocationId, handler, serializedArgs, enqueueOpts);
    return invocationId;
  }
}

export class StreamConsumerImpl implements StreamConsumer {
  constructor(
    private redis: RedisClient,
    private prefix: string,
  ) {}

  async read(
    services: string[],
    group: string,
    consumer: string,
    count: number,
    blockMs: number,
  ): Promise<Map<string, StreamMessage[]>> {
    if (services.length === 0) {
      return new Map();
    }

    const streamKeys = services.map((service) => keys(this.prefix).stream(service));
    const streamIds = services.map(() => ">" as const);

    const result = await this.redis.xreadgroup(
      group,
      consumer,
      String(count),
      String(blockMs),
      ...streamKeys,
      ...streamIds,
    );

    const messages = new Map<string, StreamMessage[]>();
    if (!result) {
      return messages;
    }

    for (const [streamKey, entries] of result) {
      const service = streamKey.replace(`${this.prefix}stream:`, "");
      const parsed = entries.map(([messageId, fields]) => parseStreamMessage(messageId, fields));
      messages.set(service, parsed);
    }

    return messages;
  }

  async ack(stream: string, group: string, ...messageIds: string[]): Promise<number> {
    const streamKey = keys(this.prefix).stream(stream);
    return this.redis.xack(streamKey, group, ...messageIds);
  }
}

function flattenFields(fields: Record<string, string>): string[] {
  return Object.entries(fields).flatMap(([key, value]) => [key, value]);
}

function parseStreamMessage(messageId: string, fields: string[]): StreamMessage {
  const parsed: Record<string, string> = {};
  for (let index = 0; index < fields.length; index += 2) {
    const key = fields[index];
    const value = fields[index + 1];
    if (key != null && value != null) {
      parsed[key] = value;
    }
  }

  return {
    messageId,
    invocationId: parsed.invocationId ?? "",
    handler: parsed.handler ?? "",
    args: parsed.args ?? "",
    attempt: parsed.attempt ?? "0",
    ...(parsed.key ? { key: parsed.key } : {}),
  };
}
