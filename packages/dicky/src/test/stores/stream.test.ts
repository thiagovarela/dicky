import { describe, expect, it } from "bun:test";
import { MockRedisClient } from "../../stores/redis";
import { StreamConsumerImpl, StreamProducerImpl } from "../../stores/stream";

const prefix = "test:stream:";

describe("StreamProducer", () => {
  it("enqueues messages", async () => {
    const redis = new MockRedisClient();
    const producer = new StreamProducerImpl(redis, prefix, 3, 86_400_000);
    const consumer = new StreamConsumerImpl(redis, prefix);

    await redis.xgroup(`${prefix}stream:orders`, "CREATE", "group", "0", "MKSTREAM");
    await producer.enqueue("orders", "inv_1", "handler", "{}", { key: "k1" });

    const messages = await consumer.read(["orders"], "group", "worker", 1, 0);
    const entries = messages.get("orders") ?? [];
    expect(entries).toHaveLength(1);
    expect(entries[0]?.invocationId).toBe("inv_1");
    expect(entries[0]?.key).toBe("k1");
  });

  it("dispatches and stores invocation record", async () => {
    const redis = new MockRedisClient();
    const producer = new StreamProducerImpl(redis, prefix, 3, 86_400_000);

    const invocationId = await producer.dispatch("orders", "process", { id: 1 });
    const invocation = await redis.hgetall(`${prefix}invocation:${invocationId}`);

    expect(invocation.service).toBe("orders");
    expect(invocation.handler).toBe("process");
  });

  it("consumes from multiple streams", async () => {
    const redis = new MockRedisClient();
    const producer = new StreamProducerImpl(redis, prefix, 3, 86_400_000);
    const consumer = new StreamConsumerImpl(redis, prefix);

    await redis.xgroup(`${prefix}stream:a`, "CREATE", "group", "0", "MKSTREAM");
    await redis.xgroup(`${prefix}stream:b`, "CREATE", "group", "0", "MKSTREAM");

    await producer.enqueue("a", "inv_a", "handler", "{}");
    await producer.enqueue("b", "inv_b", "handler", "{}");

    const messages = await consumer.read(["a", "b"], "group", "worker", 2, 0);
    expect(messages.get("a")?.[0]?.invocationId).toBe("inv_a");
    expect(messages.get("b")?.[0]?.invocationId).toBe("inv_b");
  });
});
