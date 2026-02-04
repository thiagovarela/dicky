import { describe, expect, it } from "bun:test";
import { DurableContextImpl } from "../context";
import { ReplayError, SuspendedError } from "../errors";
import type { JournalEntry } from "../types";
import { MockRedisClient } from "../stores/redis";
import { JournalStoreImpl } from "../stores/journal";
import { StateStoreImpl } from "../stores/state";
import { TimerStoreImpl } from "../stores/timer";
import { AwakeableStoreImpl } from "../stores/awakeable";
import { MockLuaScripts } from "./stores/helpers";
import type { StreamProducer } from "../stores/stream";

const prefix = "test:context:";

class TestStreamProducer implements StreamProducer {
  readonly dispatchCalls: Array<{
    service: string;
    handler: string;
    args: unknown;
    opts?: {
      key?: string;
      delay?: string;
      parentId?: string;
      parentStep?: number;
      maxRetries?: number;
      idempotencyKey?: string;
    };
  }> = [];

  async enqueue(): Promise<string> {
    return "";
  }

  async reenqueue(): Promise<void> {
    return undefined;
  }

  async reenqueueRetry(): Promise<void> {
    return undefined;
  }

  async enqueueDelayed(): Promise<void> {
    return undefined;
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
    this.dispatchCalls.push({
      service,
      handler,
      args,
      ...(opts ? { opts } : {}),
    });
    return "child";
  }
}

function createContext(invocationId = "inv_1") {
  const redis = new MockRedisClient();
  const journal = new JournalStoreImpl(redis, prefix);
  const stateStore = new StateStoreImpl(redis, prefix);
  const timerStore = new TimerStoreImpl(redis, prefix, new MockLuaScripts());
  const streamProducer = new TestStreamProducer();
  const awakeableStore = new AwakeableStoreImpl(redis, prefix, journal, streamProducer);

  const ctx = new DurableContextImpl(
    { id: invocationId, key: "key", service: "objects" },
    journal,
    timerStore,
    stateStore,
    awakeableStore,
    streamProducer,
    { initialState: { count: 0 } },
    { count: 0 },
  );

  return { ctx, redis, journal, timerStore, streamProducer, stateStore };
}

describe("DurableContext", () => {
  it("runs side effects and journals", async () => {
    const { ctx, journal } = createContext();
    const result = await ctx.run("test", () => "hello");

    expect(result).toBe("hello");
    const entry = await journal.get(ctx.invocationId, 0);
    expect(entry?.status).toBe("completed");
  });

  it("replays journaled results", async () => {
    const { ctx, journal } = createContext("inv_2");
    await journal.write({
      invocationId: "inv_2",
      sequence: 0,
      type: "run",
      name: "test",
      status: "completed",
      result: JSON.stringify("cached"),
      createdAt: Date.now(),
      completedAt: Date.now(),
    });

    let executed = false;
    const result = await ctx.run("test", () => {
      executed = true;
      return "fresh";
    });

    expect(result).toBe("cached");
    expect(executed).toBe(false);
  });

  it("throws ReplayError on failed replay", async () => {
    const { ctx, journal } = createContext("inv_3");
    await journal.write({
      invocationId: "inv_3",
      sequence: 0,
      type: "run",
      name: "test",
      status: "failed",
      error: "boom",
      createdAt: Date.now(),
      completedAt: Date.now(),
    });

    await expect(ctx.run("test", () => "x")).rejects.toBeInstanceOf(ReplayError);
  });

  it("schedules sleeps and suspends", async () => {
    const { ctx, redis } = createContext("inv_sleep");
    await expect(ctx.sleep("wait", "1s")).rejects.toBeInstanceOf(SuspendedError);

    const timerKey = `${prefix}timers`;
    const entries = await redis.zrangebyscore(timerKey, "-inf", "+inf");
    expect(entries).toHaveLength(1);
  });

  it("dispatches invokes and suspends", async () => {
    const { ctx, journal, streamProducer } = createContext("inv_invoke");
    await expect(ctx.invoke("svc", "handler", { ok: true })).rejects.toBeInstanceOf(SuspendedError);

    expect(streamProducer.dispatchCalls).toHaveLength(1);
    const entry = await journal.get("inv_invoke", 0);
    expect(entry?.status).toBe("pending");
  });

  it("dispatches send once", async () => {
    const { ctx, journal, streamProducer } = createContext("inv_send");
    await ctx.send("svc", "handler", { ok: true });

    expect(streamProducer.dispatchCalls).toHaveLength(1);
    const entry = await journal.get("inv_send", 0);
    expect(entry?.status).toBe("completed");
  });

  it("creates awakeables and suspends", async () => {
    const { ctx, journal, redis } = createContext("inv_awake");
    await expect(ctx.awakeable("webhook")).rejects.toBeInstanceOf(SuspendedError);

    const entry = await journal.get("inv_awake", 0);
    expect(entry?.status).toBe("pending");

    const awakeable = await redis.hgetall(`${prefix}awakeable:${entry?.name}`);
    expect(awakeable.status).toBe("pending");
  });

  it("returns awakeable result on replay", async () => {
    const { ctx, journal } = createContext("inv_awake_replay");
    await journal.write({
      invocationId: "inv_awake_replay",
      sequence: 0,
      type: "awakeable",
      name: "awake_1",
      status: "completed",
      result: JSON.stringify({ status: "paid" }),
      createdAt: Date.now(),
      completedAt: Date.now(),
    });

    const [awakeableId, promise] = await ctx.awakeable("webhook");
    expect(awakeableId).toBe("awake_1");
    await expect(promise).resolves.toEqual({ status: "paid" });
  });

  it("persists state", async () => {
    const { ctx, stateStore } = createContext("inv_state");
    await ctx.setState({ count: 42 });

    const stored = await stateStore.get("objects", "key");
    expect(JSON.parse(stored ?? "{}").count).toBe(42);
  });

  it("replays stored state", async () => {
    const { ctx, journal } = createContext("inv_state_replay");
    const entry: JournalEntry = {
      invocationId: "inv_state_replay",
      sequence: 0,
      type: "state-set",
      name: "setState",
      status: "completed",
      result: JSON.stringify({ count: 7 }),
      createdAt: Date.now(),
      completedAt: Date.now(),
    };

    await journal.write(entry);
    await ctx.setState({ count: 0 });
    expect(ctx.state).toEqual({ count: 7 });
  });
});
