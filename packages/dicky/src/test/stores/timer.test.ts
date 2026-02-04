import { describe, expect, it } from "bun:test";
import { MockRedisClient } from "../../stores/redis";
import { TimerStoreImpl } from "../../stores/timer";
import { MockLuaScripts } from "./helpers";

const prefix = "test:timer:";

describe("TimerStore", () => {
  it("polls expired entries", async () => {
    const redis = new MockRedisClient();
    const scripts = new MockLuaScripts();
    const store = new TimerStoreImpl(redis, prefix, scripts);

    await store.schedule({ invocationId: "inv_1", type: "sleep" }, Date.now() - 10);
    await store.schedule({ invocationId: "inv_2", type: "sleep" }, Date.now() + 1000);

    const expired = await store.pollExpired(Date.now(), 10);
    expect(expired).toHaveLength(1);
    expect(expired[0]?.invocationId).toBe("inv_1");

    const remaining = await store.pollExpired(Date.now(), 10);
    expect(remaining).toHaveLength(0);
  });
});
