import { describe, expect, it } from "bun:test";
import { StateStoreImpl } from "../../stores/state";
import { MockRedisClient } from "../../stores/redis";

const prefix = "test:state:";

describe("StateStore", () => {
  it("returns null for missing state", async () => {
    const redis = new MockRedisClient();
    const store = new StateStoreImpl(redis, prefix);

    expect(await store.get("cart", "123")).toBeNull();
  });

  it("persists state values", async () => {
    const redis = new MockRedisClient();
    const store = new StateStoreImpl(redis, prefix);

    const value = JSON.stringify({ count: 1 });
    await store.set("cart", "123", value);

    const stored = await store.get("cart", "123");
    expect(stored).toBe(value);
    expect(JSON.parse(stored ?? "{}").count).toBe(1);
  });
});
