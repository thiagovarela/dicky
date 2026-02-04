import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Dicky } from "../../dicky";
import { service } from "../../define";
import {
  buildIntegrationConfig,
  clearRedis,
  startRedis,
  stopRedis,
  testConfig,
  waitForInvocation,
} from "./setup";
import { createRedisClient, IoredisClient } from "../../stores/redis";
import { buildRedisUrl } from "../../config";

const integrationEnabled = process.env.DICKY_INTEGRATION === "1";

(integrationEnabled ? describe : describe.skip)("Integration: Awakeables", () => {
  const prefix = "test:awakeable:";

  beforeAll(async () => {
    await startRedis();
  });

  afterAll(async () => {
    await stopRedis();
  });

  it("pauses until external resolution", async () => {
    const dicky = new Dicky(
      buildIntegrationConfig({
        redis: { ...testConfig.redis, keyPrefix: prefix },
        retry: { maxRetries: 1 },
      }),
    );

    dicky.use(
      service("payment", {
        checkout: async (ctx, { orderId }: { orderId: string }) => {
          await ctx.run("create-charge", () => ({ orderId, status: "pending" }));
          const [, promise] = await ctx.awakeable("webhook");
          const webhook = await promise;
          return webhook;
        },
      }),
    );

    await dicky.start();
    const id = await dicky.send("payment", "checkout", { orderId: "order-123" });

    await waitForInvocation(dicky, id, "suspended", 5_000);

    const awakeableId = await findAwakeableId(prefix);
    await dicky.resolveAwakeable(awakeableId, { status: "paid", amount: 99 });

    await waitForInvocation(dicky, id, "completed", 5_000);
    const inv = await dicky.getInvocation(id);
    expect(inv?.result).toEqual({ status: "paid", amount: 99 });

    await dicky.stop();
    await clearRedis(prefix);
  });

  it("handles awakeable rejection", async () => {
    const dicky = new Dicky(
      buildIntegrationConfig({
        redis: { ...testConfig.redis, keyPrefix: prefix },
        retry: { maxRetries: 1 },
      }),
    );

    dicky.use(
      service("payment", {
        checkout: async (ctx, { orderId: _orderId }: { orderId: string }) => {
          const [, promise] = await ctx.awakeable("webhook");
          return promise;
        },
      }),
    );

    await dicky.start();
    const id = await dicky.send("payment", "checkout", { orderId: "order-456" });

    await waitForInvocation(dicky, id, "suspended", 5_000);

    const awakeableId = await findAwakeableId(prefix);
    await dicky.rejectAwakeable(awakeableId, "Payment declined");

    await waitForInvocation(dicky, id, "failed", 5_000);
    const inv = await dicky.getInvocation(id);
    expect(inv?.status).toBe("failed");

    await dicky.stop();
    await clearRedis(prefix);
  });
});

async function findAwakeableId(prefix: string): Promise<string> {
  const redis = await createRedisClient({ url: buildRedisUrl(testConfig.redis) });
  if (!(redis instanceof IoredisClient)) {
    throw new Error("Expected ioredis client for integration tests");
  }

  let cursor = "0";
  do {
    const [next, keys] = await redis.raw.scan(
      cursor,
      "MATCH",
      `${prefix}awakeable:*`,
      "COUNT",
      "100",
    );
    cursor = next;
    if (keys.length > 0) {
      const [first] = keys;
      if (first) {
        await redis.quit();
        return first.replace(`${prefix}awakeable:`, "");
      }
    }
  } while (cursor !== "0");

  await redis.quit();
  throw new Error("Awakeable not found");
}
