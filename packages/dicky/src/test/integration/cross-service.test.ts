import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { z } from "zod";
import { Dicky } from "../../dicky";
import { service } from "../../define";
import {
  buildIntegrationConfig,
  clearRedis,
  delay,
  integrationEnabled,
  startRedis,
  stopRedis,
  testConfig,
} from "./setup";

(integrationEnabled ? describe : describe.skip)("Integration: Cross-Service Calls", () => {
  const prefix = "test:cross:";

  beforeAll(async () => {
    await startRedis();
  });

  afterAll(async () => {
    await stopRedis();
  });

  it("ctx.send dispatches to another service", async () => {
    const notifications: string[] = [];

    const dicky = new Dicky(
      buildIntegrationConfig({ redis: { ...testConfig.redis, keyPrefix: prefix } }),
    );

    dicky.use(
      service("orders", {
        create: {
          input: z.object({ orderId: z.string() }),
          output: z.object({ orderId: z.string() }),
          handler: async (ctx, { orderId }: { orderId: string }) => {
            await ctx.send("notifications", "send", {
              userId: "user-1",
              message: `Order ${orderId} created`,
            });
            return { orderId };
          },
        },
      }),
    );

    dicky.use(
      service("notifications", {
        send: {
          input: z.object({ userId: z.string(), message: z.string() }),
          output: z.object({ sent: z.boolean() }),
          handler: async (_ctx, { message }: { userId: string; message: string }) => {
            notifications.push(message);
            return { sent: true };
          },
        },
      }),
    );

    await dicky.start();
    await dicky.send("orders", "create", { orderId: "order-123" });

    await delay(500);
    expect(notifications).toContain("Order order-123 created");

    await dicky.stop();
    await clearRedis(prefix);
  });

  it("ctx.invoke waits for another service", async () => {
    const dicky = new Dicky(
      buildIntegrationConfig({ redis: { ...testConfig.redis, keyPrefix: prefix } }),
    );

    dicky.use(
      service("orders", {
        create: {
          input: z.object({ orderId: z.string() }),
          output: z.object({
            orderId: z.string(),
            inventory: z.object({ orderId: z.string(), reserved: z.boolean() }),
          }),
          handler: async (ctx, { orderId }: { orderId: string }) => {
            const inventory = await ctx.invoke("inventory", "reserve", { orderId });
            return { orderId, inventory };
          },
        },
      }),
    );

    dicky.use(
      service("inventory", {
        reserve: {
          input: z.object({ orderId: z.string() }),
          output: z.object({ orderId: z.string(), reserved: z.boolean() }),
          handler: async (ctx, { orderId }: { orderId: string }) => {
            await ctx.sleep("processing", "100ms");
            return { orderId, reserved: true };
          },
        },
      }),
    );

    await dicky.start();
    const result = await dicky.invoke("orders", "create", { orderId: "order-456" });

    expect(result).toEqual({
      orderId: "order-456",
      inventory: { orderId: "order-456", reserved: true },
    });

    await dicky.stop();
    await clearRedis(prefix);
  });
});
