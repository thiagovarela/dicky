import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Dicky } from "../../dicky";
import { object } from "../../define";
import {
  buildIntegrationConfig,
  clearRedis,
  startRedis,
  stopRedis,
  testConfig,
  waitForInvocation,
} from "./setup";

const integrationEnabled = process.env.DICKY_INTEGRATION === "1";

(integrationEnabled ? describe : describe.skip)("Integration: Virtual Objects", () => {
  const prefix = "test:objects:";

  beforeAll(async () => {
    await startRedis();
  });

  afterAll(async () => {
    await stopRedis();
  });

  it("persists state across invocations", async () => {
    const dicky = new Dicky(
      buildIntegrationConfig({ redis: { ...testConfig.redis, keyPrefix: prefix } }),
    );

    dicky.use(
      object("counter", {
        initial: { count: 0 },
        handlers: {
          increment: async (ctx) => {
            const current = (ctx.state as { count: number }).count;
            const next = current + 1;
            await ctx.setState({ count: next });
            return next;
          },
          getCount: async (ctx) => (ctx.state as { count: number }).count,
        },
      }),
    );

    await dicky.start();

    const firstId = await dicky.send("counter", "increment", {}, { key: "user:1" });
    await waitForInvocation(dicky, firstId, "completed", 5_000);

    const count1 = await dicky.invoke("counter", "getCount", {}, { key: "user:1" });
    expect(count1).toBe(1);

    const secondId = await dicky.send("counter", "increment", {}, { key: "user:1" });
    await waitForInvocation(dicky, secondId, "completed", 5_000);

    const count2 = await dicky.invoke("counter", "getCount", {}, { key: "user:1" });
    expect(count2).toBe(2);

    await dicky.stop();
    await clearRedis(prefix);
  });
});
