import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import { Dicky } from "../../dicky";
import { service } from "../../define";
import {
  buildIntegrationConfig,
  clearRedis,
  delay,
  startRedis,
  stopRedis,
  testConfig,
  waitForInvocation,
} from "./setup";

const integrationEnabled = process.env.DICKY_INTEGRATION === "1";

(integrationEnabled ? describe : describe.skip)("Integration: Dispatch Flow", () => {
  let dicky: Dicky;
  const prefix = "test:dispatch:";

  beforeAll(async () => {
    await startRedis();
  });

  afterAll(async () => {
    await stopRedis();
  });

  afterEach(async () => {
    if (dicky) {
      await dicky.stop();
    }
    await clearRedis(prefix);
  });

  it("executes handler and completes", async () => {
    dicky = new Dicky(
      buildIntegrationConfig({ redis: { ...testConfig.redis, keyPrefix: prefix } }),
    );
    dicky.use(
      service("test", {
        echo: async (ctx, { msg }: { msg: string }) => {
          await ctx.run("log", () => msg);
          return { echoed: msg };
        },
      }),
    );

    await dicky.start();
    const id = await dicky.send("test", "echo", { msg: "hello" });

    await waitForInvocation(dicky, id, "completed", 5_000);
    const inv = await dicky.getInvocation(id);
    expect(inv?.status).toBe("completed");
    expect(inv?.result).toEqual({ echoed: "hello" });
  });

  it("handles concurrent dispatches", async () => {
    dicky = new Dicky(
      buildIntegrationConfig({ redis: { ...testConfig.redis, keyPrefix: prefix } }),
    );
    dicky.use(
      service("test", {
        process: async (_ctx, { id }: { id: number }) => ({ id }),
      }),
    );

    await dicky.start();

    const ids = await Promise.all(
      Array.from({ length: 5 }, (_, i) => dicky.send("test", "process", { id: i })),
    );

    for (const id of ids) {
      await waitForInvocation(dicky, id, "completed", 5_000);
      const inv = await dicky.getInvocation(id);
      expect(inv?.status).toBe("completed");
    }
  });

  it("respects delayed dispatch", async () => {
    dicky = new Dicky(
      buildIntegrationConfig({ redis: { ...testConfig.redis, keyPrefix: prefix } }),
    );
    dicky.use(
      service("test", {
        delayed: async () => ({ executed: Date.now() }),
      }),
    );

    await dicky.start();
    const start = Date.now();
    const id = await dicky.send("test", "delayed", {}, { delay: "200ms" });

    await delay(50);
    const pending = await dicky.getInvocation(id);
    expect(pending?.status).toBe("pending");

    await waitForInvocation(dicky, id, "completed", 5_000);
    const completed = await dicky.getInvocation(id);
    expect(completed?.completedAt ?? 0).toBeGreaterThanOrEqual(start + 200);
  });
});
