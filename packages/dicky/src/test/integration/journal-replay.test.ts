import { afterAll, beforeAll, describe, expect, it } from "bun:test";
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

(integrationEnabled ? describe : describe.skip)("Integration: Journal Replay", () => {
  const prefix = "test:replay:";

  beforeAll(async () => {
    await startRedis();
  });

  afterAll(async () => {
    await stopRedis();
  });

  it("replays journaled operations after restart", async () => {
    let runCount = 0;

    const handler = async (ctx: {
      run: (name: string, fn: () => void) => Promise<void>;
      sleep: (n: string, d: string) => Promise<void>;
    }) => {
      await ctx.run("step-1", () => {
        runCount += 1;
      });
      await ctx.run("step-2", () => {
        runCount += 1;
      });
      await ctx.sleep("wait", "200ms");
      await ctx.run("step-3", () => {
        runCount += 1;
      });
      return runCount;
    };

    const dicky = new Dicky(
      buildIntegrationConfig({ redis: { ...testConfig.redis, keyPrefix: prefix } }),
    );
    dicky.use(service("counter", { increment: handler }));

    await dicky.start();
    const id = await dicky.send("counter", "increment", {});

    await waitForInvocation(dicky, id, "suspended", 5_000);
    expect(runCount).toBe(2);

    await dicky.stop();

    await delay(250);

    const dicky2 = new Dicky(
      buildIntegrationConfig({ redis: { ...testConfig.redis, keyPrefix: prefix } }),
    );
    dicky2.use(service("counter", { increment: handler }));

    await dicky2.start();
    await waitForInvocation(dicky2, id, "completed", 10_000);

    expect(runCount).toBe(3);

    await dicky2.stop();
    await clearRedis(prefix);
  });
});
