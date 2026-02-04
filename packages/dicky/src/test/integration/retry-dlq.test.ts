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

const integrationEnabled = process.env.DICKY_INTEGRATION === "1";

(integrationEnabled ? describe : describe.skip)("Integration: Retry and DLQ", () => {
  const prefix = "test:retry:";

  beforeAll(async () => {
    await startRedis();
  });

  afterAll(async () => {
    await stopRedis();
  });

  it("retries failed invocations", async () => {
    let attempts = 0;
    const dicky = new Dicky(
      buildIntegrationConfig({
        redis: { ...testConfig.redis, keyPrefix: prefix },
        retry: { maxRetries: 2, initialDelayMs: 50, maxDelayMs: 200, backoffMultiplier: 2 },
      }),
    );

    dicky.use(
      service("unreliable", {
        flaky: async () => {
          attempts += 1;
          if (attempts < 3) {
            throw new Error("Temporary failure");
          }
          return "success";
        },
      }),
    );

    await dicky.start();
    const id = await dicky.send("unreliable", "flaky", {});

    await waitForInvocation(dicky, id, "completed", 10_000);
    expect(attempts).toBe(3);

    await dicky.stop();
    await clearRedis(prefix);
  });

  it("moves to DLQ after max retries", async () => {
    const dicky = new Dicky(
      buildIntegrationConfig({
        redis: { ...testConfig.redis, keyPrefix: prefix },
        retry: { maxRetries: 1, initialDelayMs: 50, maxDelayMs: 200, backoffMultiplier: 2 },
      }),
    );

    dicky.use(
      service("failing", {
        alwaysFail: async () => {
          throw new Error("Permanent failure");
        },
      }),
    );

    await dicky.start();
    const id = await dicky.send("failing", "alwaysFail", {});

    await waitForInvocation(dicky, id, "failed", 10_000);

    const dlq = await dicky.listDLQ("failing");
    expect(dlq.find((entry) => entry.invocationId === id)).toBeDefined();

    await dicky.stop();
    await clearRedis(prefix);
  });

  it("retries entries from DLQ", async () => {
    let attempts = 0;
    const dicky = new Dicky(
      buildIntegrationConfig({
        redis: { ...testConfig.redis, keyPrefix: prefix },
        retry: { maxRetries: 1, initialDelayMs: 50, maxDelayMs: 200, backoffMultiplier: 2 },
      }),
    );

    dicky.use(
      service("dlq-test", {
        succeedOnRetry: async () => {
          attempts += 1;
          if (attempts === 1) {
            throw new Error("First attempt fails");
          }
          return "success";
        },
      }),
    );

    await dicky.start();
    const id = await dicky.send("dlq-test", "succeedOnRetry", {});

    await waitForInvocation(dicky, id, "failed", 10_000);
    await dicky.retryDLQ(id);

    await waitForInvocation(dicky, id, "completed", 10_000);
    const inv = await dicky.getInvocation(id);
    expect(inv?.status).toBe("completed");

    await dicky.stop();
    await clearRedis(prefix);
  });
});
