/**
 * Example: Side Effects
 *
 * Demonstrates ctx.run() for idempotent side effects across retries.
 *
 * Run: bun run src/02-side-effects.ts
 */
import { strict as assert } from "node:assert";
import { Dicky, service } from "@dicky/dicky";
import { createConfig, withCleanup } from "./setup";

const config = createConfig("ex-02");
const dicky = new Dicky(config);

let sendCount = 0;
let attempts = 0;
let shouldFail = true;

type SendWelcomeResult = { messageId: string; attempts: number; sendCount: number };

dicky.use(
  service("mailer", {
    sendWelcome: async (ctx, args: unknown) => {
      const { userId } = args as { userId: string };
      attempts += 1;
      const messageId = await ctx.run("send-email", async () => {
        sendCount += 1;
        return `email-${userId}`;
      });

      if (shouldFail) {
        shouldFail = false;
        throw new Error("Temporary email outage");
      }

      return { messageId, attempts, sendCount } satisfies SendWelcomeResult;
    },
  }),
);

export { dicky, config };

export async function run() {
  sendCount = 0;
  attempts = 0;
  shouldFail = true;

  const result = (await dicky.invoke("mailer", "sendWelcome", {
    userId: "user-42",
  })) as SendWelcomeResult;
  assert.equal(result.messageId, "email-user-42");
  assert.equal(result.sendCount, 1);
  assert.equal(sendCount, 1);
  assert.equal(result.attempts, 2);
}

if (import.meta.main) {
  await withCleanup(dicky, async () => {
    await run();
    console.log("✅ 02-side-effects passed");
  });
}
