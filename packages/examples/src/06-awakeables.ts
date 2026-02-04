/**
 * Example: Awakeables
 *
 * Demonstrates waiting for external events with ctx.awakeable().
 *
 * Run: bun run src/06-awakeables.ts
 */
import { strict as assert } from "node:assert";
import { Dicky, service } from "@dicky/dicky";
import type { InvocationStatus, JournalEntry } from "@dicky/dicky";
import { z } from "zod";
import { createConfig, delay, withCleanup } from "./setup";

const config = createConfig("ex-06");

type ApprovalDecision = { status: string };

type ApprovalResult = { orderId: string; decision: ApprovalDecision };

const requestSchema = z.object({ orderId: z.string() });

type RequestArgs = z.infer<typeof requestSchema>;

const dicky = new Dicky(config).use(
  service("approval", {
    request: {
      input: requestSchema,
      output: z.object({
        orderId: z.string(),
        decision: z.object({ status: z.string() }),
      }),
      handler: async (ctx, { orderId }: RequestArgs) => {
        const [, promise] = await ctx.awakeable<ApprovalDecision>("approval");
        const decision = await promise;
        return { orderId, decision } satisfies ApprovalResult;
      },
    },
  }),
);

export { dicky, config };

export async function run() {
  const invocationId = await dicky.send("approval", "request", { orderId: "order-55" });

  await waitForInvocation(invocationId, "suspended", 5_000);

  const awakeableId = await findAwakeableId(invocationId);
  await dicky.resolveAwakeable(awakeableId, { status: "approved" });

  await waitForInvocation(invocationId, "completed", 5_000);

  const invocation = await dicky.getInvocation(invocationId);
  assert.deepEqual(invocation?.result as ApprovalResult, {
    orderId: "order-55",
    decision: { status: "approved" },
  });
}

if (import.meta.main) {
  await withCleanup(dicky, async () => {
    await run();
    console.log("✅ 06-awakeables passed");
  });
}

async function waitForInvocation(
  invocationId: string,
  status: InvocationStatus,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const invocation = await dicky.getInvocation(invocationId);
    if (invocation?.status === status) {
      return;
    }
    await delay(50);
  }
  throw new Error(`Timeout waiting for ${invocationId} to be ${status}`);
}

async function findAwakeableId(invocationId: string): Promise<string> {
  const journal = await dicky.getJournal(invocationId);
  const entry = journal.find((item: JournalEntry) => item.type === "awakeable");
  if (!entry) {
    throw new Error("Awakeable entry not found");
  }
  return entry.name;
}
