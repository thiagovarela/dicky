/**
 * Example: Saga Pattern
 *
 * Demonstrates compensating transactions with ctx.run().
 *
 * Run: bun run src/08-saga-pattern.ts
 */
import { strict as assert } from "node:assert";
import { Dicky, service } from "@dicky/dicky";
import { z } from "zod";
import { createConfig, withCleanup } from "./setup";

const config = createConfig("ex-08");

const sagaLog = {
  actions: [] as string[],
  compensations: [] as string[],
};

let shouldFail = true;

type SagaResult =
  | { status: "confirmed"; flightId: string; hotelId: string }
  | { status: "compensated"; reason: string };

const bookTripSchema = z.object({ tripId: z.string() });

type BookTripArgs = z.infer<typeof bookTripSchema>;

const dicky = new Dicky(config).use(
  service("travel", {
    bookTrip: {
      input: bookTripSchema,
      output: z.union([
        z.object({ status: z.literal("confirmed"), flightId: z.string(), hotelId: z.string() }),
        z.object({ status: z.literal("compensated"), reason: z.string() }),
      ]),
      handler: async (ctx, { tripId }: BookTripArgs) => {
        try {
          const flightId = await ctx.run("reserve-flight", () => {
            sagaLog.actions.push("flight");
            return `flight-${tripId}`;
          });
          const hotelId = await ctx.run("reserve-hotel", () => {
            sagaLog.actions.push("hotel");
            return `hotel-${tripId}`;
          });

          if (shouldFail) {
            shouldFail = false;
            throw new Error("payment-declined");
          }

          return { status: "confirmed", flightId, hotelId } satisfies SagaResult;
        } catch (error) {
          await ctx.run("cancel-hotel", () => {
            sagaLog.compensations.push("hotel");
          });
          await ctx.run("cancel-flight", () => {
            sagaLog.compensations.push("flight");
          });

          return {
            status: "compensated",
            reason: error instanceof Error ? error.message : "unknown",
          } satisfies SagaResult;
        }
      },
    },
  }),
);

export { dicky, config };

export async function run() {
  sagaLog.actions = [];
  sagaLog.compensations = [];
  shouldFail = true;

  const result = await dicky.invoke("travel", "bookTrip", { tripId: "trip-9" });
  assert.equal(result.status, "compensated");
  assert.deepEqual(sagaLog.actions, ["flight", "hotel"]);
  assert.deepEqual(sagaLog.compensations, ["hotel", "flight"]);
}

if (import.meta.main) {
  await withCleanup(dicky, async () => {
    await run();
    console.log("✅ 08-saga-pattern passed");
  });
}
