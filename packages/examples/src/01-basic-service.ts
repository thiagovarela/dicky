/**
 * Example: Basic Service
 *
 * Demonstrates registering a service and invoking a handler.
 *
 * Run: bun run src/01-basic-service.ts
 */
import { strict as assert } from "node:assert";
import { Dicky, service } from "@dicky/dicky";
import { z } from "zod";
import { createConfig, withCleanup } from "./setup";

const config = createConfig("ex-01");

const greetSchema = z.object({ name: z.string() });

type GreetArgs = z.infer<typeof greetSchema>;

const dicky = new Dicky(config).use(
  service("hello", {
    greet: {
      input: greetSchema,
      output: z.string(),
      handler: async (_ctx, { name }: GreetArgs) => `Hello, ${name}!`,
    },
  }),
);

export { dicky, config };

export async function run() {
  const result = await dicky.invoke("hello", "greet", { name: "Dicky" });
  assert.equal(result, "Hello, Dicky!");
}

if (import.meta.main) {
  await withCleanup(dicky, async () => {
    await run();
    console.log("✅ 01-basic-service passed");
  });
}
