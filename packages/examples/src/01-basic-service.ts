/**
 * Example: Basic Service
 *
 * Demonstrates registering a service and invoking a handler.
 *
 * Run: bun run src/01-basic-service.ts
 */
import { strict as assert } from "node:assert";
import { Dicky, service } from "@dicky/dicky";
import { createConfig, withCleanup } from "./setup";

const config = createConfig("ex-01");
const dicky = new Dicky(config);

dicky.use(
  service("hello", {
    greet: async (_ctx, args: unknown) => {
      const { name } = args as { name: string };
      return `Hello, ${name}!`;
    },
  }),
);

export { dicky, config };

export async function run() {
  const result = (await dicky.invoke("hello", "greet", { name: "Dicky" })) as string;
  assert.equal(result, "Hello, Dicky!");
}

if (import.meta.main) {
  await withCleanup(dicky, async () => {
    await run();
    console.log("✅ 01-basic-service passed");
  });
}
