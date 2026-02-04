/**
 * Example: Virtual Objects
 *
 * Demonstrates stateful, keyed services with durable state.
 *
 * Run: bun run src/04-virtual-objects.ts
 */
import { strict as assert } from "node:assert";
import { Dicky, object } from "@dicky/dicky";
import { z } from "zod";
import { createConfig, withCleanup } from "./setup";

const config = createConfig("ex-04");

type CartState = { items: string[] };

const addItemSchema = z.object({ item: z.string() });

type AddItemArgs = z.infer<typeof addItemSchema>;

const dicky = new Dicky(config).use(
  object("cart", {
    initial: { items: [] as string[] },
    handlers: {
      addItem: {
        input: addItemSchema,
        output: z.object({ items: z.array(z.string()) }),
        handler: async (ctx, { item }: AddItemArgs) => {
          const current = ctx.state as CartState;
          const next = { items: [...current.items, item] };
          await ctx.setState(next);
          return next;
        },
      },
      getItems: async (ctx, _args: {}) => ctx.state as CartState,
    },
  }),
);

export { dicky, config };

export async function run() {
  const key = "cart-123";

  await dicky.invoke("cart", "addItem", { item: "apple" }, { key });
  await dicky.invoke("cart", "addItem", { item: "banana" }, { key });

  const result = await dicky.invoke("cart", "getItems", {}, { key });
  assert.deepEqual(result.items, ["apple", "banana"]);
}

if (import.meta.main) {
  await withCleanup(dicky, async () => {
    await run();
    console.log("✅ 04-virtual-objects passed");
  });
}
