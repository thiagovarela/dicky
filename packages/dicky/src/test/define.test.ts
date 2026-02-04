import { describe, expect, it } from "bun:test";
import { object, service } from "../define";
import type { ArgsOf } from "../types";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

type Expect<T extends true> = T;

describe("service", () => {
  it("creates service definitions", () => {
    const def = service("users", {
      getProfile: async (_ctx, { id }: { id: string }) => ({ id }),
    });

    expect(def.__kind).toBe("service");
    expect(def.name).toBe("users");
    expect(def.handlers.getProfile).toBeDefined();

    type Args = ArgsOf<typeof def.handlers.getProfile>;
    type _ArgsCheck = Expect<Equal<Args, { id: string }>>;
    const typeAssertions: _ArgsCheck[] = [true];
    void typeAssertions;
  });
});

describe("object", () => {
  it("creates object definitions", () => {
    const counter = object("counter", {
      initial: { count: 0 },
      handlers: {
        increment: async (ctx) => {
          const current = (ctx.state as { count: number }).count;
          await ctx.setState({ count: current + 1 });
        },
      },
    });

    expect(counter.__kind).toBe("object");
    expect(counter.name).toBe("counter");
    expect(counter.initial).toEqual({ count: 0 });
  });
});
