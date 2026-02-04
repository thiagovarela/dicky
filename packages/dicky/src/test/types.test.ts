import { describe, it, expect } from "bun:test";
import type {
  AddToRegistry,
  ArgsOf,
  DQConfig,
  DurableContext,
  Handler,
  ObjectDef,
  ReturnOf,
  ServiceDef,
} from "../types.js";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

type Expect<T extends true> = T;

type ExampleHandler = (ctx: DurableContext, args: { userId: string }) => Promise<number>;

type _ArgsExtract = Expect<Equal<ArgsOf<ExampleHandler>, { userId: string }>>;

type _ReturnExtract = Expect<Equal<ReturnOf<ExampleHandler>, number>>;

type ExampleService = ServiceDef<"onboarding", { welcome: ExampleHandler }>;

type ExampleObject = ObjectDef<"counter", { count: number }, { increment: ExampleHandler }>;

type _ServiceKind = Expect<Equal<ExampleService["__kind"], "service">>;

type _ObjectKind = Expect<Equal<ExampleObject["__kind"], "object">>;

type _ServiceDiscriminated = Expect<
  Equal<Extract<ExampleService | ExampleObject, { __kind: "service" }>, ExampleService>
>;

type BaseRegistry = {
  base: ServiceDef<"base", { run: Handler }>;
};

type RegistryWithService = AddToRegistry<BaseRegistry, ExampleService>;

type _RegistryHasService = Expect<Equal<RegistryWithService["onboarding"], ExampleService>>;

type _RegistryKeepsBase = Expect<Equal<RegistryWithService["base"], BaseRegistry["base"]>>;

type TypeAssertions = [
  _ArgsExtract,
  _ReturnExtract,
  _ServiceKind,
  _ObjectKind,
  _ServiceDiscriminated,
  _RegistryHasService,
  _RegistryKeepsBase,
];

const typeAssertions: TypeAssertions = [true, true, true, true, true, true, true];

void typeAssertions;

describe("types", () => {
  it("accepts configuration shape", () => {
    const config: DQConfig = {
      redis: { host: "localhost", port: 6379 },
      worker: { concurrency: 5 },
    };

    expect(config.redis.host).toBe("localhost");
  });
});
