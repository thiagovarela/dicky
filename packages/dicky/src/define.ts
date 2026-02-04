import type { DurableContext, Handler, ObjectDef, ServiceDef } from "./types";

/**
 * Define a stateless service. Returns a typed definition
 * to pass to `dicky.use()`.
 */
export function service<TName extends string, THandlers extends Record<string, Handler>>(
  name: TName,
  handlers: THandlers,
): ServiceDef<TName, THandlers> {
  return {
    __kind: "service" as const,
    name,
    handlers,
  };
}

/**
 * Define a virtual object (keyed, stateful, single-writer).
 * Returns a typed definition to pass to `dicky.use()`.
 */
export function object<
  TName extends string,
  TState,
  THandlers extends Record<string, (ctx: DurableContext, args: any) => Promise<any>>,
>(name: TName, def: { initial: TState; handlers: THandlers }): ObjectDef<TName, TState, THandlers> {
  return {
    __kind: "object" as const,
    name,
    initial: def.initial,
    handlers: def.handlers,
  };
}
