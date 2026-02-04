export type InvocationId = string;

// Service/Object definitions
export interface ServiceDef<
  TName extends string = string,
  THandlers extends Record<string, Handler> = Record<string, Handler>,
> {
  readonly __kind: "service";
  readonly name: TName;
  readonly handlers: THandlers;
}

export interface ObjectDef<
  TName extends string = string,
  TState = unknown,
  THandlers extends Record<string, Handler> = Record<string, Handler>,
> {
  readonly __kind: "object";
  readonly name: TName;
  readonly initial: TState;
  readonly handlers: THandlers;
}

// Registry for type-safe composition
export type Registry = Record<string, ServiceDef | ObjectDef>;
export type AddToRegistry<R extends Registry, T extends ServiceDef | ObjectDef> = R &
  Record<T["name"], T>;

// Invocation lifecycle
export type InvocationStatus =
  | "pending"
  | "running"
  | "suspended"
  | "completed"
  | "failed"
  | "cancelled";

export interface Invocation {
  id: InvocationId;
  service: string;
  handler: string;
  key?: string;
  args: unknown;
  status: InvocationStatus;
  attempt: number;
  maxRetries: number;
  createdAt: number;
  updatedAt: number;
  completedAt?: number;
  result?: unknown;
  error?: string;
  parentId?: InvocationId;
  parentStep?: number;
}

// Journal entries
export type JournalEntryType =
  | "run"
  | "sleep"
  | "invoke"
  | "send"
  | "awakeable"
  | "state-get"
  | "state-set";

export type JournalEntryStatus = "pending" | "completed" | "failed";

export interface JournalEntry {
  invocationId: InvocationId;
  sequence: number;
  type: JournalEntryType;
  name: string;
  status: JournalEntryStatus;
  result?: string; // JSON-serialized
  error?: string;
  createdAt: number;
  completedAt?: number;
}

// Durable Context - developer-facing API
export interface DurableContext<TState = unknown> {
  readonly invocationId: InvocationId;
  readonly key: string | undefined;
  readonly state: TState;

  // Execute side effect with exactly-once semantics
  run<T>(name: string, fn: () => T | Promise<T>): Promise<T>;

  // Sleep durably
  sleep(name: string, duration: string): Promise<void>;

  // Invoke another handler and await result
  invoke<T>(service: string, handler: string, args?: unknown, opts?: { key?: string }): Promise<T>;

  // Fire-and-forget dispatch
  send(
    service: string,
    handler: string,
    args?: unknown,
    opts?: { key?: string; delay?: string },
  ): Promise<void>;

  // Create awakeable for external resolution
  awakeable<T>(name: string): Promise<[string, Promise<T>]>;

  // Resolve awakeable from handler
  resolveAwakeable(awakeableId: string, value?: unknown): Promise<void>;

  // Reject awakeable
  rejectAwakeable(awakeableId: string, error: string): Promise<void>;

  // State management for virtual objects
  setState(newState: TState): Promise<void>;
}

// Dispatch options
export interface SendOptions {
  key?: string;
  delay?: string;
  maxRetries?: number;
  idempotencyKey?: string;
}

export interface InvokeOptions extends SendOptions {
  timeoutMs?: number;
}

// Configuration
export interface RedisConfig {
  host: string;
  port: number;
  password?: string;
  db?: number;
  keyPrefix?: string;
  tls?: boolean;
}

export interface WorkerConfig {
  concurrency?: number;
  pollIntervalMs?: number;
  lockTtlMs?: number;
  lockRenewMs?: number;
  ackTimeoutMs?: number;
  timerPollIntervalMs?: number;
}

export interface RetryConfig {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  backoffMultiplier?: number;
}

export interface LogConfig {
  level?: "debug" | "info" | "warn" | "error";
}

export interface DQConfig {
  redis: RedisConfig;
  worker?: WorkerConfig;
  retry?: RetryConfig;
  log?: LogConfig;
}

// Type inference utilities
export type Handler = (ctx: DurableContext, args: any) => Promise<any>;
export type ArgsOf<H> = H extends (ctx: any, args: infer A) => any ? A : never;
export type ReturnOf<H> = H extends (...args: any[]) => Promise<infer R> ? R : never;
