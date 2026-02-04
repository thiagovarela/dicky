import type { RedisClient, RedisDriver } from "./stores/redis";
import type { ZodType, ZodTypeAny } from "zod";

export type InvocationId = string;

// Service/Object definitions
export interface ServiceDef<
  TName extends string = string,
  THandlers extends Record<string, HandlerDef> = Record<string, HandlerDef>,
> {
  readonly __kind: "service";
  readonly name: TName;
  readonly handlers: THandlers;
}

export interface ObjectDef<
  TName extends string = string,
  TState = unknown,
  THandlers extends Record<string, HandlerDef> = Record<string, HandlerDef>,
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

  /**
   * Execute a side effect with exactly-once semantics via the journal.
   */
  run<T>(name: string, fn: () => T | Promise<T>): Promise<T>;

  /**
   * Sleep durably for a given duration string (e.g. "5s", "500ms").
   */
  sleep(name: string, duration: string): Promise<void>;

  /**
   * Invoke another handler and await its result.
   */
  invoke<T>(service: string, handler: string, args?: unknown, opts?: { key?: string }): Promise<T>;

  /**
   * Fire-and-forget dispatch to another handler.
   */
  send(
    service: string,
    handler: string,
    args?: unknown,
    opts?: { key?: string; delay?: string },
  ): Promise<void>;

  /**
   * Create an awakeable that can be resolved externally.
   */
  awakeable<T>(name: string): Promise<[string, Promise<T>]>;

  /**
   * Resolve a previously created awakeable.
   */
  resolveAwakeable(awakeableId: string, value?: unknown): Promise<void>;

  /**
   * Reject a previously created awakeable with an error message.
   */
  rejectAwakeable(awakeableId: string, error: string): Promise<void>;

  /**
   * Persist state for virtual objects.
   */
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
  url?: string;
  driver?: RedisDriver;
  client?: RedisClient;
  blockingClient?: RedisClient;
  factory?: (url: string) => Promise<RedisClient>;
  blockingFactory?: (url: string) => Promise<RedisClient>;
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

export interface RetentionConfig {
  completionTtlMs?: number;
  invocationTtlMs?: number;
  journalTtlMs?: number;
  idempotencyTtlMs?: number;
}

export interface ShutdownConfig {
  handleSignals?: boolean;
  signals?: Array<NodeJS.Signals>;
}

export interface LogConfig {
  level?: "debug" | "info" | "warn" | "error";
}

export interface DickyConfig {
  redis: RedisConfig;
  worker?: WorkerConfig;
  retry?: RetryConfig;
  retention?: RetentionConfig;
  shutdown?: ShutdownConfig;
  log?: LogConfig;
}

export interface ResolvedConfig {
  redis: RedisConfig & { keyPrefix: string; url: string };
  worker: Required<WorkerConfig>;
  retry: Required<RetryConfig>;
  retention: Required<RetentionConfig>;
  shutdown: Required<ShutdownConfig>;
  log?: LogConfig;
}

export interface RegisteredHandler {
  handler: Handler;
  input?: ZodTypeAny;
  output?: ZodTypeAny;
}

export interface RegisteredService {
  kind: "service" | "object";
  name: string;
  handlers: Record<string, RegisteredHandler>;
  initialState?: unknown;
}

export interface Metrics {
  service: string;
  completed: number;
  failed: number;
  replayed: number;
  totalSteps: number;
  avgDurationMs: number;
  pending: number;
  active: number;
  processingErrors: number;
}

export interface DLQEntry {
  invocationId: InvocationId;
  service: string;
  handler: string;
  args: unknown;
  error: string;
  attempt: number;
  failedAt: number;
  streamMessageId: string;
}

// Type inference utilities
export type Handler<TArgs = unknown, TResult = unknown> = {
  bivarianceHack(ctx: DurableContext, args: TArgs): Promise<TResult>;
}["bivarianceHack"];

export interface HandlerWithSchema<TArgs = unknown, TResult = unknown> {
  input: ZodType<TArgs>;
  output?: ZodType<TResult>;
  handler: Handler<TArgs, TResult>;
}

export type HandlerDef<TArgs = unknown, TResult = unknown> =
  | Handler<TArgs, TResult>
  | HandlerWithSchema<TArgs, TResult>;

type HandlerFnOf<H> = H extends { handler: infer F } ? F : H;

export type ArgsOf<H> = HandlerFnOf<H> extends (ctx: any, args: infer A) => any ? A : never;
export type ReturnOf<H> = HandlerFnOf<H> extends (...args: any[]) => Promise<infer R>
  ? R
  : never;
