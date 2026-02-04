# Dicky API

This is how users should create services. 

## Service definition

```typescript
import { service } from '@dicky/dicky';

export const onboarding = service('onboarding', {
  welcome: async (ctx, { userId }: { userId: string }) => {
    const user = await ctx.run('fetch-user', () => db.getUser(userId));
    await ctx.sleep('cool-down', '24h');
    await ctx.run('send-email', () => email.send(user.email, 'Welcome!'));
    return { sent: true };
  },

  remind: async (ctx, { userId, msg }: { userId: string; msg: string }) => {
    await ctx.run('notify', () => push.send(userId, msg));
  },
});
```

## Client definition

```typescript
import { Dicky } from '@dicky/dicky';

import { onboarding } from './services/onboarding';

export const dicky = new Dicky({ url: env.REDIS_URL })
  .add(onboarding)
  
```

## Consumer

```typescript
import { dicky } from '@lib/dicky';

// Fully type safe
await dicky.send('onboarding', 'welcome', { userId: user.id });
  
```


### Advanced dispatch options

```typescript
import { dicky } from '@lib/dicky';

// Delayed execution
await dicky.send('onboarding', 'remind', { userId: '123', msg: 'Hey' }, { delay: '30m' });

// Idempotent (deduplicates by key)
await dicky.send('onboarding', 'welcome', { userId: '123' }, {
  idempotencyKey: `welcome:${userId}`,
});

// Override max retries
await dicky.send('onboarding', 'welcome', { userId: '123' }, { maxRetries: 5 });

// Invoke with timeout
const result = await dicky.invoke('wallet', 'deposit',
  { amount: 50 },
  { key: 'user_123', timeoutMs: 10_000 },
);
```

### Awakeables (pause until external signal)

```typescript
// In a handler — wait for an external webhook
export const payments = service('payments', {
  checkout: async (ctx, { orderId }: { orderId: string }) => {
    await ctx.run('create-charge', () => stripe.charges.create({ ... }));

    // Pause execution until webhook arrives
    const [awakeableId, promise] = await ctx.awakeable('wait-webhook');

    // Share awakeableId with the external system
    await ctx.run('store-awakeable', () => db.saveAwakeableId(orderId, awakeableId));

    // Execution suspends here. Process can crash and restart.
    const webhook = await promise;

    if (webhook.status === 'paid') {
      await ctx.run('fulfill', () => fulfillOrder(orderId));
    }
  },
});

// In a webhook handler — resolve the awakeable
app.post('/webhooks/stripe', async (req, reply) => {
  const awakeableId = await db.getAwakeableId(req.body.orderId);
  await dicky.resolveAwakeable(awakeableId, { status: req.body.status });
  return reply.status(200).send();
});
```

### Handler calling other handlers

```typescript
export const orders = service('orders', {
  process: async (ctx, { orderId }: { orderId: string }) => {
    const order = await ctx.run('fetch', () => db.getOrder(orderId));

    // invoke: call another handler and wait for result (journaled)
    const balance = await ctx.invoke('wallet', 'withdraw',
      { amount: order.total },
      { key: order.userId },
    );

    // send: fire-and-forget to another handler (journaled)
    await ctx.send('onboarding', 'remind', {
      userId: order.userId,
      msg: `Order ${orderId} confirmed!`,
    });

    return { orderId, balance };
  },
});
```

### Lifecycle

```typescript
// main.ts
import { dicky } from '@lib/dicky';

await dicky.start();
await app.listen({ port: 3000 });

const shutdown = async () => {
  await app.close();
  await dicky.stop();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
```


## Type Definitions (`src/types.ts`)

Define ALL types in this file. Every other module imports from here.

```typescript
import type { Logger } from 'pino';

// ---------------------------------------------------------------------------
// Utility types for handler inference
// ---------------------------------------------------------------------------

export type Handler = (ctx: DurableContext, args: any) => Promise;

export type ArgsOf = H extends (ctx: any, args: infer A) => any ? A : never;

export type ReturnOf = H extends (...args: any[]) => Promise ? R : never;

// ---------------------------------------------------------------------------
// Service & Object definition types (returned by factory functions)
// ---------------------------------------------------------------------------

export interface ServiceDef<
  TName extends string = string,
  THandlers extends Record = Record,
> {
  readonly __kind: 'service';
  readonly name: TName;
  readonly handlers: THandlers;
}

export interface ObjectDef<
  TName extends string = string,
  TState = unknown,
  THandlers extends Record = Record,
> {
  readonly __kind: 'object';
  readonly name: TName;
  readonly initial: TState;
  readonly handlers: THandlers;
}

export type AnyDef = ServiceDef | ObjectDef;

// ---------------------------------------------------------------------------
// Registry type — accumulated via .use()
// ---------------------------------------------------------------------------

export type Registry = Record }>;

export type AddToRegistry<
  R extends Registry,
  TName extends string,
  THandlers extends Record,
> = R & Record;

// ---------------------------------------------------------------------------
// Invocation
// ---------------------------------------------------------------------------

export type InvocationId = string; // format: `inv_${nanoid()}`

export type InvocationStatus =
  | 'pending'
  | 'running'
  | 'suspended'
  | 'completed'
  | 'failed'
  | 'cancelled';

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

// ---------------------------------------------------------------------------
// Journal
// ---------------------------------------------------------------------------

export type JournalEntryType =
  | 'run'
  | 'sleep'
  | 'invoke'
  | 'send'
  | 'awakeable'
  | 'state-get'
  | 'state-set';

export type JournalEntryStatus = 'pending' | 'completed' | 'failed';

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

// ---------------------------------------------------------------------------
// Durable Context (developer-facing API)
// ---------------------------------------------------------------------------

export interface DurableContext<TState = unknown> {
  readonly invocationId: InvocationId;
  readonly key: string | undefined;

  /**
   * Execute a side effect with exactly-once semantics.
   * On first execution: runs fn, journals result, returns it.
   * On replay: returns journaled result without calling fn.
   */
  run(name: string, fn: () => T | Promise): Promise;

  /**
   * Sleep durably. Survives process restarts.
   * Duration format: '5s', '30m', '2h', '1d', '500ms'
   */
  sleep(name: string, duration: string): Promise;

  /**
   * Call another durable handler and await its result.
   * The call and result are journaled.
   */
  invoke<T = unknown>(
    service: string,
    handler: string,
    args?: unknown,
    opts?: { key?: string },
  ): Promise;

  /**
   * Fire-and-forget dispatch to another durable handler.
   * The dispatch is journaled (guaranteed delivery).
   */
  send(
    service: string,
    handler: string,
    args?: unknown,
    opts?: { key?: string; delay?: string },
  ): Promise;

  /**
   * Create an awakeable — pauses execution until resolved externally.
   * Returns [awakeableId, promise].
   */
  awakeable<T = unknown>(name: string): Promise]>;

  /**
   * Resolve an awakeable from within a handler.
   */
  resolveAwakeable(awakeableId: string, value?: unknown): Promise;

  /**
   * Reject an awakeable with an error.
   */
  rejectAwakeable(awakeableId: string, error: string): Promise;

  /**
   * Virtual object state. Only available in object handlers.
   * Reading state is free (loaded at invocation start).
   */
  readonly state: TState;

  /**
   * Update virtual object state. Journaled.
   */
  setState(newState: TState): Promise;
}

// ---------------------------------------------------------------------------
// Dispatch options
// ---------------------------------------------------------------------------

export interface SendOptions {
  /** Virtual object key (required for objects, ignored for services). */
  key?: string;
  /** Delay before execution starts. Format: '5s', '30m', '2h', '1d'. */
  delay?: string;
  /** Override default max retries. */
  maxRetries?: number;
  /** Idempotency key — deduplicates dispatches. */
  idempotencyKey?: string;
}

export interface InvokeOptions extends SendOptions {
  /** How long to wait for the result before timing out (ms). Default: 30000. */
  timeoutMs?: number;
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

export interface RedisConfig {
  host: string;
  port: number;
  password?: string;
  db?: number;
  keyPrefix?: string;  // default: 'dicky:'
  tls?: boolean;
}

export interface WorkerConfig {
  /** Max concurrent invocations per process. Default: 10. */
  concurrency?: number;
  /** Stream poll interval in ms. Default: 100. */
  pollIntervalMs?: number;
  /** Virtual object lock TTL in ms. Default: 30000. */
  lockTtlMs?: number;
  /** Lock renewal interval in ms. Default: 10000. */
  lockRenewMs?: number;
  /** Reclaim timeout — messages pending longer than this are reclaimed. Default: 60000. */
  ackTimeoutMs?: number;
  /** Timer poll interval in ms. Default: 1000. */
  timerPollIntervalMs?: number;
}

export interface RetryConfig {
  /** Default max retries for all handlers. Default: 3. */
  maxRetries?: number;
  /** Initial retry delay in ms. Default: 1000. */
  initialDelayMs?: number;
  /** Max retry delay in ms. Default: 30000. */
  maxDelayMs?: number;
  /** Exponential backoff multiplier. Default: 2. */
  backoffMultiplier?: number;
}

export interface LogConfig {
  level?: 'debug' | 'info' | 'warn' | 'error';
  /** Bring your own pino instance. */
  logger?: Logger;
}

export interface DickyConfig {
  redis: RedisConfig;
  worker?: WorkerConfig;
  retry?: RetryConfig;
  log?: LogConfig;
}

/** Config with all defaults resolved. Internal use only. */
export interface ResolvedConfig {
  redis: Required<Omit> & Pick;
  worker: Required;
  retry: Required;
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

export interface Metrics {
  service: string;
  completed: number;
  failed: number;
  replayed: number;
  totalSteps: number;
  avgDurationMs: number;
  pending: number;
  active: number;
}

// ---------------------------------------------------------------------------
// DLQ
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Internal: Stream messages
// ---------------------------------------------------------------------------

export interface StreamMessage {
  messageId: string; // Redis stream message ID (e.g. '1234567890-0')
  invocationId: InvocationId;
  handler: string;
  key?: string;
  args: string; // JSON-serialized
  attempt: string; // stringified number
}

// ---------------------------------------------------------------------------
// Internal: Timer entries
// ---------------------------------------------------------------------------

export type TimerType = 'sleep' | 'delayed' | 'retry';

export interface TimerEntry {
  invocationId: InvocationId;
  sequence?: number; // present for sleep timers
  type: TimerType;
}

// ---------------------------------------------------------------------------
// Internal: Awakeable
// ---------------------------------------------------------------------------

export interface AwakeableEntry {
  invocationId: InvocationId;
  sequence: number;
  status: 'pending' | 'resolved' | 'rejected';
  result?: string;
  error?: string;
}

// ---------------------------------------------------------------------------
// Internal: registered service/object metadata (runtime, no generics)
// ---------------------------------------------------------------------------

export interface RegisteredService {
  kind: 'service' | 'object';
  name: string;
  handlers: Record;
  initialState?: unknown; // only for objects
}
```

---

## Factory Functions (`src/define.ts`)

These are pure data constructors. They don't touch Redis or register anything — they just return typed definitions that `.use()` consumes.

```typescript
import type { DurableContext, ServiceDef, ObjectDef, Handler } from './types.js';

/**
 * Define a stateless service. Returns a typed definition
 * to pass to `dicky.use()`.
 */
export function service<
  TName extends string,
  THandlers extends Record,
>(name: TName, handlers: THandlers): ServiceDef {
  return {
    __kind: 'service' as const,
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
  THandlers extends Record<
    string,
    (ctx: DurableContext, args: any) => Promise
  >,
>(
  name: TName,
  def: { initial: TState; handlers: THandlers },
): ObjectDef {
  return {
    __kind: 'object' as const,
    name,
    initial: def.initial,
    handlers: def.handlers,
  };
}
```

## Dicky Class — Public API (`src/dicky.ts`)

The `Dicky` class is the single entry point. It accumulates type information through `.use()` calls and delegates to internal modules.

### Type-safe dispatch signatures

```typescript
import type {
  DickyConfig,
  ResolvedConfig,
  Registry,
  AddToRegistry,
  ServiceDef,
  ObjectDef,
  Handler,
  ArgsOf,
  ReturnOf,
  SendOptions,
  InvokeOptions,
  Invocation,
  InvocationId,
  JournalEntry,
  Metrics,
  DLQEntry,
  RegisteredService,
} from './types.js';

export class Dicky {
  private services = new Map();
  private config: ResolvedConfig;
  private started = false;
  // ... worker, redis, logger, etc.

  constructor(config: DickyConfig) {
    this.config = resolveConfig(config);
  }

  // ─── Registration via .use() ───

  /**
   * Register a service definition. Returns `this` with an
   * expanded type that includes the new service.
   */
  use>(
    def: ServiceDef,
  ): Dicky<AddToRegistry>;

  /**
   * Register an object definition.
   */
  use>(
    def: ObjectDef,
  ): Dicky<AddToRegistry>;

  /**
   * Implementation (single overload body).
   */
  use(def: ServiceDef | ObjectDef): Dicky {
    if (this.started) throw new Error('Cannot register after start()');
    if (this.services.has(def.name)) throw new Error(`Service "${def.name}" already registered`);

    if (def.__kind === 'service') {
      this.services.set(def.name, {
        kind: 'service',
        name: def.name,
        handlers: def.handlers,
      });
    } else {
      this.services.set(def.name, {
        kind: 'object',
        name: def.name,
        handlers: def.handlers,
        initialState: def.initial,
      });
    }

    return this as Dicky;
  }

  // ─── Dispatch: send (fire-and-forget) ───

  /**
   * Enqueue an invocation. Returns the invocation ID immediately.
   *
   * Type inference:
   * - If S is a registered service name → handler names and args are typed
   * - If S is an arbitrary string → falls back to (string, unknown)
   */
  async send<
    S extends (keyof R & string) | (string & {}),
    H extends S extends keyof R
      ? (keyof R[S]['handlers'] & string) | (string & {})
      : string,
  >(
    service: S,
    handler: H,
    args: S extends keyof R
      ? H extends keyof R[S]['handlers']
        ? ArgsOf
        : unknown
      : unknown,
    opts?: SendOptions,
  ): Promise {
    // 1. Validate service + handler exist (if registered locally)
    // 2. Generate invocation ID
    // 3. Create invocation record in Redis
    // 4. If delay: ZADD to timer sorted set
    //    Else: XADD to service stream
    // 5. Return invocation ID
  }

  // ─── Dispatch: invoke (request-response) ───

  /**
   * Dispatch and wait for the handler to complete.
   * Uses Redis pub/sub on channel `dicky:completion:{invocationId}`.
   */
  async invoke<
    S extends (keyof R & string) | (string & {}),
    H extends S extends keyof R
      ? (keyof R[S]['handlers'] & string) | (string & {})
      : string,
  >(
    service: S,
    handler: H,
    args: S extends keyof R
      ? H extends keyof R[S]['handlers']
        ? ArgsOf
        : unknown
      : unknown,
    opts?: InvokeOptions,
  ): Promise<
    S extends keyof R
      ? H extends keyof R[S]['handlers']
        ? ReturnOf
        : unknown
      : unknown
  > {
    // 1. Subscribe to completion channel BEFORE dispatching (avoid race)
    // 2. Call this.send(service, handler, args, opts)
    // 3. Wait for pub/sub message or timeout
    // 4. Parse and return result, or throw if failed
  }

  // ─── Lifecycle ───

  /**
   * Start the runtime:
   * 1. Connect to Redis
   * 2. Load Lua scripts (SCRIPT LOAD, cache SHAs)
   * 3. Create consumer groups for each registered service
   * 4. Start worker loop (XREADGROUP polling)
   * 5. Start timer loop (sorted set polling)
   * 6. Start reclaim loop (XAUTOCLAIM for stale messages)
   */
  async start(): Promise;

  /**
   * Graceful shutdown:
   * 1. Stop accepting new work from streams
   * 2. Wait for active invocations to reach next journal checkpoint
   * 3. Release all virtual object locks
   * 4. Close Redis connections
   */
  async stop(): Promise;

  // ─── Awakeable resolution (from outside handlers) ───

  async resolveAwakeable(awakeableId: string, value?: unknown): Promise;
  async rejectAwakeable(awakeableId: string, error: string): Promise;

  // ─── Introspection ───

  async getInvocation(id: InvocationId): Promise;
  async getJournal(id: InvocationId): Promise;
  async cancel(id: InvocationId): Promise;
  async getMetrics(service?: string): Promise;
  async listDLQ(service: string, opts?: { limit?: number }): Promise;
  async retryDLQ(id: InvocationId): Promise;
}
```

### Lua Scripts

```typescript
/**
 * Load Lua scripts at startup. Cache SHA hashes for EVALSHA.
 */
export class LuaScripts {
  private shas = new Map();

  async load(redis: Redis): Promise {
    // Load each .lua file from the lua/ directory
    // For each: SCRIPT LOAD → store SHA
    // Scripts: journal-write, timer-poll, lock-acquire, lock-renew, lock-release
  }

  async eval(redis: Redis, script: string, keys: string[], args: string[]): Promise {
    const sha = this.shas.get(script);
    if (!sha) throw new Error(`Script "${script}" not loaded`);
    try {
      return await redis.evalsha(sha, keys.length, ...keys, ...args);
    } catch (err: any) {
      if (err.message?.includes('NOSCRIPT')) {
        // Fallback: reload and retry
        await this.load(redis);
        return this.eval(redis, script, keys, args);
      }
      throw err;
    }
  }
}
```

### Key Builder (`src/utils.ts`)

```typescript
export function keys(prefix: string) {
  return {
    stream: (service: string) => `${prefix}stream:${service}`,
    consumerGroup: (service: string) => `${prefix}cg:${service}`,
    consumerName: (workerId: string) => `${prefix}worker:${workerId}`,
    journal: (invocationId: string) => `${prefix}journal:${invocationId}`,
    invocation: (invocationId: string) => `${prefix}invocation:${invocationId}`,
    state: (objectName: string, key: string) => `${prefix}state:${objectName}:${key}`,
    lock: (objectName: string, key: string) => `${prefix}lock:${objectName}:${key}`,
    timers: () => `${prefix}timers`,
    awakeable: (awakeableId: string) => `${prefix}awakeable:${awakeableId}`,
    dlq: (service: string) => `${prefix}dlq:${service}`,
    completion: (invocationId: string) => `${prefix}completion:${invocationId}`,
    idempotency: (key: string) => `${prefix}idempotent:${key}`,
    metrics: (service: string) => `${prefix}metrics:${service}`,
  };
}
```

### Journal Store (`src/journal.ts`)

```typescript
export class JournalStore {
  constructor(
    private redis: Redis,
    private scripts: LuaScripts,
    private prefix: string,
  ) {}

  /**
   * Read a journal entry by invocation ID and sequence number.
   * Returns null on miss (first execution).
   */
  async get(invocationId: string, sequence: number): Promise {
    const key = keys(this.prefix).journal(invocationId);
    const raw = await this.redis.hget(key, String(sequence));
    return raw ? JSON.parse(raw) : null;
  }

  /**
   * Write a journal entry atomically.
   * Uses Lua script to prevent double-write (HEXISTS + HSET).
   * Returns false if entry already exists at this sequence.
   */
  async write(entry: JournalEntry): Promise {
    const key = keys(this.prefix).journal(entry.invocationId);
    const result = await this.scripts.eval(this.redis, 'journal-write', [key], [
      String(entry.sequence),
      JSON.stringify(entry),
    ]);
    return result === 1;
  }

  /**
   * Mark a pending entry as completed.
   * Used by timer loop for sleep wake-ups and awakeable resolutions.
   */
  async complete(invocationId: string, sequence: number, result?: string): Promise {
    const key = keys(this.prefix).journal(invocationId);
    const raw = await this.redis.hget(key, String(sequence));
    if (!raw) return;
    const entry: JournalEntry = JSON.parse(raw);
    entry.status = 'completed';
    entry.result = result;
    entry.completedAt = Date.now();
    await this.redis.hset(key, String(sequence), JSON.stringify(entry));
  }

  /**
   * Get all entries for an invocation, ordered by sequence.
   */
  async getAll(invocationId: string): Promise {
    const key = keys(this.prefix).journal(invocationId);
    const all = await this.redis.hgetall(key);
    return Object.entries(all)
      .map(([, v]) => JSON.parse(v) as JournalEntry)
      .sort((a, b) => a.sequence - b.sequence);
  }

  /**
   * Get current journal length (number of entries).
   */
  async length(invocationId: string): Promise {
    const key = keys(this.prefix).journal(invocationId);
    return this.redis.hlen(key);
  }
}
```

### DurableContext Implementation (`src/context.ts`)

The context maintains a **sequence counter** that increments with every journaled operation. On replay, it reads from the journal instead of executing.

```typescript
import { SuspendedError } from './errors.js';
import type {
  DurableContext,
  InvocationId,
  JournalEntry,
  JournalEntryType,
} from './types.js';

export class DurableContextImpl<TState = unknown> implements DurableContext {
  private sequence = 0;
  private _state: TState;

  constructor(
    private invocation: { id: InvocationId; key?: string },
    private journal: JournalStore,
    private timerStore: TimerStore,
    private stateStore: StateStore,
    private awakeableStore: AwakeableStore,
    private streamProducer: StreamProducer,
    private objectDef: { initialState?: TState } | undefined,
    initialState: TState,
  ) {
    this._state = initialState;
  }

  get invocationId() { return this.invocation.id; }
  get key() { return this.invocation.key; }
  get state() { return this._state; }

  async run(name: string, fn: () => T | Promise): Promise {
    const seq = this.sequence++;

    // 1. Check journal (replay path)
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === 'completed') {
      // Replay: return stored result without executing fn
      return existing.result != null ? JSON.parse(existing.result) : undefined;
    }

    if (existing?.status === 'failed') {
      // Replay: re-throw stored error
      throw new ReplayError(name, existing.error ?? 'Unknown error');
    }

    // 2. First execution — run the side effect
    try {
      const result = await fn();
      const serialized = result !== undefined ? JSON.stringify(result) : undefined;

      // 3. Journal the result
      await this.journal.write({
        invocationId: this.invocationId,
        sequence: seq,
        type: 'run',
        name,
        status: 'completed',
        result: serialized,
        createdAt: Date.now(),
        completedAt: Date.now(),
      });

      return result;
    } catch (err) {
      // 4. Journal the failure
      const errorMsg = err instanceof Error ? err.message : String(err);
      await this.journal.write({
        invocationId: this.invocationId,
        sequence: seq,
        type: 'run',
        name,
        status: 'failed',
        error: errorMsg,
        createdAt: Date.now(),
        completedAt: Date.now(),
      });
      throw err;
    }
  }

  async sleep(name: string, duration: string): Promise {
    const seq = this.sequence++;

    // Check journal
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === 'completed') {
      // Sleep already elapsed on previous run — skip
      return;
    }

    if (existing?.status === 'pending') {
      // Sleep was scheduled but hasn't elapsed — suspend again
      throw new SuspendedError(this.invocationId, seq, 'sleep');
    }

    // First execution — schedule timer
    const durationMs = parseDuration(duration);
    const wakeAt = Date.now() + durationMs;

    await this.journal.write({
      invocationId: this.invocationId,
      sequence: seq,
      type: 'sleep',
      name,
      status: 'pending',
      createdAt: Date.now(),
    });

    await this.timerStore.schedule(
      { invocationId: this.invocationId, sequence: seq, type: 'sleep' },
      wakeAt,
    );

    throw new SuspendedError(this.invocationId, seq, 'sleep');
  }

  async invoke<T = unknown>(
    service: string,
    handler: string,
    args?: unknown,
    opts?: { key?: string },
  ): Promise {
    const seq = this.sequence++;
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === 'completed') {
      return existing.result != null ? JSON.parse(existing.result) : undefined;
    }

    if (existing?.status === 'failed') {
      throw new ReplayError(`${service}.${handler}`, existing.error ?? 'Unknown error');
    }

    if (existing?.status === 'pending') {
      // Sub-invocation dispatched but not yet completed — suspend
      throw new SuspendedError(this.invocationId, seq, 'awakeable');
    }

    // First execution: dispatch the sub-invocation and suspend
    // The sub-invocation's completion will write back to our journal
    const childId = await this.streamProducer.dispatch(service, handler, args, {
      key: opts?.key,
      parentId: this.invocationId,
      parentStep: seq,
    });

    await this.journal.write({
      invocationId: this.invocationId,
      sequence: seq,
      type: 'invoke',
      name: `${service}.${handler}`,
      status: 'pending',
      createdAt: Date.now(),
    });

    throw new SuspendedError(this.invocationId, seq, 'awakeable');
  }

  async send(
    service: string,
    handler: string,
    args?: unknown,
    opts?: { key?: string; delay?: string },
  ): Promise {
    const seq = this.sequence++;
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === 'completed') {
      return; // already dispatched on previous run
    }

    // Dispatch and journal
    await this.streamProducer.dispatch(service, handler, args, {
      key: opts?.key,
      delay: opts?.delay,
    });

    await this.journal.write({
      invocationId: this.invocationId,
      sequence: seq,
      type: 'send',
      name: `${service}.${handler}`,
      status: 'completed',
      createdAt: Date.now(),
      completedAt: Date.now(),
    });
  }

  async awakeable<T = unknown>(name: string): Promise]> {
    const seq = this.sequence++;
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === 'completed') {
      // Awakeable was resolved on a previous run
      const value = existing.result != null ? JSON.parse(existing.result) : undefined;
      return [existing.name, Promise.resolve(value as T)];
    }

    if (existing?.status === 'failed') {
      throw new ReplayError(name, existing.error ?? 'Awakeable rejected');
    }

    if (existing?.status === 'pending') {
      // Awakeable created but not yet resolved — suspend
      throw new SuspendedError(this.invocationId, seq, 'awakeable');
    }

    // First execution: create awakeable
    const awakeableId = newAwakeableId();

    await this.awakeableStore.create({
      invocationId: this.invocationId,
      sequence: seq,
      status: 'pending',
    }, awakeableId);

    await this.journal.write({
      invocationId: this.invocationId,
      sequence: seq,
      type: 'awakeable',
      name: awakeableId, // store the ID in name field for replay
      status: 'pending',
      createdAt: Date.now(),
    });

    throw new SuspendedError(this.invocationId, seq, 'awakeable');
  }

  async resolveAwakeable(awakeableId: string, value?: unknown): Promise {
    await this.awakeableStore.resolve(awakeableId, value);
  }

  async rejectAwakeable(awakeableId: string, error: string): Promise {
    await this.awakeableStore.reject(awakeableId, error);
  }

  async setState(newState: TState): Promise {
    const seq = this.sequence++;
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === 'completed') {
      // Replay: apply the state that was set
      this._state = existing.result != null ? JSON.parse(existing.result) : newState;
      return;
    }

    // Write state to Redis
    await this.stateStore.set(
      this.invocation.key!,
      JSON.stringify(newState),
    );
    this._state = newState;

    await this.journal.write({
      invocationId: this.invocationId,
      sequence: seq,
      type: 'state-set',
      name: 'setState',
      status: 'completed',
      result: JSON.stringify(newState),
      createdAt: Date.now(),
      completedAt: Date.now(),
    });
  }
}
```

### Worker Loop (`src/worker.ts`)

The worker consumes from Redis Streams and executes handlers.

```typescript
export class Worker {
  private workerId: string;
  private running = false;
  private activeInvocations = new Set();
  private abortController = new AbortController();

  constructor(
    private redis: Redis,
    private blockingRedis: Redis, // separate connection for blocking XREADGROUP
    private services: Map,
    private journal: JournalStore,
    private timerStore: TimerStore,
    private lockManager: LockManager,
    private stateStore: StateStore,
    private awakeableStore: AwakeableStore,
    private streamProducer: StreamProducer,
    private dlq: DLQStore,
    private metrics: MetricsCollector,
    private config: ResolvedConfig,
    private logger: Logger,
  ) {
    this.workerId = newWorkerId();
  }

  async start(): Promise {
    this.running = true;

    // Ensure consumer groups exist for each service
    for (const [name] of this.services) {
      await this.ensureConsumerGroup(name);
    }

    // Start loops (all run concurrently, non-blocking)
    await Promise.all([
      this.consumerLoop(),
      this.timerLoop(),
      this.reclaimLoop(),
    ]);
  }

  async stop(): Promise {
    this.running = false;
    this.abortController.abort();

    // Wait for active invocations to complete (with timeout)
    const timeout = setTimeout(() => {}, 10_000);
    while (this.activeInvocations.size > 0) {
      await new Promise((r) => setTimeout(r, 100));
    }
    clearTimeout(timeout);
  }

  // ─── Consumer Loop ───

  private async consumerLoop(): Promise {
    const concurrency = this.config.worker.concurrency;
    let activeCount = 0;

    while (this.running) {
      // Respect concurrency limit
      if (activeCount >= concurrency) {
        await new Promise((r) => setTimeout(r, 50));
        continue;
      }

      // XREADGROUP across all service streams
      // Block for pollIntervalMs — doesn't block the event loop
      // (uses the dedicated blockingRedis connection)
      const streams = [...this.services.keys()].map(
        (name) => keys(this.config.redis.keyPrefix).stream(name),
      );
      const groups = [...this.services.keys()].map(
        (name) => keys(this.config.redis.keyPrefix).consumerGroup(name),
      );

      try {
        // Read one message from any registered stream
        for (const [serviceName, service] of this.services) {
          const streamKey = keys(this.config.redis.keyPrefix).stream(serviceName);
          const groupName = keys(this.config.redis.keyPrefix).consumerGroup(serviceName);
          const consumerName = keys(this.config.redis.keyPrefix).consumerName(this.workerId);

          const results = await this.blockingRedis.xreadgroup(
            'GROUP', groupName, consumerName,
            'COUNT', '1',
            'BLOCK', String(this.config.worker.pollIntervalMs),
            'STREAMS', streamKey, '>',
          );

          if (!results || results.length === 0) continue;

          const [, messages] = results[0]!;
          for (const [messageId, fields] of messages!) {
            const msg = parseStreamMessage(messageId, fields);
            activeCount++;
            this.activeInvocations.add(msg.invocationId);

            // Process in background (non-blocking)
            this.processMessage(serviceName, msg)
              .catch((err) => this.logger.error({ err, msg }, 'Failed to process message'))
              .finally(() => {
                activeCount--;
                this.activeInvocations.delete(msg.invocationId);
              });
          }
        }
      } catch (err) {
        if (!this.running) break; // expected during shutdown
        this.logger.error({ err }, 'Consumer loop error');
        await new Promise((r) => setTimeout(r, 1000)); // backoff on error
      }
    }
  }

  // ─── Process a single message ───

  private async processMessage(serviceName: string, msg: StreamMessage): Promise {
    const service = this.services.get(serviceName);
    if (!service) {
      this.logger.warn({ serviceName }, 'Message for unknown service');
      return;
    }

    const handler = service.handlers[msg.handler];
    if (!handler) {
      this.logger.warn({ serviceName, handler: msg.handler }, 'Unknown handler');
      return;
    }

    const invocation = await this.loadOrCreateInvocation(msg, serviceName, service);
    const streamKey = keys(this.config.redis.keyPrefix).stream(serviceName);
    const groupName = keys(this.config.redis.keyPrefix).consumerGroup(serviceName);

    // Acquire lock for virtual objects
    let lockGuard: LockGuard | null = null;
    if (service.kind === 'object' && invocation.key) {
      try {
        lockGuard = await this.lockManager.acquire(serviceName, invocation.key);
      } catch {
        // Can't get lock — the message stays pending, will be reclaimed later
        this.logger.debug({ serviceName, key: invocation.key }, 'Lock conflict, will retry');
        return;
      }
    }

    try {
      // Update status to running
      await this.updateInvocationStatus(invocation.id, 'running');

      // Load virtual object state
      let initialState: unknown = undefined;
      if (service.kind === 'object' && invocation.key) {
        const stored = await this.stateStore.get(serviceName, invocation.key);
        initialState = stored != null ? JSON.parse(stored) : service.initialState;
      }

      // Create context
      const ctx = new DurableContextImpl(
        invocation,
        this.journal,
        this.timerStore,
        this.stateStore,
        this.awakeableStore,
        this.streamProducer,
        service.kind === 'object' ? { initialState: service.initialState } : undefined,
        initialState,
      );

      // Execute the handler
      const args = JSON.parse(msg.args);
      const result = await handler(ctx, args);

      // Success — complete the invocation
      const serializedResult = result !== undefined ? JSON.stringify(result) : undefined;
      await this.completeInvocation(invocation.id, serializedResult);
      await this.ack(streamKey, groupName, msg.messageId);

      // Notify waiters (for invoke/dispatchAndWait)
      await this.publishCompletion(invocation.id, 'completed', serializedResult);

      // If this invocation has a parent (ctx.invoke from another handler),
      // complete the parent's journal entry and re-enqueue the parent
      if (invocation.parentId != null && invocation.parentStep != null) {
        await this.journal.complete(invocation.parentId, invocation.parentStep, serializedResult);
        await this.streamProducer.reenqueue(invocation.parentId);
      }

      this.metrics.recordCompleted(serviceName);

    } catch (err) {
      if (err instanceof SuspendedError) {
        // Handler suspended (sleep/awakeable) — ACK the message
        await this.updateInvocationStatus(invocation.id, 'suspended');
        await this.ack(streamKey, groupName, msg.messageId);
        return;
      }

      // Handler error — retry or DLQ
      const attempt = Number(msg.attempt);
      const maxRetries = invocation.maxRetries;

      if (attempt + 1 >= maxRetries) {
        // Exhausted retries — move to DLQ
        const errorMsg = err instanceof Error ? err.message : String(err);
        await this.dlq.push(invocation, errorMsg);
        await this.failInvocation(invocation.id, errorMsg);
        await this.ack(streamKey, groupName, msg.messageId);
        await this.publishCompletion(invocation.id, 'failed', undefined, errorMsg);

        if (invocation.parentId != null && invocation.parentStep != null) {
          await this.journal.fail(invocation.parentId, invocation.parentStep, errorMsg);
          await this.streamProducer.reenqueue(invocation.parentId);
        }

        this.metrics.recordFailed(serviceName);
      } else {
        // Schedule retry with exponential backoff
        const delay = computeRetryDelay(
          attempt,
          this.config.retry.initialDelayMs,
          this.config.retry.maxDelayMs,
          this.config.retry.backoffMultiplier,
        );
        await this.timerStore.schedule(
          { invocationId: invocation.id, type: 'retry' },
          Date.now() + delay,
        );
        await this.ack(streamKey, groupName, msg.messageId);
      }
    } finally {
      // Release virtual object lock
      if (lockGuard) {
        await lockGuard.release();
      }
    }
  }

  // ─── Timer Loop ───

  private async timerLoop(): Promise {
    const interval = this.config.worker.timerPollIntervalMs;

    while (this.running) {
      try {
        const expired = await this.timerStore.pollExpired(Date.now(), 10);

        for (const entry of expired) {
          switch (entry.type) {
            case 'sleep': {
              // Mark journal entry as completed
              await this.journal.complete(entry.invocationId, entry.sequence!, undefined);
              // Re-enqueue the invocation to resume execution
              await this.streamProducer.reenqueue(entry.invocationId);
              break;
            }
            case 'delayed': {
              // Delayed dispatch — enqueue for first time
              await this.streamProducer.enqueueDelayed(entry.invocationId);
              break;
            }
            case 'retry': {
              // Retry — re-enqueue with incremented attempt counter
              await this.streamProducer.reenqueueRetry(entry.invocationId);
              break;
            }
          }
        }
      } catch (err) {
        if (!this.running) break;
        this.logger.error({ err }, 'Timer loop error');
      }

      await new Promise((r) => setTimeout(r, interval));
    }
  }

  // ─── Reclaim Loop (stale message recovery) ───

  private async reclaimLoop(): Promise {
    const interval = this.config.worker.ackTimeoutMs / 2;

    while (this.running) {
      try {
        for (const [serviceName] of this.services) {
          const streamKey = keys(this.config.redis.keyPrefix).stream(serviceName);
          const groupName = keys(this.config.redis.keyPrefix).consumerGroup(serviceName);
          const consumerName = keys(this.config.redis.keyPrefix).consumerName(this.workerId);
          const minIdleMs = this.config.worker.ackTimeoutMs;

          // XAUTOCLAIM: atomically claim messages pending longer than ackTimeoutMs
          const [, reclaimed] = await this.redis.xautoclaim(
            streamKey, groupName, consumerName,
            minIdleMs, '0-0', 'COUNT', '10',
          );

          for (const [messageId, fields] of reclaimed ?? []) {
            const msg = parseStreamMessage(messageId, fields);
            // Re-process. Journal ensures idempotency —
            // already-completed steps will be replayed.
            await this.processMessage(serviceName, msg);
          }
        }
      } catch (err) {
        if (!this.running) break;
        this.logger.error({ err }, 'Reclaim loop error');
      }

      await new Promise((r) => setTimeout(r, interval));
    }
  }

  // ─── Helpers ───

  private async ensureConsumerGroup(serviceName: string): Promise {
    const streamKey = keys(this.config.redis.keyPrefix).stream(serviceName);
    const groupName = keys(this.config.redis.keyPrefix).consumerGroup(serviceName);
    try {
      await this.redis.xgroup('CREATE', streamKey, groupName, '0', 'MKSTREAM');
    } catch (err: any) {
      if (!err.message?.includes('BUSYGROUP')) throw err;
      // Group already exists — fine
    }
  }

  private async ack(stream: string, group: string, messageId: string): Promise {
    await this.redis.xack(stream, group, messageId);
  }
}
```

### Stream Producer (`src/stream.ts`)

```typescript
export class StreamProducer {
  constructor(
    private redis: Redis,
    private prefix: string,
  ) {}

  /**
   * Add an invocation to a service's stream.
   */
  async enqueue(
    serviceName: string,
    invocationId: string,
    handler: string,
    args: string,
    opts?: { key?: string; attempt?: number },
  ): Promise {
    const streamKey = keys(this.prefix).stream(serviceName);
    const fields: Record = {
      invocationId,
      handler,
      args,
      attempt: String(opts?.attempt ?? 0),
    };
    if (opts?.key) fields.key = opts.key;

    return this.redis.xadd(streamKey, '*', ...Object.entries(fields).flat());
  }

  /**
   * Re-enqueue an existing invocation (for sleep wake-up, awakeable resolution).
   * Reads invocation metadata from Redis to reconstruct the stream message.
   */
  async reenqueue(invocationId: string): Promise {
    const invKey = keys(this.prefix).invocation(invocationId);
    const inv = await this.redis.hgetall(invKey);
    if (!inv.service) return;

    await this.enqueue(inv.service, invocationId, inv.handler, inv.args, {
      key: inv.key,
      attempt: Number(inv.attempt ?? 0),
    });
  }

  /**
   * Re-enqueue with incremented attempt counter.
   */
  async reenqueueRetry(invocationId: string): Promise {
    const invKey = keys(this.prefix).invocation(invocationId);
    const inv = await this.redis.hgetall(invKey);
    if (!inv.service) return;

    const newAttempt = Number(inv.attempt ?? 0) + 1;
    await this.redis.hset(invKey, 'attempt', String(newAttempt));

    await this.enqueue(inv.service, invocationId, inv.handler, inv.args, {
      key: inv.key,
      attempt: newAttempt,
    });
  }

  /**
   * Enqueue a delayed invocation for the first time.
   */
  async enqueueDelayed(invocationId: string): Promise {
    return this.reenqueue(invocationId);
  }

  /**
   * Dispatch a new invocation (used by ctx.invoke / ctx.send).
   * Creates the invocation record and enqueues.
   */
  async dispatch(
    service: string,
    handler: string,
    args: unknown,
    opts?: {
      key?: string;
      delay?: string;
      parentId?: string;
      parentStep?: number;
      maxRetries?: number;
      idempotencyKey?: string;
    },
  ): Promise {
    const invocationId = newInvocationId();
    const serializedArgs = JSON.stringify(args ?? null);

    // Create invocation record
    const invKey = keys(this.prefix).invocation(invocationId);
    await this.redis.hmset(invKey, {
      id: invocationId,
      service,
      handler,
      args: serializedArgs,
      status: 'pending',
      attempt: '0',
      maxRetries: String(opts?.maxRetries ?? 3),
      createdAt: String(Date.now()),
      updatedAt: String(Date.now()),
      ...(opts?.key ? { key: opts.key } : {}),
      ...(opts?.parentId ? { parentId: opts.parentId } : {}),
      ...(opts?.parentStep != null ? { parentStep: String(opts.parentStep) } : {}),
    });

    if (opts?.delay) {
      // Schedule for later
      const delayMs = parseDuration(opts.delay);
      const timerKey = keys(this.prefix).timers();
      const entry: TimerEntry = { invocationId, type: 'delayed' };
      await this.redis.zadd(timerKey, Date.now() + delayMs, JSON.stringify(entry));
    } else {
      // Enqueue immediately
      await this.enqueue(service, invocationId, handler, serializedArgs, { key: opts?.key });
    }

    return invocationId;
  }
}
```

### Lock Manager (`src/lock.ts`)

Virtual objects require single-writer semantics.

```typescript
export class LockManager {
  constructor(
    private redis: Redis,
    private scripts: LuaScripts,
    private prefix: string,
    private ttlMs: number,
    private renewMs: number,
  ) {}

  async acquire(objectName: string, key: string): Promise {
    const lockKey = keys(this.prefix).lock(objectName, key);
    const token = newLockToken();

    const acquired = await this.scripts.eval(this.redis, 'lock-acquire', [lockKey], [
      token,
      String(this.ttlMs),
    ]);

    if (acquired !== 1) {
      throw new LockConflictError(objectName, key);
    }

    return new LockGuard(this.redis, this.scripts, lockKey, token, this.ttlMs, this.renewMs);
  }
}

export class LockGuard {
  private renewTimer: ReturnType | null = null;

  constructor(
    private redis: Redis,
    private scripts: LuaScripts,
    private lockKey: string,
    private token: string,
    private ttlMs: number,
    renewMs: number,
  ) {
    // Auto-renew lock while held
    this.renewTimer = setInterval(async () => {
      try {
        await this.scripts.eval(this.redis, 'lock-renew', [this.lockKey], [
          this.token,
          String(this.ttlMs),
        ]);
      } catch {
        // Lock lost — stop renewing
        this.stopRenew();
      }
    }, renewMs);
  }

  async release(): Promise {
    this.stopRenew();
    await this.scripts.eval(this.redis, 'lock-release', [this.lockKey], [this.token]);
  }

  private stopRenew(): void {
    if (this.renewTimer) {
      clearInterval(this.renewTimer);
      this.renewTimer = null;
    }
  }
}
```

### Timer Store (`src/timer.ts`)

```typescript
export class TimerStore {
  constructor(
    private redis: Redis,
    private scripts: LuaScripts,
    private prefix: string,
  ) {}

  /**
   * Schedule a timer entry at a future timestamp.
   */
  async schedule(entry: TimerEntry, wakeAtMs: number): Promise {
    const timerKey = keys(this.prefix).timers();
    await this.redis.zadd(timerKey, wakeAtMs, JSON.stringify(entry));
  }

  /**
   * Poll for expired timers. Atomically removes and returns them.
   * Uses Lua script for atomicity (ZRANGEBYSCORE + ZREM).
   */
  async pollExpired(now: number, limit: number): Promise {
    const timerKey = keys(this.prefix).timers();
    const results = await this.scripts.eval(this.redis, 'timer-poll', [timerKey], [
      String(now),
      String(limit),
    ]) as string[];

    return (results ?? []).map((r) => JSON.parse(r) as TimerEntry);
  }
}
```

### State Store (`src/state.ts`)

```typescript
export class StateStore {
  constructor(
    private redis: Redis,
    private prefix: string,
  ) {}

  async get(objectName: string, key: string): Promise {
    const stateKey = keys(this.prefix).state(objectName, key);
    return this.redis.get(stateKey);
  }

  async set(objectName: string, key: string, value: string): Promise {
    const stateKey = keys(this.prefix).state(objectName, key);
    await this.redis.set(stateKey, value);
  }
}
```

### Awakeable Store (`src/awakeable.ts`)

```typescript
export class AwakeableStore {
  constructor(
    private redis: Redis,
    private journal: JournalStore,
    private streamProducer: StreamProducer,
    private prefix: string,
  ) {}

  async create(entry: AwakeableEntry, awakeableId: string): Promise {
    const awakeableKey = keys(this.prefix).awakeable(awakeableId);
    await this.redis.hmset(awakeableKey, {
      invocationId: entry.invocationId,
      sequence: String(entry.sequence),
      status: 'pending',
    });
  }

  async resolve(awakeableId: string, value?: unknown): Promise {
    const awakeableKey = keys(this.prefix).awakeable(awakeableId);
    const entry = await this.redis.hgetall(awakeableKey);
    if (!entry.invocationId) throw new AwakeableError(awakeableId, 'not found');
    if (entry.status !== 'pending') throw new AwakeableError(awakeableId, `already ${entry.status}`);

    const serialized = value !== undefined ? JSON.stringify(value) : undefined;

    await this.redis.hmset(awakeableKey, { status: 'resolved', result: serialized ?? '' });

    // Complete the journal entry
    await this.journal.complete(entry.invocationId, Number(entry.sequence), serialized);

    // Re-enqueue the invocation to resume
    await this.streamProducer.reenqueue(entry.invocationId);
  }

  async reject(awakeableId: string, error: string): Promise {
    const awakeableKey = keys(this.prefix).awakeable(awakeableId);
    const entry = await this.redis.hgetall(awakeableKey);
    if (!entry.invocationId) throw new AwakeableError(awakeableId, 'not found');
    if (entry.status !== 'pending') throw new AwakeableError(awakeableId, `already ${entry.status}`);

    await this.redis.hmset(awakeableKey, { status: 'rejected', error });

    // Fail the journal entry
    const journalKey = keys(this.prefix).journal(entry.invocationId);
    const seq = Number(entry.sequence);
    const raw = await this.redis.hget(journalKey, String(seq));
    if (raw) {
      const journalEntry: JournalEntry = JSON.parse(raw);
      journalEntry.status = 'failed';
      journalEntry.error = error;
      journalEntry.completedAt = Date.now();
      await this.redis.hset(journalKey, String(seq), JSON.stringify(journalEntry));
    }

    // Re-enqueue the invocation to resume (handler will see the error on replay)
    await this.streamProducer.reenqueue(entry.invocationId);
  }
}
```

### DLQ Store (`src/dlq.ts`)

```typescript
export class DLQStore {
  constructor(
    private redis: Redis,
    private prefix: string,
  ) {}

  async push(invocation: Invocation, error: string): Promise {
    const dlqKey = keys(this.prefix).dlq(invocation.service);
    await this.redis.xadd(dlqKey, '*',
      'invocationId', invocation.id,
      'service', invocation.service,
      'handler', invocation.handler,
      'args', JSON.stringify(invocation.args),
      'error', error,
      'attempt', String(invocation.attempt),
      'failedAt', String(Date.now()),
    );
  }

  async list(service: string, limit = 50): Promise {
    const dlqKey = keys(this.prefix).dlq(service);
    const results = await this.redis.xrevrange(dlqKey, '+', '-', 'COUNT', limit);
    return results.map(([messageId, fields]) => {
      const map = Object.fromEntries(
        fields.reduce((acc, val, i) => {
          if (i % 2 === 0) acc.push([val, fields[i + 1]!]);
          return acc;
        }, []),
      );
      return {
        invocationId: map.invocationId!,
        service: map.service!,
        handler: map.handler!,
        args: JSON.parse(map.args!),
        error: map.error!,
        attempt: Number(map.attempt),
        failedAt: Number(map.failedAt),
        streamMessageId: messageId,
      };
    });
  }
}
```

### Metrics Collector (`src/metrics.ts`)

```typescript
export class MetricsCollector {
  constructor(
    private redis: Redis,
    private prefix: string,
  ) {}

  async recordCompleted(service: string, durationMs?: number): Promise {
    const key = keys(this.prefix).metrics(service);
    const pipeline = this.redis.pipeline();
    pipeline.hincrby(key, 'completed', 1);
    if (durationMs != null) {
      pipeline.hincrby(key, 'totalDurationMs', Math.round(durationMs));
    }
    await pipeline.exec();
  }

  async recordFailed(service: string): Promise {
    const key = keys(this.prefix).metrics(service);
    await this.redis.hincrby(key, 'failed', 1);
  }

  async recordReplayed(service: string, steps: number): Promise {
    const key = keys(this.prefix).metrics(service);
    const pipeline = this.redis.pipeline();
    pipeline.hincrby(key, 'replayed', 1);
    pipeline.hincrby(key, 'totalSteps', steps);
    await pipeline.exec();
  }

  async get(service: string): Promise {
    const key = keys(this.prefix).metrics(service);
    const raw = await this.redis.hgetall(key);
    const completed = Number(raw.completed ?? 0);
    const totalDurationMs = Number(raw.totalDurationMs ?? 0);
    return {
      service,
      completed,
      failed: Number(raw.failed ?? 0),
      replayed: Number(raw.replayed ?? 0),
      totalSteps: Number(raw.totalSteps ?? 0),
      avgDurationMs: completed > 0 ? totalDurationMs / completed : 0,
      pending: 0,  // TODO: compute from stream length
      active: 0,   // TODO: compute from PEL
    };
  }
}
```

### Custom Error Types (`src/errors.ts`)

```typescript
export class DickyError extends Error {
  constructor(message: string, public readonly code: string, public override readonly cause?: unknown) {
    super(message);
    this.name = 'DickyError';
  }
}

export class SideEffectError extends DickyError {
  constructor(public readonly stepName: string, cause: unknown) {
    const msg = cause instanceof Error ? cause.message : String(cause);
    super(`Side effect "${stepName}" failed: ${msg}`, 'SIDE_EFFECT_FAILED', cause);
    this.name = 'SideEffectError';
  }
}

export class ReplayError extends DickyError {
  constructor(public readonly stepName: string, public readonly journaledError: string) {
    super(`Replayed failure at "${stepName}": ${journaledError}`, 'REPLAY_FAILED');
    this.name = 'ReplayError';
  }
}

/**
 * NOT a real error — control flow signal.
 * Thrown by ctx.sleep() and ctx.awakeable() to suspend execution.
 * The worker catches this and ACKs the message.
 */
export class SuspendedError extends DickyError {
  constructor(
    public readonly invocationId: string,
    public readonly sequence: number,
    public readonly reason: 'sleep' | 'awakeable',
  ) {
    super(`Invocation ${invocationId} suspended at step ${sequence} (${reason})`, 'SUSPENDED');
    this.name = 'SuspendedError';
  }
}

export class LockConflictError extends DickyError {
  constructor(public readonly objectName: string, public readonly objectKey: string) {
    super(`Cannot acquire lock for ${objectName}:${objectKey}`, 'LOCK_CONFLICT');
    this.name = 'LockConflictError';
  }
}

export class CancelledError extends DickyError {
  constructor(public readonly invocationId: string) {
    super(`Invocation ${invocationId} was cancelled`, 'CANCELLED');
    this.name = 'CancelledError';
  }
}

export class TimeoutError extends DickyError {
  constructor(public readonly invocationId: string, public readonly timeoutMs: number) {
    super(`Timed out waiting for invocation ${invocationId} after ${timeoutMs}ms`, 'TIMEOUT');
    this.name = 'TimeoutError';
  }
}

export class MaxRetriesExceededError extends DickyError {
  constructor(public readonly invocationId: string, public readonly attempts: number, cause: unknown) {
    const msg = cause instanceof Error ? cause.message : String(cause);
    super(`Invocation ${invocationId} failed after ${attempts} attempts: ${msg}`, 'MAX_RETRIES_EXCEEDED', cause);
    this.name = 'MaxRetriesExceededError';
  }
}

export class AwakeableError extends DickyError {
  constructor(awakeableId: string, reason: string) {
    super(`Awakeable ${awakeableId}: ${reason}`, 'AWAKEABLE_ERROR');
    this.name = 'AwakeableError';
  }
}
```

### Utility Functions (`src/utils.ts`)

```typescript
import { nanoid } from 'nanoid';
import type { DickyConfig, ResolvedConfig } from './types.js';

// ─── ID generators ───

export function newInvocationId(): string { return `inv_${nanoid(21)}`; }
export function newWorkerId(): string { return `wkr_${nanoid(12)}`; }
export function newAwakeableId(): string { return `awk_${nanoid(16)}`; }
export function newLockToken(): string { return `ltk_${nanoid(16)}`; }

// ─── Duration parsing ───

const DURATION_RE = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/i;
const MULTIPLIERS: Record = {
  ms: 1, s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000,
};

export function parseDuration(input: string): number {
  const match = DURATION_RE.exec(input.trim());
  if (!match) throw new Error(`Invalid duration "${input}". Expected:  (ms|s|m|h|d)`);
  return Math.round(Number.parseFloat(match[1]!) * MULTIPLIERS[match[2]!.toLowerCase()]!);
}

// ─── Config resolution ───

export function resolveConfig(config: DickyConfig): ResolvedConfig {
  return {
    redis: {
      host: config.redis.host,
      port: config.redis.port,
      password: config.redis.password,
      db: config.redis.db ?? 0,
      keyPrefix: config.redis.keyPrefix ?? 'dicky:',
      tls: config.redis.tls,
    },
    worker: {
      concurrency: config.worker?.concurrency ?? 10,
      pollIntervalMs: config.worker?.pollIntervalMs ?? 100,
      lockTtlMs: config.worker?.lockTtlMs ?? 30_000,
      lockRenewMs: config.worker?.lockRenewMs ?? 10_000,
      ackTimeoutMs: config.worker?.ackTimeoutMs ?? 60_000,
      timerPollIntervalMs: config.worker?.timerPollIntervalMs ?? 1_000,
    },
    retry: {
      maxRetries: config.retry?.maxRetries ?? 3,
      initialDelayMs: config.retry?.initialDelayMs ?? 1_000,
      maxDelayMs: config.retry?.maxDelayMs ?? 30_000,
      backoffMultiplier: config.retry?.backoffMultiplier ?? 2,
    },
  };
}

// ─── Retry delay ───

export function computeRetryDelay(
  attempt: number,
  initialDelayMs: number,
  maxDelayMs: number,
  multiplier: number,
): number {
  const exponential = initialDelayMs * multiplier ** attempt;
  const capped = Math.min(exponential, maxDelayMs);
  const jitter = capped * 0.25 * (Math.random() * 2 - 1);
  return Math.round(capped + jitter);
}

// ─── Key builders ───

export function keys(prefix: string) {
  return {
    stream: (service: string) => `${prefix}stream:${service}`,
    consumerGroup: (service: string) => `${prefix}cg:${service}`,
    consumerName: (workerId: string) => `${prefix}worker:${workerId}`,
    journal: (invocationId: string) => `${prefix}journal:${invocationId}`,
    invocation: (invocationId: string) => `${prefix}invocation:${invocationId}`,
    state: (objectName: string, key: string) => `${prefix}state:${objectName}:${key}`,
    lock: (objectName: string, key: string) => `${prefix}lock:${objectName}:${key}`,
    timers: () => `${prefix}timers`,
    awakeable: (awakeableId: string) => `${prefix}awakeable:${awakeableId}`,
    dlq: (service: string) => `${prefix}dlq:${service}`,
    completion: (invocationId: string) => `${prefix}completion:${invocationId}`,
    idempotency: (key: string) => `${prefix}idempotent:${key}`,
    metrics: (service: string) => `${prefix}metrics:${service}`,
  };
}
```

---

## Lua Scripts

### `lua/journal-write.lua`

Atomic check-and-write for journal entries. Prevents double-write if two workers somehow process the same invocation.

```lua
-- KEYS[1] = journal hash key (dicky:journal:{invocationId})
-- ARGV[1] = sequence number (field name)
-- ARGV[2] = serialized JournalEntry (value)
-- Returns: 1 if written, 0 if already exists

local exists = redis.call('HEXISTS', KEYS[1], ARGV[1])
if exists == 1 then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
```

### `lua/timer-poll.lua`

Atomically pop expired timers from the sorted set.

```lua
-- KEYS[1] = timer sorted set key (dicky:timers)
-- ARGV[1] = current timestamp (unix ms)
-- ARGV[2] = max count to pop
-- Returns: array of expired timer entries (JSON strings)

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #expired == 0 then
  return {}
end
redis.call('ZREM', KEYS[1], unpack(expired))
return expired
```

### `lua/lock-acquire.lua`

Atomic lock acquisition with TTL.

```lua
-- KEYS[1] = lock key (dicky:lock:{objectName}:{key})
-- ARGV[1] = lock token
-- ARGV[2] = TTL in milliseconds
-- Returns: 1 if acquired, 0 if conflict

local result = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
if result then
  return 1
end
return 0
```

### `lua/lock-renew.lua`

Extend lock TTL only if we still own it.

```lua
-- KEYS[1] = lock key
-- ARGV[1] = expected lock token
-- ARGV[2] = new TTL in milliseconds
-- Returns: 1 if renewed, 0 if lost

local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
```

### `lua/lock-release.lua`

Release lock only if we still own it.

```lua
-- KEYS[1] = lock key
-- ARGV[1] = expected lock token
-- Returns: 1 if released, 0 if already gone or different owner

local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
```

---


### Test Cases

#### `test/journal.test.ts`

- Write a journal entry → read it back → matches
- Write same sequence twice → second write returns false (idempotent)
- Read non-existent sequence → returns null
- getAll returns entries sorted by sequence
- complete() transitions pending → completed

#### `test/context.test.ts`

- `ctx.run()` first execution → calls fn, journals result, returns it
- `ctx.run()` replay → returns journaled result WITHOUT calling fn
- `ctx.run()` replayed failure → throws ReplayError
- `ctx.run()` fn throws → journals error, re-throws
- Sequence counter increments across multiple `ctx.run()` calls
- `ctx.setState()` first execution → writes to Redis, journals
- `ctx.setState()` replay → applies journaled state, doesn't write Redis

#### `test/worker.test.ts`

- Dispatch a job → worker picks it up → handler executes → invocation completes
- Dispatch to unknown handler → logs warning, doesn't crash
- Handler throws → retry scheduled → eventually DLQ after maxRetries
- Concurrent dispatches to same service → respects concurrency limit
- Worker stop → drains active invocations

#### `test/timer.test.ts`

- `ctx.sleep('wait', '1s')` → handler suspends → timer fires after ~1s → handler resumes
- Timer poll atomically removes expired entries
- Multiple timers expire at same time → all processed

#### `test/state.test.ts`

- Object handler reads initial state → correct
- Object handler sets state → subsequent invocation reads updated state
- State is keyed: different keys have independent state

#### `test/awakeable.test.ts`

- `ctx.awakeable()` → suspends → `resolveAwakeable()` → handler resumes with value
- `ctx.awakeable()` → suspends → `rejectAwakeable()` → handler resumes with error
- Resolve non-existent awakeable → throws AwakeableError
- Resolve already-resolved awakeable → throws AwakeableError

#### `test/replay.test.ts` (crash + replay scenarios)

- Handler runs 3 steps, crashes after step 2 → restart → steps 1-2 replayed (fn NOT called), step 3 executes normally
- Handler sleeps, process restarts → timer fires → handler resumes from after sleep, all prior steps replayed
- Handler with ctx.invoke → child completes → parent resumes → prior steps replayed
- Multiple retries with partial progress → each retry replays completed steps

#### `test/integration.test.ts` (full end-to-end)

- Register service + dispatch + wait for result via `invoke()`
- Virtual object: concurrent deposits → serial execution → correct final balance
- Saga pattern: multi-step workflow with compensation on failure
- `send()` with delay → job doesn't execute until delay elapses
- Idempotency key → duplicate dispatches produce only one invocation
- DLQ: list entries, retry a DLQ entry
- Cancel a pending invocation
- getInvocation / getJournal introspection

---

## Implementation Order

Build and test in this order. Each layer builds on the previous.

1. **Scaffolding**: package.json, tsconfig, biome, tsup, vitest config
2. **Types + Errors + Utils**: `types.ts`, `errors.ts`, `utils.ts` (duration parsing, ID gen, key builders)
3. **Redis + Lua**: `redis.ts` (connection factory, Lua script loader), all `.lua` files
4. **Journal**: `journal.ts` + `test/journal.test.ts`
5. **Timer**: `timer.ts` + `test/timer.test.ts`
6. **Lock**: `lock.ts` (test as part of state/integration tests)
7. **State**: `state.ts` + `test/state.test.ts`
8. **Stream Producer**: `stream.ts`
9. **Context**: `context.ts` + `test/context.test.ts`
10. **Worker**: `worker.ts` + `test/worker.test.ts`
11. **Awakeable**: `awakeable.ts` + `test/awakeable.test.ts`
12. **DLQ**: `dlq.ts`
13. **Metrics**: `metrics.ts`
14. **Define factories**: `define.ts` (service/object factory functions)
15. **Dicky class**: `dicky.ts` (composes everything, .use() method, dispatch, lifecycle)
16. **Public exports**: `index.ts`
17. **Replay tests**: `test/replay.test.ts`
18. **Integration tests**: `test/integration.test.ts`
19. **Examples**: all example files
