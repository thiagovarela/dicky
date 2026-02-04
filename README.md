# Dicky - Durable Execution Engine for TypeScript

Embeddable durable execution runtime backed by Redis Streams.

## Overview

Dicky provides **Restate-style durable execution** without requiring an HTTP sidecar or separate process. It runs directly in your Node.js event loop, enabling functions to execute for hours or days with guaranteed progress.

## Key Features

- **Journaled Side Effects**: Every side effect is recorded and deduplicated on replay
- **Durable Timers**: Time-based checkpoints that survive process restarts
- **Virtual Objects**: Stateful services with transparent durability
- **Type-Safe Dispatch**: Elysia-style `.use()` composition for full autocomplete
- **No External Dependencies**: Runs entirely within your Node.js application

## Why Dicky?

Traditional durable execution systems require complex infrastructure:
- External sidecar processes
- HTTP/gRPC interfaces
- Dedicated control planes

Dicky embeds directly into your existing Node.js application:

```
Your Node.js App
├── Event Loop
│   ├── Dicky Runtime
│   │   ├── Stream Consumer
│   │   ├── Timer Manager
│   │   └── Reclaim Loop
│   └── Your Application Code
└── Redis
    └── Streams (Journal, Timers, State)
```

## Quick Example

```typescript
import { Dicky, service } from "@dicky/dicky";
import { z } from "zod";

const dicky = new Dicky({
  redis: { host: "localhost", port: 6379 },
});

const userService = service("users", {
  sendWelcomeEmail: {
    input: z.object({ userId: z.string() }),
    output: z.object({ success: z.boolean() }),
    handler: async (ctx, { userId }) => {
      await ctx.run("send-email", async () => {
        await emailProvider.send(userId, "Welcome!");
      });
      return { success: true };
    },
  },
  processOrder: {
    input: z.object({ orderId: z.string() }),
    output: z.object({ processed: z.boolean() }),
    handler: async (ctx, { orderId }) => {
      await ctx.sleep("wait", "60s"); // Durable timer
      await ctx.invoke("inventory", "reserve", { orderId });
      return { processed: true };
    },
  },
});

dicky.use(userService);
await dicky.start();
```

Handlers can optionally provide Zod input/output schemas. Dicky validates invocation arguments and handler results before persisting completion, and uses the schemas to infer handler types.

## Durable Execution Guarantees

### Recovery Scenario

```
Timeline:
────────────────────────────────────────────────────────────►
│
├─ Step 1: Execute      [✓] ─────────────────────────────────┤
├─ Step 2: Execute       [✓] ─────────────────────────────────┤
├─ Step 3: Execute        [✓] ─────────────────┐               │
├─ Step 4: Execute         [✗] Crash          │               │
│                                                 Recovery    │
├─ Step 4: Replay        [→] Resumed from journal             │
├─ Step 5: Execute                                 [✓] ────────┤
│
```

### What Survives?

- ✅ Side effects (idempotent operations are replayed safely)
- ✅ Timer checkpoints (persisted across restarts)
- ✅ State mutations (virtual object state)
- ❌ In-memory heap (fresh on restart)

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────┐
│                    Dicky Runtime                        │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │   Consumer  │  │   Timer     │  │   Reclaim   │    │
│  │   Loop      │  │   Manager   │  │   Loop      │    │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘    │
│         │                │                │           │
│         ▼                ▼                ▼           │
│  ┌─────────────────────────────────────────────────┐   │
│  │              Redis Streams                       │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────────┐ │   │
│  │  │ Journal │  │ Timers  │  │ Checkpoint Store│ │   │
│  │  └─────────┘  └─────────┘  └─────────────────┘ │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Type-Safe Composition

Define services in separate files with full type inference:

```typescript
// services/users.ts
import { service } from "@dicky/dicky";
import { z } from "zod";

export const userService = service("users", {
  getProfile: {
    input: z.object({ id: z.string() }),
    output: z.object({ id: z.string() }),
    handler: async (_ctx, { id }) => db.user.find(id),
  },
  updateProfile: {
    input: z.object({ id: z.string(), data: z.any() }),
    output: z.object({ id: z.string() }).passthrough(),
    handler: async (_ctx, args) => db.user.update(args.id, args.data),
  },
});

// index.ts
import { Dicky } from "@dicky/dicky";
import { userService } from "./services/users";
import { orderService } from "./services/orders";

const dicky = new Dicky({ redis: { host: "localhost", port: 6379 } })
  .use(userService)
  .use(orderService);

await dicky.start();

// Typed invocations:
await dicky.invoke("users", "getProfile", { id: "user-1" });
```

## Installation

```bash
npm install dicky
```

## Requirements

- Node.js 20+
- Redis 7.0+ (with Streams support)

## Redis Client Adapters

Dicky can create adapter-backed clients from a URL. Set `redis.adapter` to
`"bun"` or `"ioredis"` and optionally tune the connection pool size.

```ts
import { Dicky } from "@dicky/dicky";

const dicky = new Dicky({
  redis: {
    url: "redis://localhost:6379",
    adapter: "bun", // or "ioredis"
    pool: { size: 3 },
  },
});
```

You can still supply your own client via `redis.client` when needed:

```ts
import Redis from "ioredis";
import { Dicky } from "@dicky/dicky";
import { ioredisAdapter } from "@dicky/dicky/adapters/ioredis";

const raw = new Redis("redis://localhost:6379");
const dicky = new Dicky({
  redis: {
    client: ioredisAdapter(raw),
  },
});
```

## Documentation

- [Getting Started](docs/getting-started.md)
- [Architecture](docs/architecture.md)
- [API Reference](docs/api.md)
- [Examples](examples/)

## License

MIT
