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
import { dicky } from 'dicky'

const userService = dicky.service('users')
  .method('sendWelcomeEmail', async (ctx, { userId }) => {
    await ctx.sideEffect(async () => {
      await emailProvider.send(userId, 'Welcome!')
    })
    return { success: true }
  })
  .method('processOrder', async (ctx, { orderId }) => {
    await ctx.sleep(60_000) // Durable timer
    await ctx.call('inventory.reserve', { orderId })
    return { processed: true }
  })

dicky.use(userService)

dicky.listen(3000)
```

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
export const userService = dicky.service('users')
  .method('getProfile', async (ctx, { id }) => {
    return db.user.find(id)
  })
  .method('updateProfile', async (ctx, args) => {
    return db.user.update(args.id, args.data)
  })

// index.ts
import { userService } from './services/users'
import { orderService } from './services/orders'

dicky.use(userService)
dicky.use(orderService)

// Full autocomplete available here:
dicky.services.       // → users, orders
dicky.services.users. // → getProfile, updateProfile
```

## Installation

```bash
npm install dicky
```

## Requirements

- Node.js 20+
- Redis 7.0+ (with Streams support)

## Documentation

- [Getting Started](docs/getting-started.md)
- [Architecture](docs/architecture.md)
- [API Reference](docs/api.md)
- [Examples](examples/)

## License

MIT
