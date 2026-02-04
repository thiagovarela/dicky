# @dicky/examples

Runnable examples for the Dicky durable execution engine.

## Usage

Run an individual example:

```bash
bun run --filter @dicky/examples src/01-basic-service.ts
```

List available examples:

```bash
bun run --filter @dicky/examples list
```

Run all examples as tests:

```bash
bun run --filter @dicky/examples test
```

## Examples

- `01-basic-service` - minimal service and invoke()
- `02-side-effects` - ctx.run() idempotent side effects
- `04-virtual-objects` - stateful keyed entities
- `06-awakeables` - external event completion
- `08-saga-pattern` - compensating transactions
