import { describe, it, expect } from "bun:test";
import {
  AwakeableError,
  DQError,
  LockConflictError,
  ReplayError,
  SuspendedError,
  TimeoutError,
  deserializeError,
  serializeError,
} from "../errors.js";

describe("errors", () => {
  it("includes context in messages", () => {
    const suspended = new SuspendedError("inv_123", 2, "sleep");
    const replay = new ReplayError("step-1", "boom");
    const conflict = new LockConflictError("cart", "key-1");
    const awakeable = new AwakeableError("awake_1", "missing");
    const timeout = new TimeoutError("inv_456", "handler", 5000);
    const dq = new DQError("E_TEST", "test message");

    expect(suspended.message).toContain("inv_123");
    expect(replay.message).toContain("step-1");
    expect(conflict.message).toContain("cart");
    expect(awakeable.message).toContain("awake_1");
    expect(timeout.message).toContain("5000");
    expect(dq.message).toContain("test message");
  });

  it("supports instanceof checks", () => {
    const error = new LockConflictError("order", "key");
    expect(error).toBeInstanceOf(LockConflictError);
    expect(error).toBeInstanceOf(Error);
  });

  it("serializes and deserializes", () => {
    const original = new TimeoutError("inv_789", "process", 2500);
    const serialized = serializeError(original);
    const parsed = JSON.parse(JSON.stringify(serialized));
    const roundtrip = deserializeError(parsed);

    expect(roundtrip).toBeInstanceOf(TimeoutError);
    expect((roundtrip as TimeoutError).timeoutMs).toBe(2500);
    expect((roundtrip as TimeoutError).invocationId).toBe("inv_789");
  });

  it("serializes with toJSON", () => {
    const original = new SuspendedError("inv_abc", 3, "awakeable");
    const parsed = JSON.parse(JSON.stringify(original));
    const roundtrip = deserializeError(parsed);

    expect(roundtrip).toBeInstanceOf(SuspendedError);
    expect((roundtrip as SuspendedError).sequence).toBe(3);
  });
});
