import { describe, it, expect } from "bun:test";
import {
  keys,
  newAwakeableId,
  newInvocationId,
  newLockToken,
  newWorkerId,
  parseDuration,
} from "../utils.js";

describe("parseDuration", () => {
  it("parses supported durations", () => {
    expect(parseDuration("500ms")).toBe(500);
    expect(parseDuration("5s")).toBe(5_000);
    expect(parseDuration("30m")).toBe(30 * 60 * 1000);
    expect(parseDuration("2h")).toBe(2 * 60 * 60 * 1000);
    expect(parseDuration("1d")).toBe(24 * 60 * 60 * 1000);
  });

  it("throws on invalid duration", () => {
    expect(() => parseDuration("5"))
      .toThrow("Invalid duration format: 5");
  });
});

describe("keys", () => {
  it("builds prefixed keys", () => {
    const prefixed = keys("dq:");

    expect(prefixed.stream("orders")).toBe("dq:stream:orders");
    expect(prefixed.consumerGroup("orders")).toBe("dq:cg:orders");
    expect(prefixed.consumerName("worker1")).toBe("dq:worker:worker1");
    expect(prefixed.journal("inv1")).toBe("dq:journal:inv1");
    expect(prefixed.invocation("inv1")).toBe("dq:invocation:inv1");
    expect(prefixed.state("cart", "key")).toBe("dq:state:cart:key");
    expect(prefixed.lock("cart", "key")).toBe("dq:lock:cart:key");
    expect(prefixed.timers()).toBe("dq:timers");
    expect(prefixed.awakeable("awake1")).toBe("dq:awakeable:awake1");
    expect(prefixed.dlq("orders")).toBe("dq:dlq:orders");
    expect(prefixed.completion("inv1")).toBe("dq:completion:inv1");
    expect(prefixed.idempotency("key1")).toBe("dq:idempotent:key1");
    expect(prefixed.metrics("orders")).toBe("dq:metrics:orders");
  });
});

describe("id generation", () => {
  it("creates correctly formatted ids", () => {
    expect(newInvocationId()).toMatch(/^inv_[A-Za-z0-9_-]{21}$/);
    expect(newAwakeableId()).toMatch(/^awake_[A-Za-z0-9_-]{21}$/);
    expect(newWorkerId()).toMatch(/^worker-[A-Za-z0-9_-]{21}$/);
    expect(newLockToken()).toMatch(/^[A-Za-z0-9_-]{21}$/);
  });
});
