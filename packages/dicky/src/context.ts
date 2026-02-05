import { ReplayError, SuspendedError } from "./errors";
import type { DurableContext, InvocationId } from "./types";
import { newAwakeableId, parseDuration } from "./utils";
import type { AwakeableStore } from "./stores/awakeable";
import type { JournalStore } from "./stores/journal";
import type { StateStore } from "./stores/state";
import type { StreamProducer } from "./stores/stream";
import type { TimerStore } from "./stores/timer";

export class DurableContextImpl<TState = unknown> implements DurableContext<TState> {
  private sequence = 0;
  private _state: TState;

  constructor(
    private invocation: { id: InvocationId; key?: string; service: string },
    private journal: JournalStore,
    private timerStore: TimerStore,
    private stateStore: StateStore,
    private awakeableStore: AwakeableStore,
    private streamProducer: StreamProducer,
    initialState: TState,
  ) {
    this._state = initialState;
  }

  get invocationId(): InvocationId {
    return this.invocation.id;
  }

  get key(): string | undefined {
    return this.invocation.key;
  }

  get state(): TState {
    return this._state;
  }

  async run<T>(name: string, fn: () => T | Promise<T>): Promise<T> {
    const seq = this.sequence++;

    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === "completed") {
      return existing.result != null ? (JSON.parse(existing.result) as T) : (undefined as T);
    }

    if (existing?.status === "failed") {
      throw new ReplayError(name, existing.error ?? "Unknown error");
    }

    try {
      const result = await fn();
      let serialized: string | undefined;
      try {
        serialized = result !== undefined ? JSON.stringify(result) : undefined;
      } catch (error) {
        const errorMsg = error instanceof Error ? error.message : String(error);
        await this.journal.write({
          invocationId: this.invocationId,
          sequence: seq,
          type: "run",
          name,
          status: "failed",
          error: `Serialization failed: ${errorMsg}`,
          createdAt: Date.now(),
          completedAt: Date.now(),
        });
        throw new SerializationError(
          errorMsg,
          error instanceof Error ? error : new Error(errorMsg),
        );
      }

      await this.journal.write({
        invocationId: this.invocationId,
        sequence: seq,
        type: "run",
        name,
        status: "completed",
        ...(serialized !== undefined ? { result: serialized } : {}),
        createdAt: Date.now(),
        completedAt: Date.now(),
      });

      return result;
    } catch (error) {
      if (error instanceof SerializationError) {
        throw error.original;
      }
      const errorMsg = error instanceof Error ? error.message : String(error);
      await this.journal.write({
        invocationId: this.invocationId,
        sequence: seq,
        type: "run",
        name,
        status: "failed",
        error: errorMsg,
        createdAt: Date.now(),
        completedAt: Date.now(),
      });
      throw error;
    }
  }

  async sleep(name: string, duration: string): Promise<void> {
    const seq = this.sequence++;
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === "completed") {
      return;
    }

    if (existing?.status === "pending") {
      throw new SuspendedError(this.invocationId, seq, "sleep");
    }

    const durationMs = parseDuration(duration);
    const wakeAt = Date.now() + durationMs;

    await this.journal.write({
      invocationId: this.invocationId,
      sequence: seq,
      type: "sleep",
      name,
      status: "pending",
      createdAt: Date.now(),
    });

    await this.timerStore.schedule(
      { invocationId: this.invocationId, sequence: seq, type: "sleep" },
      wakeAt,
    );

    throw new SuspendedError(this.invocationId, seq, "sleep");
  }

  async invoke<T>(
    service: string,
    handler: string,
    args?: unknown,
    opts?: { key?: string },
  ): Promise<T> {
    const seq = this.sequence++;
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === "completed") {
      return existing.result != null ? (JSON.parse(existing.result) as T) : (undefined as T);
    }

    if (existing?.status === "failed") {
      throw new ReplayError(`${service}.${handler}`, existing.error ?? "Unknown error");
    }

    if (existing?.status === "pending") {
      throw new SuspendedError(this.invocationId, seq, "invoke");
    }

    await this.streamProducer.dispatch(service, handler, args, {
      parentId: this.invocationId,
      parentStep: seq,
      ...(opts?.key ? { key: opts.key } : {}),
    });

    await this.journal.write({
      invocationId: this.invocationId,
      sequence: seq,
      type: "invoke",
      name: `${service}.${handler}`,
      status: "pending",
      createdAt: Date.now(),
    });

    throw new SuspendedError(this.invocationId, seq, "invoke");
  }

  async send(
    service: string,
    handler: string,
    args?: unknown,
    opts?: { key?: string; delay?: string },
  ): Promise<void> {
    const seq = this.sequence++;
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === "completed") {
      return;
    }

    if (existing?.status === "failed") {
      throw new ReplayError(`${service}.${handler}`, existing.error ?? "Unknown error");
    }

    if (existing?.status === "pending") {
      // Send operations should never be pending since they're fire-and-forget
      // and immediately marked as completed. If we see this, something is wrong.
      throw new ReplayError(
        `${service}.${handler}`,
        "Send operation found in pending state - this should never happen",
      );
    }

    const dispatchOpts = {
      ...(opts?.key ? { key: opts.key } : {}),
      ...(opts?.delay ? { delay: opts.delay } : {}),
    };

    await this.streamProducer.dispatch(
      service,
      handler,
      args,
      Object.keys(dispatchOpts).length > 0 ? dispatchOpts : undefined,
    );

    await this.journal.write({
      invocationId: this.invocationId,
      sequence: seq,
      type: "send",
      name: `${service}.${handler}`,
      status: "completed",
      createdAt: Date.now(),
      completedAt: Date.now(),
    });
  }

  async awakeable<T>(name: string): Promise<[string, Promise<T>]> {
    const seq = this.sequence++;
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === "completed") {
      const value = existing.result != null ? JSON.parse(existing.result) : undefined;
      return [existing.name, Promise.resolve(value as T)];
    }

    if (existing?.status === "failed") {
      throw new ReplayError(name, existing.error ?? "Awakeable rejected");
    }

    if (existing?.status === "pending") {
      throw new SuspendedError(this.invocationId, seq, "awakeable");
    }

    const awakeableId = newAwakeableId();

    await this.awakeableStore.create(
      {
        invocationId: this.invocationId,
        sequence: seq,
        status: "pending",
      },
      awakeableId,
    );

    await this.journal.write({
      invocationId: this.invocationId,
      sequence: seq,
      type: "awakeable",
      name: awakeableId,
      status: "pending",
      createdAt: Date.now(),
    });

    throw new SuspendedError(this.invocationId, seq, "awakeable");
  }

  async resolveAwakeable(awakeableId: string, value?: unknown): Promise<void> {
    await this.awakeableStore.resolve(awakeableId, value);
  }

  async rejectAwakeable(awakeableId: string, error: string): Promise<void> {
    await this.awakeableStore.reject(awakeableId, error);
  }

  async setState(newState: TState): Promise<void> {
    const seq = this.sequence++;
    const existing = await this.journal.get(this.invocationId, seq);

    if (existing?.status === "completed") {
      this._state = existing.result != null ? (JSON.parse(existing.result) as TState) : newState;
      return;
    }

    if (!this.invocation.key) {
      throw new Error("Cannot set state without object key");
    }

    let serialized: string;
    try {
      serialized = JSON.stringify(newState);
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      throw new Error(`Serialization failed: ${errorMsg}`);
    }
    await this.stateStore.set(this.invocation.service, this.invocation.key, serialized);
    this._state = newState;

    await this.journal.write({
      invocationId: this.invocationId,
      sequence: seq,
      type: "state-set",
      name: "setState",
      status: "completed",
      result: serialized,
      createdAt: Date.now(),
      completedAt: Date.now(),
    });
  }
}

class SerializationError extends Error {
  constructor(
    message: string,
    readonly original: Error = new Error(message),
  ) {
    super(message);
    this.name = "SerializationError";
  }
}
