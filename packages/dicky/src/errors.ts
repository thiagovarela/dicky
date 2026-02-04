import type { InvocationId } from "./types.js";

export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
  [key: string]: unknown;
}

function finalizeError<T extends Error>(
  error: T,
  name: string,
  ctor: Function,
): T {
  error.name = name;
  Object.setPrototypeOf(error, ctor.prototype);
  if (Error.captureStackTrace) {
    Error.captureStackTrace(error, ctor);
  }
  return error;
}

/**
 * Serialize an error for transport.
 */
export function serializeError(error: Error): SerializedError {
  const serialized: SerializedError = {
    name: error.name,
    message: error.message,
  };

  if (error.stack) {
    serialized.stack = error.stack;
  }

  for (const key of Object.getOwnPropertyNames(error)) {
    if (key === "name" || key === "message" || key === "stack") {
      continue;
    }
    serialized[key] = (error as Record<string, unknown>)[key];
  }

  return serialized;
}

/**
 * Deserialize an error previously serialized with {@link serializeError}.
 */
export function deserializeError(serialized: SerializedError): Error {
  const applyStack = (error: Error) => {
    if (serialized.stack) {
      error.stack = serialized.stack;
    }
    return error;
  };

  switch (serialized.name) {
    case "SuspendedError": {
      const error = new SuspendedError(
        serialized.invocationId as InvocationId,
        serialized.sequence as number,
        serialized.type as "sleep" | "awakeable" | "invoke",
      );
      return applyStack(error);
    }
    case "ReplayError": {
      const error = new ReplayError(
        serialized.step as string,
        serialized.originalError as string | undefined,
      );
      return applyStack(error);
    }
    case "LockConflictError": {
      const error = new LockConflictError(
        serialized.objectName as string,
        serialized.key as string,
      );
      return applyStack(error);
    }
    case "AwakeableError": {
      const error = new AwakeableError(
        serialized.awakeableId as string,
        serialized.reason as string,
      );
      return applyStack(error);
    }
    case "TimeoutError": {
      const error = new TimeoutError(
        serialized.invocationId as InvocationId,
        serialized.handler as string,
        serialized.timeoutMs as number,
      );
      return applyStack(error);
    }
    case "DQError": {
      const error = new DQError(
        serialized.code as string,
        serialized.message as string,
      );
      return applyStack(error);
    }
    default: {
      const error = new Error(serialized.message);
      error.name = serialized.name;
      for (const [key, value] of Object.entries(serialized)) {
        if (key === "name" || key === "message" || key === "stack") {
          continue;
        }
        (error as Record<string, unknown>)[key] = value;
      }
      return applyStack(error);
    }
  }
}

export class SuspendedError extends Error {
  constructor(
    readonly invocationId: InvocationId,
    readonly sequence: number,
    readonly type: "sleep" | "awakeable" | "invoke",
  ) {
    super(`Invocation ${invocationId} suspended at sequence ${sequence} (${type})`);
    finalizeError(this, "SuspendedError", new.target);
  }

  toJSON(): SerializedError {
    return serializeError(this);
  }
}

export class ReplayError extends Error {
  constructor(
    readonly step: string,
    readonly originalError?: string,
  ) {
    super(`Replay error at step "${step}": ${originalError || "unknown"}`);
    finalizeError(this, "ReplayError", new.target);
  }

  toJSON(): SerializedError {
    return serializeError(this);
  }
}

export class LockConflictError extends Error {
  constructor(
    readonly objectName: string,
    readonly key: string,
  ) {
    super(`Lock conflict for object "${objectName}" with key "${key}"`);
    finalizeError(this, "LockConflictError", new.target);
  }

  toJSON(): SerializedError {
    return serializeError(this);
  }
}

export class AwakeableError extends Error {
  constructor(
    readonly awakeableId: string,
    readonly reason: string,
  ) {
    super(`Awakeable error for ${awakeableId}: ${reason}`);
    finalizeError(this, "AwakeableError", new.target);
  }

  toJSON(): SerializedError {
    return serializeError(this);
  }
}

export class TimeoutError extends Error {
  constructor(
    readonly invocationId: InvocationId,
    readonly handler: string,
    readonly timeoutMs: number,
  ) {
    super(`Invocation ${invocationId} timed out after ${timeoutMs}ms waiting for ${handler}`);
    finalizeError(this, "TimeoutError", new.target);
  }

  toJSON(): SerializedError {
    return serializeError(this);
  }
}

export class DQError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    finalizeError(this, "DQError", new.target);
  }

  toJSON(): SerializedError {
    return serializeError(this);
  }
}
