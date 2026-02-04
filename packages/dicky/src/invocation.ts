import type { Invocation } from "./types";

export function parseInvocationRecord(
  record: Record<string, string>,
  defaults: { service: string; args: string },
): Invocation {
  const argsRaw = record.args ?? defaults.args ?? "null";
  const parsedResult = parseJson(record.result);

  return {
    id: record.id ?? "",
    service: record.service ?? defaults.service,
    handler: record.handler ?? "",
    args: safeJsonParse(argsRaw, null),
    status: (record.status as Invocation["status"]) ?? "pending",
    attempt: Number.parseInt(record.attempt ?? "0", 10),
    maxRetries: Number.parseInt(record.maxRetries ?? "0", 10),
    createdAt: Number.parseInt(record.createdAt ?? "0", 10),
    updatedAt: Number.parseInt(record.updatedAt ?? "0", 10),
    ...(record.key ? { key: record.key } : {}),
    ...(record.completedAt ? { completedAt: Number.parseInt(record.completedAt, 10) } : {}),
    ...(parsedResult !== undefined ? { result: parsedResult } : {}),
    ...(record.error ? { error: record.error } : {}),
    ...(record.parentId ? { parentId: record.parentId } : {}),
    ...(record.parentStep ? { parentStep: Number.parseInt(record.parentStep, 10) } : {}),
  };
}

function safeJsonParse<T>(value: string, fallback: T): T {
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function parseJson(value?: string): unknown {
  if (value == null) {
    return undefined;
  }
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}
