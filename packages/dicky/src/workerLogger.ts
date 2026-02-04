export interface Logger {
  debug?(data: unknown, message?: string): void;
  info?(data: unknown, message?: string): void;
  warn?(data: unknown, message?: string): void;
  error(data: unknown, message?: string): void;
}

export function createLogger(): Logger {
  return {
    error: (data, message) => console.error(message ?? "", data),
    warn: (data, message) => console.warn(message ?? "", data),
    info: (data, message) => console.info(message ?? "", data),
    debug: (data, message) => console.debug(message ?? "", data),
  };
}
