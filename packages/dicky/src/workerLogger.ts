export interface Logger {
  debug?(data: unknown, message?: string): void;
  info?(data: unknown, message?: string): void;
  warn?(data: unknown, message?: string): void;
  error(data: unknown, message?: string): void;
}
