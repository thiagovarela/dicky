import { keys } from "../utils.js";
import type { RedisClient } from "./redis.js";

export interface StateStore {
  get(objectName: string, key: string): Promise<string | null>;
  set(objectName: string, key: string, value: string): Promise<void>;
}

export class StateStoreImpl implements StateStore {
  constructor(
    private redis: RedisClient,
    private prefix: string,
  ) {}

  async get(objectName: string, key: string): Promise<string | null> {
    const stateKey = keys(this.prefix).state(objectName, key);
    return this.redis.get(stateKey);
  }

  async set(objectName: string, key: string, value: string): Promise<void> {
    const stateKey = keys(this.prefix).state(objectName, key);
    await this.redis.set(stateKey, value);
  }
}
