import type { RedisClient } from "./redis.js";

export interface LuaScripts {
  eval(redis: RedisClient, script: string, keys: string[], args: string[]): Promise<unknown>;
}

export class LuaScriptRegistry implements LuaScripts {
  private shas = new Map<string, string>();

  register(name: string, sha: string): void {
    this.shas.set(name, sha);
  }

  async load(redis: RedisClient, scripts: Record<string, string>): Promise<void> {
    for (const [name, script] of Object.entries(scripts)) {
      const sha = await redis.scriptLoad(script);
      this.register(name, sha);
    }
  }

  async eval(redis: RedisClient, script: string, keys: string[], args: string[]): Promise<unknown> {
    const sha = this.shas.get(script);
    if (!sha) {
      throw new Error(`Script "${script}" not loaded`);
    }
    return redis.evalsha(sha, String(keys.length), ...keys, ...args);
  }
}
