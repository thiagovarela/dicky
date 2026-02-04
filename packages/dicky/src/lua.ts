import { readdir, readFile } from "node:fs/promises";
import { extname, join, basename } from "node:path";
import type { RedisClient } from "./stores/redis";

export interface LuaScripts {
  load(redis: RedisClient): Promise<void>;
  eval(redis: RedisClient, script: string, keys: string[], args: string[]): Promise<unknown>;
}

export class LuaScriptsImpl implements LuaScripts {
  private shas = new Map<string, string>();
  private scriptDir: string;

  constructor(scriptDir: string = join(process.cwd(), "src", "lua")) {
    this.scriptDir = scriptDir;
  }

  list(): string[] {
    return [...this.shas.keys()];
  }

  getSha(script: string): string | undefined {
    return this.shas.get(script);
  }

  async load(redis: RedisClient): Promise<void> {
    const files = await readdir(this.scriptDir);
    const luaFiles = files.filter((file) => extname(file) === ".lua");

    for (const file of luaFiles) {
      const scriptName = basename(file, ".lua");
      const scriptPath = join(this.scriptDir, file);
      const content = await readFile(scriptPath, "utf8");
      const sha = await redis.scriptLoad(content);
      this.shas.set(scriptName, sha);
    }
  }

  async eval(redis: RedisClient, script: string, keys: string[], args: string[]): Promise<unknown> {
    const sha = this.shas.get(script);
    if (!sha) {
      throw new Error(`Script "${script}" not loaded`);
    }

    try {
      return await redis.evalsha(sha, String(keys.length), ...keys, ...args);
    } catch (error) {
      if (error instanceof Error && error.message.includes("NOSCRIPT")) {
        await this.load(redis);
        return this.eval(redis, script, keys, args);
      }
      throw error;
    }
  }
}
