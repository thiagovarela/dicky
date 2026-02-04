import { afterAll, beforeAll, describe, it } from "bun:test";
import { readdirSync } from "node:fs";
import { basename } from "node:path";
import type { Dicky, DickyConfig } from "@dicky/dicky";
import { withCleanup } from "../src/setup";
import { startRedis, stopRedis } from "../../../test/redis";

type ExampleModule = {
  dicky: Dicky;
  config: DickyConfig;
  run: () => Promise<void>;
};

const examplesDir = new URL("../src", import.meta.url);
const files = readdirSync(examplesDir).filter(
  (file) => file.endsWith(".ts") && file !== "setup.ts",
);

describe("examples", () => {
  beforeAll(async () => {
    await startRedis();
  });

  afterAll(async () => {
    await stopRedis();
  });

  for (const file of files) {
    const name = basename(file, ".ts");
    it(name, async () => {
      const moduleUrl = new URL(`../src/${file}`, import.meta.url).href;
      const mod = (await import(moduleUrl)) as ExampleModule;
      if (!mod.run || !mod.dicky) {
        throw new Error(`Example ${file} must export run() and dicky`);
      }
      await withCleanup(mod.dicky, mod.run);
    });
  }
});
