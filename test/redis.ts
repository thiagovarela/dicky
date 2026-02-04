import { connect } from "node:net";

const defaultRedisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";

export async function startRedis(redisUrl: string = defaultRedisUrl): Promise<void> {
  if (!process.env.REDIS_URL) {
    const available = isDockerAvailable();
    if (!available) {
      throw new Error("Docker is required for Redis-backed tests. Set REDIS_URL or start Docker.");
    }

    const { exitCode } = Bun.spawnSync({
      cmd: ["docker", "compose", "-f", "docker-compose.yml", "ps", "-q", "redis"],
      stdout: "pipe",
      stderr: "pipe",
    });

    if (exitCode === 0) {
      Bun.spawnSync({
        cmd: ["docker", "compose", "-f", "docker-compose.yml", "up", "-d", "redis"],
        stdout: "pipe",
        stderr: "pipe",
      });
    }
  }

  await waitForRedis(redisUrl);
}

export async function stopRedis(): Promise<void> {
  if (process.env.REDIS_URL) {
    return;
  }

  Bun.spawnSync({
    cmd: ["docker", "compose", "-f", "docker-compose.yml", "down", "--remove-orphans"],
    stdout: "pipe",
    stderr: "pipe",
  });
}

function isDockerAvailable(): boolean {
  try {
    const result = Bun.spawnSync({
      cmd: ["docker", "info"],
      stdout: "pipe",
      stderr: "pipe",
    });
    return result.exitCode === 0;
  } catch {
    return false;
  }
}

async function waitForRedis(redisUrl: string): Promise<void> {
  const { host, port } = parseRedisUrl(redisUrl);
  const timeoutAt = Date.now() + 10_000;

  while (Date.now() < timeoutAt) {
    const ok = await pingRedis(host, port);
    if (ok) {
      return;
    }
    await delay(250);
  }

  throw new Error("Redis did not start in time");
}

function parseRedisUrl(redisUrl: string): { host: string; port: number } {
  try {
    const url = new URL(redisUrl);
    return {
      host: url.hostname || "localhost",
      port: url.port ? Number.parseInt(url.port, 10) : 6379,
    };
  } catch {
    return { host: "localhost", port: 6379 };
  }
}

async function pingRedis(host: string, port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = connect({ host, port });
    let settled = false;

    const cleanup = () => {
      if (settled) {
        return;
      }
      settled = true;
      socket.removeAllListeners();
      socket.destroy();
    };

    socket.setTimeout(1000);
    socket.on("error", () => {
      cleanup();
      resolve(false);
    });
    socket.on("timeout", () => {
      cleanup();
      resolve(false);
    });
    socket.on("connect", () => {
      socket.write("*1\r\n$4\r\nPING\r\n");
    });
    socket.on("data", (data) => {
      const response = data.toString();
      cleanup();
      resolve(response.includes("PONG"));
    });
  });
}

async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
