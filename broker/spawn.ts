import { spawn } from "child_process";
import { existsSync, readFileSync, statSync, unlinkSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { createRequire } from "module";
import net from "net";
import { randomUUID } from "crypto";
import { createMessageReader, writeMessage } from "./framing.ts";
import {
  ensureParleyRuntimeDir,
  getAgentDirPath,
  getBrokerConnectTarget,
  getParleyDirPath,
  migrateLegacyRuntimeDir,
  PARLEY_PROTOCOL_NAME,
  PARLEY_PROTOCOL_VERSION,
  PARLEY_RUNTIME_FILE_MODE,
  restrictParleyRuntimeFile,
  type BrokerConnectTarget,
} from "./paths.ts";

const PARLEY_DIR = getParleyDirPath();
const EXTENSION_DIR = join(dirname(fileURLToPath(import.meta.url)), "..");
const BROKER_PID = join(PARLEY_DIR, "broker.pid");
const BROKER_SPAWN_LOCK = join(PARLEY_DIR, "broker.spawn.lock");
const BROKER_STARTUP_STDERR_LIMIT = 4_000;

type BrokerLaunchSpec =
  | {
    kind: "direct";
    command: string;
    args: string[];
    captureStartupStderr: boolean;
  }
  | {
    kind: "windows-launcher";
    command: string;
    args: string[];
    launcherPath: string;
    launcherCommandLine: string;
    captureStartupStderr: boolean;
  };

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function getTsxCliPath(extensionDir: string = EXTENSION_DIR): string {
  // Resolve tsx via Node's module resolution so it works regardless of whether
  // tsx is bundled under extensionDir/node_modules or hoisted to a workspace
  // root by npm. We resolve the tsx package main entry (its "exports" field
  // does not expose ./dist/cli.mjs as a subpath) and then locate cli.mjs next
  // to it. If resolution fails, prefer the flat plugin-store layout before the
  // legacy nested fallback.
  try {
    const requireFromExtension = createRequire(join(extensionDir, "package.json"));
    const tsxMain = requireFromExtension.resolve("tsx");
    return join(dirname(tsxMain), "cli.mjs");
  } catch {
    const siblingTsxCli = join(extensionDir, "..", "tsx", "dist", "cli.mjs");
    if (existsSync(siblingTsxCli)) {
      return siblingTsxCli;
    }
    return join(extensionDir, "node_modules", "tsx", "dist", "cli.mjs");
  }
}

function quoteWindowsArg(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

export function getWindowsHiddenLauncherPath(parleyDir: string = PARLEY_DIR): string {
  return join(parleyDir, "broker-launch.vbs");
}

function usesDefaultBrokerCommand(brokerCommand: string, brokerArgs: string[]): boolean {
  return brokerCommand === "npx"
    && brokerArgs.length === 2
    && brokerArgs[0] === "--no-install"
    && brokerArgs[1] === "tsx";
}

function getNodeCommand(nodePath: string): string {
  const executableName = nodePath.split(/[\\/]/).pop();
  return executableName && /^node(?:js)?(?:\.exe)?$/i.test(executableName)
    ? nodePath
    : "node";
}

export function getWindowsBrokerCommandLine(
  brokerPath: string,
  extensionDir: string = EXTENSION_DIR,
  nodePath: string = process.execPath,
  brokerCommand = "npx",
  brokerArgs: string[] = ["--no-install", "tsx"],
): string {
  if (usesDefaultBrokerCommand(brokerCommand, brokerArgs)) {
    return [quoteWindowsArg(getNodeCommand(nodePath)), quoteWindowsArg(getTsxCliPath(extensionDir)), quoteWindowsArg(brokerPath)].join(" ");
  }

  return [quoteWindowsArg(brokerCommand), ...brokerArgs.map(quoteWindowsArg), quoteWindowsArg(brokerPath)].join(" ");
}

export function getWindowsHiddenLauncherScript(commandLine: string): string {
  return [
    'Set WshShell = CreateObject("WScript.Shell")',
    `WshShell.Run "${commandLine.replace(/"/g, '""')}", 0, False`,
    'Set WshShell = Nothing',
    '',
  ].join("\r\n");
}

export function isBrokerHealthOkMessage(message: unknown, requestId: string): boolean {
  if (typeof message !== "object" || message === null || !("type" in message)) {
    return false;
  }
  const response = message as Record<string, unknown>;
  return response.type === "health_ok"
    && response.requestId === requestId
    && response.protocol === PARLEY_PROTOCOL_NAME
    && response.version === PARLEY_PROTOCOL_VERSION;
}

export function writeWindowsHiddenLauncher(
  commandLine: string,
  launcherPath: string = getWindowsHiddenLauncherPath(),
): string {
  ensureParleyRuntimeDir(dirname(launcherPath));
  writeFileSync(launcherPath, `\uFEFF${getWindowsHiddenLauncherScript(commandLine)}`, {
    encoding: "utf16le",
    mode: PARLEY_RUNTIME_FILE_MODE,
  });
  restrictParleyRuntimeFile(launcherPath);
  return launcherPath;
}

export function getBrokerLaunchSpec(
  brokerPath: string,
  brokerCommand: string,
  brokerArgs: string[],
  extensionDir: string = EXTENSION_DIR,
  platform: NodeJS.Platform = process.platform,
  parleyDir: string = PARLEY_DIR,
  nodePath: string = process.execPath,
): BrokerLaunchSpec {
  if (platform === "win32") {
    const launcherPath = getWindowsHiddenLauncherPath(parleyDir);
    return {
      kind: "windows-launcher",
      command: "wscript.exe",
      args: ["//E:VBScript", launcherPath],
      launcherPath,
      launcherCommandLine: getWindowsBrokerCommandLine(brokerPath, extensionDir, nodePath, brokerCommand, brokerArgs),
      captureStartupStderr: false,
    };
  }

  if (usesDefaultBrokerCommand(brokerCommand, brokerArgs)) {
    return {
      kind: "direct",
      command: getNodeCommand(nodePath),
      args: [getTsxCliPath(extensionDir), brokerPath],
      captureStartupStderr: true,
    };
  }

  return {
    kind: "direct",
    command: brokerCommand,
    args: [...brokerArgs, brokerPath],
    captureStartupStderr: false,
  };
}

export function getBrokerSpawnOptions(
  extensionDir: string = EXTENSION_DIR,
  env: NodeJS.ProcessEnv = process.env,
  captureStderr = true,
): {
  detached: true;
  stdio: "ignore" | ["ignore", "ignore", "pipe"];
  cwd: string;
  env: NodeJS.ProcessEnv;
  windowsHide: true;
} {
  return {
    detached: true,
    stdio: captureStderr ? ["ignore", "ignore", "pipe"] : "ignore",
    cwd: extensionDir,
    env: { ...env, PI_CODING_AGENT_DIR: getAgentDirPath(env), NODE_NO_WARNINGS: "1" },
    windowsHide: true,
  };
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

// Parley 1.1.0 relocated the runtime dir; spawn refuses while a legacy
// intercom broker is still running so the roster never splits. The client's
// reconnect loop retries after the legacy broker idles out.
// Parley 1.1.0 relocated the runtime dir; spawn refuses while a legacy
// intercom broker is still running so the roster never splits. The client's
// reconnect loop retries after the legacy broker idles out.
function assertRuntimeCutoverComplete(): void {
  const migration = migrateLegacyRuntimeDir();
  if (migration.status === "blocked") {
    throw new Error(
      "parley: a legacy intercom broker is still running from the intercom/ runtime dir. Restart Pi sessions so it drains; if a federation peer link (e.g. a FlightDeck bridge) holds it open, disconnect the link or stop the drained broker — its state migrates on the next start.",
    );
  }
  if (migration.status === "conflict") {
    throw new Error(
      `parley: both ${join(getAgentDirPath(), "intercom")} and ${getParleyDirPath()} hold runtime state; resolve manually (remove or merge one) before starting.`,
    );
  }
}

export async function spawnBrokerIfNeeded(brokerCommand: string, brokerArgs: string[], depth = 0): Promise<void> {
  // The spawn lock opens parley/broker.spawn.lock, so the directory must
  // exist first. A fresh parley/ dir with no state entries lets the legacy
  // migration adopt intercom/ state wholesale.
  ensureParleyRuntimeDir(PARLEY_DIR);

  const ownsLock = acquireSpawnLock();
  if (!ownsLock) {
    // A concurrent start owns the spawn lock. Wait for its broker, then
    // re-enter so the attach decision is made under the same lock — a
    // lock-free status probe cannot close the legacy pid/lock handoff
    // race, and attaching to a healthy broker beside an unresolved legacy
    // runtime would split the roster.
    await waitForBroker();
    if (depth >= 1) {
      throw new Error("parley: broker startup lock is still held after the broker became healthy; retrying shortly.");
    }
    return spawnBrokerIfNeeded(brokerCommand, brokerArgs, depth + 1);
  }

  try {
    // Fully serialized under the spawn lock: a healthy parley broker is not
    // enough to attach — while a legacy intercom runtime is unresolved (live
    // broker, in-flight legacy startup, or dual state), attaching would split
    // the roster and stall the cutover, so that broker is left to drain. The
    // lock also serializes the one-time legacy migration across concurrent
    // first starts.
    assertRuntimeCutoverComplete();
    if (await isBrokerRunning()) {
      return;
    }

    const brokerPath = join(dirname(fileURLToPath(import.meta.url)), "broker.ts");
    const launch = getBrokerLaunchSpec(brokerPath, brokerCommand, brokerArgs);
    if (launch.kind === "windows-launcher") {
      writeWindowsHiddenLauncher(launch.launcherCommandLine, launch.launcherPath);
    }
    const child = spawn(launch.command, launch.args, getBrokerSpawnOptions(EXTENSION_DIR, process.env, launch.captureStartupStderr));
    let brokerStderr = "";
    const rememberBrokerStderr = (chunk: Buffer | string) => {
      brokerStderr = `${brokerStderr}${chunk.toString()}`.slice(-BROKER_STARTUP_STDERR_LIMIT);
    };
    const brokerStartupError = (message: string, cause?: unknown) => {
      const stderr = brokerStderr.trim();
      const errorMessage = stderr ? `${message}\nBroker stderr:\n${stderr}` : message;
      return cause === undefined ? new Error(errorMessage) : new Error(errorMessage, { cause });
    };
    child.stderr?.on("data", rememberBrokerStderr);
    (child.stderr as (NodeJS.ReadableStream & { unref?: () => void }) | null)?.unref?.();
    child.unref();

    await new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        child.stderr?.off("data", rememberBrokerStderr);
        child.stderr?.resume();
        child.off("error", onError);
        child.off("close", onExit);
      };

      const onError = (error: Error) => {
        cleanup();
        reject(brokerStartupError(`Failed to spawn parley broker: ${error.message}`, error));
      };

      const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
        if (launch.kind === "windows-launcher" && code === 0 && signal === null) {
          return;
        }
        cleanup();
        if (signal) {
          reject(brokerStartupError(`Parley broker exited before startup with signal ${signal}`));
          return;
        }
        reject(brokerStartupError(`Parley broker exited before startup with code ${code ?? "unknown"}`));
      };

      child.once("error", onError);
      child.once("close", onExit);
      waitForBroker().then(() => {
        cleanup();
        resolve();
      }, (error) => {
        cleanup();
        const startupError = toError(error);
        reject(brokerStartupError(startupError.message, startupError));
      });
    });
  } finally {
    releaseSpawnLock();
  }
}

async function isBrokerRunning(): Promise<boolean> {
  if (await checkSocketConnectable()) {
    return true;
  }

  if (!existsSync(BROKER_PID)) return false;

  try {
    const pid = parseInt(readFileSync(BROKER_PID, "utf-8").trim(), 10);
    if (!Number.isFinite(pid)) return false;
    process.kill(pid, 0);
    return checkSocketConnectable();
  } catch {
    // Missing or unreadable PID state means there is no live broker to reuse.
    return false;
  }
}

function connectToBrokerTarget(target: BrokerConnectTarget): net.Socket {
  return typeof target === "string"
    ? net.connect(target)
    : net.connect({ host: target.host, port: target.port });
}

function checkSocketConnectable(): Promise<boolean> {
  return new Promise((resolve) => {
    let target: BrokerConnectTarget;
    try {
      target = getBrokerConnectTarget();
    } catch {
      resolve(false);
      return;
    }

    const socket = connectToBrokerTarget(target);
    const requestId = randomUUID();
    const expectedStateId = typeof target === "string" ? undefined : target.stateId;
    let settled = false;
    const finish = (isConnected: boolean) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      socket.off("connect", onConnect);
      socket.off("error", onError);
      socket.off("data", reader);
      socket.destroy();
      resolve(isConnected);
    };
    const onConnect = () => {
      try {
        writeMessage(socket, {
          type: "health",
          requestId,
          ...(expectedStateId ? { stateId: expectedStateId } : {}),
        });
      } catch {
        finish(false);
      }
    };
    const onError = () => finish(false);
    const reader = createMessageReader((message) => {
      finish(isBrokerHealthOkMessage(message, requestId));
    }, () => finish(false));
    socket.on("connect", onConnect);
    socket.on("error", onError);
    socket.on("data", reader);
    const timeout = setTimeout(() => finish(false), 1000);
  });
}

function acquireSpawnLock(): boolean {
  const maxRetries = 5;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      writeFileSync(BROKER_SPAWN_LOCK, `${process.pid}\n${Date.now()}\n`, {
        flag: "wx",
        mode: PARLEY_RUNTIME_FILE_MODE,
      });
      restrictParleyRuntimeFile(BROKER_SPAWN_LOCK);
      return true;
    } catch (error) {
      if (!(error instanceof Error) || (error as NodeJS.ErrnoException).code !== "EEXIST") {
        throw error;
      }
      if (isSpawnLockStale()) {
        try {
          unlinkSync(BROKER_SPAWN_LOCK);
        } catch {
          // If we can't delete the stale lock, retry a few times before giving up
        }
        continue;
      }
      return false;
    }
  }
  return false;
}

function isSpawnLockStale(): boolean {
  if (!existsSync(BROKER_SPAWN_LOCK)) {
    return false;
  }

  try {
    const [pidLine = "", createdAtLine = "0"] = readFileSync(BROKER_SPAWN_LOCK, "utf-8").trim().split("\n");
    const pid = Number.parseInt(pidLine, 10);
    const createdAt = Number.parseInt(createdAtLine, 10);

    if (!Number.isFinite(pid) || !Number.isFinite(createdAt)) {
      // Empty or partial content: the holder may be between the lock's
      // exclusive create and its write. Steal only a lock that has also
      // been sitting untouched; a freshly created one is treated as held.
      return Date.now() - statSync(BROKER_SPAWN_LOCK).mtimeMs > 10_000;
    }

    if (Number.isFinite(pid)) {
      try {
        process.kill(pid, 0);
      } catch {
        // The process that created the lock is gone.
        return true;
      }
    }

    return Date.now() - createdAt > 10_000;
  } catch {
    // Unreadable or empty content: the holder may be between the lock's
    // exclusive create and its write. Steal only a lock that has also been
    // sitting untouched; a freshly created one is treated as held.
    try {
      return Date.now() - statSync(BROKER_SPAWN_LOCK).mtimeMs > 10_000;
    } catch {
      return false;
    }
  }
}

function releaseSpawnLock(): void {
  try {
    unlinkSync(BROKER_SPAWN_LOCK);
  } catch {
    // Another cleanup path may already have removed the lock.
  }
}

async function waitForBroker(timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await checkSocketConnectable()) {
      return;
    }
    await sleep(100);
  }
  throw new Error("Broker failed to start within timeout");
}
