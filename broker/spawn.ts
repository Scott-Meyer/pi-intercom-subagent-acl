import { spawn } from "child_process";
import { existsSync, writeFileSync } from "fs";
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
  PARLEY_RUNTIME_FILE_MODE,
  restrictParleyRuntimeFile,
  type BrokerConnectTarget,
} from "./paths.ts";

import { tryAcquireProcessLock } from "./process-lock.ts";
import { isBrokerHealthOkMessage } from "./protocol.ts";
import { BROKER_RUNTIME_OCCUPIED_EXIT_CODE } from "./runtime-claim.ts";

const PARLEY_DIR = getParleyDirPath();
const EXTENSION_DIR = join(dirname(fileURLToPath(import.meta.url)), "..");
const BROKER_STARTUP_LOCK_DIR = join(PARLEY_DIR, "broker.startup");
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
  // nested installation fallback.
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

/** Reuse a wire-compatible broker or start one for the current runtime.
 * Competing clients wait for the spawn winner's health handshake. */
export async function spawnBrokerIfNeeded(brokerCommand: string, brokerArgs: string[]): Promise<void> {
  ensureParleyRuntimeDir(PARLEY_DIR);
  if (await isBrokerRunning()) return;

  const startup = tryAcquireProcessLock(BROKER_STARTUP_LOCK_DIR);
  if (startup.status === "occupied") {
    await waitForBroker();
    return;
  }

  try {
    // Another client may have completed startup before we acquired the lock.
    if (await isBrokerRunning()) return;

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
        if (signal === null && ((launch.kind === "windows-launcher" && code === 0) || code === BROKER_RUNTIME_OCCUPIED_EXIT_CODE)) {
          // A direct starter may own the runtime before health is published, or
          // a broker may outlive its spawning client. Keep waiting for that owner.
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
    startup.lease.release();
  }
}

function isBrokerRunning(): Promise<boolean> {
  return checkSocketConnectable();
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
