import test from "node:test";
import { createMessageReader, writeMessage } from "./framing.ts";
import { PARLEY_PROTOCOL_NAME, PARLEY_PROTOCOL_VERSION } from "./paths.ts";
import net from "node:net";
import assert from "node:assert/strict";
import path from "node:path";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  getBrokerLaunchSpec,
  getBrokerSpawnOptions,
  getTsxCliPath,
  getWindowsHiddenLauncherScript,
  getWindowsBrokerCommandLine,
  getWindowsHiddenLauncherPath,
  isBrokerHealthOkMessage,
  writeWindowsHiddenLauncher,
} from "./spawn.ts";

test("getTsxCliPath resolves tsx cli via module resolution", () => {
  const cliPath = getTsxCliPath();
  // getTsxCliPath resolves the tsx package main entry and locates cli.mjs next
  // to it, so the path reflects the real install location (bundled under
  // extensionDir or hoisted by npm) rather than a hardcoded relative path.
  assert.equal(path.basename(cliPath), "cli.mjs");
  assert.equal(path.basename(path.dirname(cliPath)), "dist");
  assert.equal(path.basename(path.dirname(path.dirname(cliPath))), "tsx");
});

test("getTsxCliPath falls back to a flat sibling tsx install", () => {
  const storeDir = mkdtempSync(path.join(tmpdir(), "pi-parley-store-"));

  try {
    const extensionDir = path.join(storeDir, "node_modules", "pi-parley");
    const cliPath = path.join(storeDir, "node_modules", "tsx", "dist", "cli.mjs");
    mkdirSync(extensionDir, { recursive: true });
    mkdirSync(path.dirname(cliPath), { recursive: true });
    writeFileSync(cliPath, "");

    assert.equal(getTsxCliPath(extensionDir), cliPath);
  } finally {
    rmSync(storeDir, { recursive: true, force: true });
  }
});

test("getTsxCliPath keeps the nested fallback when no sibling tsx cli exists", () => {
  const storeDir = mkdtempSync(path.join(tmpdir(), "pi-parley-store-"));

  try {
    const extensionDir = path.join(storeDir, "pi-parley");
    mkdirSync(extensionDir, { recursive: true });

    assert.equal(
      getTsxCliPath(extensionDir),
      path.join(extensionDir, "node_modules", "tsx", "dist", "cli.mjs"),
    );
  } finally {
    rmSync(storeDir, { recursive: true, force: true });
  }
});

test("getWindowsHiddenLauncherPath points at the broker launcher script", () => {
  const launcherPath = getWindowsHiddenLauncherPath("C:/tmp/parley");
  assert.equal(launcherPath, path.join("C:/tmp/parley", "broker-launch.vbs"));
});

test("getWindowsBrokerCommandLine wraps node, resolved tsx cli, and broker path", () => {
  const commandLine = getWindowsBrokerCommandLine(
    "C:/repo/broker.ts",
    "C:/repo",
    "C:/Program Files/nodejs/node.exe",
  );
  const expectedTsxPath = getTsxCliPath("C:/repo");
  assert.equal(
    commandLine,
    `"C:/Program Files/nodejs/node.exe" "${expectedTsxPath}" "C:/repo/broker.ts"`,
  );
});

test("getWindowsHiddenLauncherScript runs the broker command without showing a console", () => {
  const script = getWindowsHiddenLauncherScript('"C:/Program Files/nodejs/node.exe" "C:/repo/node_modules/tsx/dist/cli.mjs" "C:/repo/broker.ts"');
  assert.match(script, /WshShell\.Run/);
  assert.match(script, /, 0, False/);
});

test("writeWindowsHiddenLauncher writes a UTF-16LE script with a BOM", () => {
  const parleyDir = mkdtempSync(path.join(tmpdir(), "pi-parley-用户-"));
  const launcherPath = path.join(parleyDir, "broker-launch.vbs");
  const commandLine = '"C:/Users/用户/node.exe" "C:/repo/用户/broker.ts"';

  try {
    writeWindowsHiddenLauncher(commandLine, launcherPath);
    const contents = readFileSync(launcherPath);
    assert.deepEqual([...contents.subarray(0, 2)], [0xff, 0xfe]);
    assert.equal(contents.toString("utf16le"), `\uFEFF${getWindowsHiddenLauncherScript(commandLine)}`);
  } finally {
    rmSync(parleyDir, { recursive: true, force: true });
  }
});

test("getBrokerLaunchSpec uses wscript launcher on Windows without writing files", () => {
  const parleyDir = mkdtempSync(path.join(tmpdir(), "pi-parley-"));

  try {
    const spec = getBrokerLaunchSpec(
      "C:/repo/broker.ts",
      "npx",
      ["--no-install", "tsx"],
      "C:/repo",
      "win32",
      parleyDir,
      "C:/Program Files/nodejs/node.exe",
    );
    assert.equal(spec.command, "wscript.exe");
    assert.deepEqual(spec.args, ["//E:VBScript", path.join(parleyDir, "broker-launch.vbs")]);
    assert.equal(spec.kind, "windows-launcher");
    const expectedTsxPath = getTsxCliPath("C:/repo");
    assert.equal(spec.launcherCommandLine, `"C:/Program Files/nodejs/node.exe" "${expectedTsxPath}" "C:/repo/broker.ts"`);
    assert.equal(spec.captureStartupStderr, false);
    assert.equal(existsSync(path.join(parleyDir, "broker-launch.vbs")), false);
  } finally {
    rmSync(parleyDir, { recursive: true, force: true });
  }
});

test("getBrokerLaunchSpec falls back to PATH node for a standalone Pi executable on Windows", () => {
  const parleyDir = mkdtempSync(path.join(tmpdir(), "pi-parley-"));

  try {
    const spec = getBrokerLaunchSpec(
      "C:/repo/broker.ts",
      "npx",
      ["--no-install", "tsx"],
      "C:/repo",
      "win32",
      parleyDir,
      "C:/Program Files/Pi/pi.exe",
    );
    assert.equal(spec.kind, "windows-launcher");
    assert.equal(spec.launcherCommandLine, `"node" "${getTsxCliPath("C:/repo")}" "C:/repo/broker.ts"`);
  } finally {
    rmSync(parleyDir, { recursive: true, force: true });
  }
});

test("getBrokerLaunchSpec uses custom broker command on Windows", () => {
  const parleyDir = mkdtempSync(path.join(tmpdir(), "pi-parley-"));

  try {
    const spec = getBrokerLaunchSpec("C:/repo/broker.ts", "bun", ["--smol"], "C:/repo", "win32", parleyDir, "C:/Program Files/nodejs/node.exe");
    assert.equal(spec.command, "wscript.exe");
    assert.deepEqual(spec.args, ["//E:VBScript", path.join(parleyDir, "broker-launch.vbs")]);
    assert.equal(spec.kind, "windows-launcher");
    assert.equal(spec.launcherCommandLine, `"bun" "--smol" "C:/repo/broker.ts"`);
  } finally {
    rmSync(parleyDir, { recursive: true, force: true });
  }
});

test("getBrokerLaunchSpec uses node + resolved tsx for the default non-Windows launch", () => {
  const spec = getBrokerLaunchSpec("C:/repo/broker.ts", "npx", ["--no-install", "tsx"], "C:/repo", "linux", "/tmp/parley", "/usr/bin/node");
  assert.equal(spec.command, "/usr/bin/node");
  assert.deepEqual(spec.args, [
    getTsxCliPath("C:/repo"),
    "C:/repo/broker.ts",
  ]);
  assert.equal(spec.kind, "direct");
  assert.equal(spec.captureStartupStderr, true);
});

test("getBrokerLaunchSpec falls back to PATH node for a standalone Pi executable on non-Windows", () => {
  const spec = getBrokerLaunchSpec(
    "/repo/broker.ts",
    "npx",
    ["--no-install", "tsx"],
    "/repo",
    "darwin",
    "/tmp/parley",
    "/Applications/Pi.app/Contents/MacOS/pi",
  );
  assert.equal(spec.command, "node");
  assert.deepEqual(spec.args, [
    getTsxCliPath("/repo"),
    "/repo/broker.ts",
  ]);
  assert.equal(spec.kind, "direct");
});

test("getBrokerLaunchSpec uses custom broker command on non-Windows", () => {
  const spec = getBrokerLaunchSpec("/repo/broker.ts", "bun", [], "/repo", "linux", "/tmp/parley", "/usr/bin/node");
  assert.equal(spec.command, "bun");
  assert.deepEqual(spec.args, ["/repo/broker.ts"]);
  assert.equal(spec.kind, "direct");
  assert.equal(spec.captureStartupStderr, false);
});

test("getBrokerSpawnOptions hides the broker console window on Windows", () => {
  const options = getBrokerSpawnOptions("C:/repo");
  assert.equal(options.windowsHide, true);
  assert.equal(options.detached, true);
  assert.deepEqual(options.stdio, ["ignore", "ignore", "pipe"]);
  assert.equal(options.cwd, "C:/repo");
});

test("getBrokerSpawnOptions keeps portable defaults on non-Windows platforms", () => {
  const options = getBrokerSpawnOptions("/repo");
  assert.equal(options.windowsHide, true);
  assert.equal(options.detached, true);
  assert.deepEqual(options.stdio, ["ignore", "ignore", "pipe"]);
  assert.equal(options.cwd, "/repo");
});

test("getBrokerSpawnOptions can keep custom broker stderr ignored", () => {
  const options = getBrokerSpawnOptions("/repo", process.env, false);
  assert.equal(options.detached, true);
  assert.equal(options.stdio, "ignore");
  assert.equal(options.cwd, "/repo");
});

test("getBrokerSpawnOptions passes an absolute PI_CODING_AGENT_DIR to the broker", () => {
  const options = getBrokerSpawnOptions("/repo", { PI_CODING_AGENT_DIR: "relative-agent" });
  assert.equal(options.env.PI_CODING_AGENT_DIR, path.resolve("relative-agent"));
});

test("spawnBrokerIfNeeded includes stderr from default broker startup failures", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "pi-parley-spawn-"));
  const extensionDir = path.join(root, "pi-parley");
  const brokerDir = path.join(extensionDir, "broker");
  const fakeTsxCli = path.join(extensionDir, "node_modules", "tsx", "dist", "cli.mjs");
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;

  try {
    mkdirSync(brokerDir, { recursive: true });
    mkdirSync(path.dirname(fakeTsxCli), { recursive: true });
    writeFileSync(path.join(extensionDir, "package.json"), JSON.stringify({ type: "module" }));
    writeFileSync(fakeTsxCli, "process.stderr.write('fake tsx failed\\n'); process.exit(1);\n");

    const sourceDir = path.dirname(fileURLToPath(import.meta.url));
    for (const fileName of ["spawn.ts", "framing.ts", "paths.ts"]) {
      cpSync(path.join(sourceDir, fileName), path.join(brokerDir, fileName));
    }
    cpSync(path.join(sourceDir, "..", "env-compat.ts"), path.join(extensionDir, "env-compat.ts"));

    process.env.PI_CODING_AGENT_DIR = path.join(root, "agent");
    const moduleUrl = `${pathToFileURL(path.join(brokerDir, "spawn.ts")).href}?case=${Date.now()}`;
    const imported = await import(moduleUrl) as typeof import("./spawn.ts");

    await assert.rejects(
      () => imported.spawnBrokerIfNeeded("npx", ["--no-install", "tsx"]),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /Parley broker exited before startup with code 1/);
        assert.match(error.message, /Broker stderr:\nfake tsx failed/);
        return true;
      },
    );
  } finally {
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    rmSync(root, { recursive: true, force: true });
  }
});

test("isBrokerHealthOkMessage requires the parley protocol marker", () => {
  assert.equal(isBrokerHealthOkMessage({ type: "health_ok", requestId: "req-1", protocol: "pi-parley", version: 1 }, "req-1"), true);
  assert.equal(isBrokerHealthOkMessage({ type: "health_ok", requestId: "req-1" }, "req-1"), false);
  assert.equal(isBrokerHealthOkMessage({ type: "health_ok", requestId: "req-2", protocol: "pi-parley", version: 1 }, "req-1"), false);
  assert.equal(isBrokerHealthOkMessage("ok", "req-1"), false);
});

async function importSpawnWithAgentDir(root: string): Promise<typeof import("./spawn.ts")> {
  const extensionDir = path.join(root, "pi-parley");
  const brokerDir = path.join(extensionDir, "broker");
  const fakeTsxCli = path.join(extensionDir, "node_modules", "tsx", "dist", "cli.mjs");
  mkdirSync(brokerDir, { recursive: true });
  mkdirSync(path.dirname(fakeTsxCli), { recursive: true });
  writeFileSync(path.join(extensionDir, "package.json"), JSON.stringify({ type: "module" }));
  writeFileSync(fakeTsxCli, "process.exit(0);\n");
  const sourceDir = path.dirname(fileURLToPath(import.meta.url));
  for (const fileName of ["spawn.ts", "framing.ts", "paths.ts"]) {
    cpSync(path.join(sourceDir, fileName), path.join(brokerDir, fileName));
  }
  cpSync(path.join(sourceDir, "..", "env-compat.ts"), path.join(extensionDir, "env-compat.ts"));
  const moduleUrl = `${pathToFileURL(path.join(brokerDir, "spawn.ts")).href}?case=${Date.now()}-${Math.random()}`;
  return await import(moduleUrl) as typeof import("./spawn.ts");
}

/** A broker stub that genuinely answers the framed health handshake, so
 * isBrokerRunning/waitForBroker see a healthy parley broker. onFirstHealth
 * runs once when the first health request arrives (e.g. to simulate the
 * spawn winner releasing its lock after startup). */
function listenHealthyParleyBroker(
  sockPath: string,
  onFirstHealth?: () => void,
): { server: import("node:net").Server; close: () => Promise<void> } {
  const sockets = new Set<import("node:net").Socket>();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    let answeredFirst = false;
    const reader = createMessageReader((message) => {
      const request = message as { type?: string; requestId?: string };
      if (request.type === "health" && typeof request.requestId === "string") {
        if (!answeredFirst) {
          answeredFirst = true;
          onFirstHealth?.();
        }
        writeMessage(socket, {
          type: "health_ok",
          requestId: request.requestId,
          protocol: PARLEY_PROTOCOL_NAME,
          version: PARLEY_PROTOCOL_VERSION,
        });
      }
    });
    socket.on("data", reader);
    socket.on("error", () => undefined);
  });
  server.listen(sockPath);
  return {
    server,
    close: () => new Promise<void>((resolve) => {
      for (const socket of sockets) socket.destroy();
      server.close(() => resolve());
      setTimeout(() => resolve(), 500);
    }),
  };
}

function writeUnresolvedLegacyRuntime(agentDir: string): void {
  const legacyDir = path.join(agentDir, "intercom");
  mkdirSync(legacyDir, { recursive: true });
  writeFileSync(path.join(legacyDir, "config.json"), JSON.stringify({ enabled: true }));
  // A live legacy broker holds the intercom runtime.
  writeFileSync(path.join(legacyDir, "broker.pid"), String(process.pid));
}

test("spawnBrokerIfNeeded refuses a healthy parley broker while a legacy intercom runtime is unresolved", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "parley-gate-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  let broker: { server: import("node:net").Server; close: () => Promise<void> } | undefined;
  try {
    const agentDir = path.join(root, "agent");
    const parleyDir = path.join(agentDir, "parley");
    mkdirSync(parleyDir, { recursive: true });
    writeUnresolvedLegacyRuntime(agentDir);
    // A healthy parley broker answers health on the target socket.
    broker = listenHealthyParleyBroker(path.join(parleyDir, "broker.sock"));
    await new Promise<void>((resolve) => broker!.server.once("listening", resolve));

    process.env.PI_CODING_AGENT_DIR = agentDir;
    const imported = await importSpawnWithAgentDir(root);

    // With an early-return-on-healthy bypass this call would succeed and
    // attach; the cutover gate must refuse instead.
    await assert.rejects(
      () => imported.spawnBrokerIfNeeded("npx", ["--no-install", "tsx"]),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /legacy intercom broker is still running/);
        return true;
      },
    );
  } finally {
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    await broker?.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("spawnBrokerIfNeeded contention path re-enters the cutover gate before attaching", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "parley-ctn-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  let broker: { server: import("node:net").Server; close: () => Promise<void> } | undefined;
  try {
    const agentDir = path.join(root, "agent");
    const parleyDir = path.join(agentDir, "parley");
    mkdirSync(parleyDir, { recursive: true });
    writeUnresolvedLegacyRuntime(agentDir);
    const lockPath = path.join(parleyDir, "broker.spawn.lock");
    // A concurrent start owns the spawn lock...
    writeFileSync(lockPath, `${process.pid}\n${Date.now()}\n`, { flag: "wx" });
    // ...and its broker is healthy. When the winner's first health answer
    // arrives it releases the lock, exactly as a real spawn winner would;
    // the contender must then re-acquire and hit the cutover gate.
    broker = listenHealthyParleyBroker(path.join(parleyDir, "broker.sock"), () => {
      try { unlinkSync(lockPath); } catch { /* already released */ }
    });
    await new Promise<void>((resolve) => broker!.server.once("listening", resolve));

    process.env.PI_CODING_AGENT_DIR = agentDir;
    const imported = await importSpawnWithAgentDir(root);

    await assert.rejects(
      () => imported.spawnBrokerIfNeeded("npx", ["--no-install", "tsx"]),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /legacy intercom broker is still running/);
        return true;
      },
    );
  } finally {
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    await broker?.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("isSpawnLockStale treats a fresh PID-only target lock as held", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "parley-lock-pidonly-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  try {
    const agentDir = path.join(root, "agent");
    process.env.PI_CODING_AGENT_DIR = agentDir;
    const imported = await importSpawnWithAgentDir(root);
    const parleyDir = path.join(agentDir, "parley");
    mkdirSync(parleyDir, { recursive: true });
    // A live owner wrote only its PID before the timestamp line.
    writeFileSync(path.join(parleyDir, "broker.spawn.lock"), `${process.pid}\n`);
    assert.equal(imported.isSpawnLockStale(), false);
  } finally {
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    rmSync(root, { recursive: true, force: true });
  }
});
