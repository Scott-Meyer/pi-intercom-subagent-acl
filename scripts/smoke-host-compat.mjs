import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "..");
// Keep actual Unix socket paths within macOS's short sockaddr_un limit.
const scratch = mkdtempSync(join(tmpdir(), "pl-host-"));
const packed = join(scratch, "packed");
const agentDirs = [];

const hosts = [
  {
    label: "upstream",
    spec: "@mariozechner/pi-coding-agent@0.73.1",
    packagePath: "@mariozechner/pi-coding-agent",
    forbiddenPackagePaths: ["@earendil-works/pi-coding-agent", "@earendil-works/pi-tui"],
  },
  {
    label: "fork-minimum",
    spec: "@earendil-works/pi-coding-agent@0.80.3",
    packagePath: "@earendil-works/pi-coding-agent",
    forbiddenPackagePaths: ["@mariozechner/pi-coding-agent", "@mariozechner/pi-tui"],
    // Pin matching sibling packages so caret ranges do not mix SDK families.
    overrides: {
      "@earendil-works/pi-agent-core": "0.80.3",
      "@earendil-works/pi-ai": "0.80.3",
      "@earendil-works/pi-tui": "0.80.3",
    },
  },
  {
    label: "fork-current",
    spec: "@earendil-works/pi-coding-agent@0.85.1",
    packagePath: "@earendil-works/pi-coding-agent",
    forbiddenPackagePaths: ["@mariozechner/pi-coding-agent", "@mariozechner/pi-tui"],
  },
];

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: "utf8",
    maxBuffer: 20 * 1024 * 1024,
    ...options,
  });
  if (result.status !== 0) {
    throw new Error([
      `${command} ${args.join(" ")} failed with ${result.status}`,
      result.stdout,
      result.stderr,
    ].filter(Boolean).join("\n"));
  }
  return result;
}

try {
  mkdirSync(packed, { recursive: true });
  const packResult = run("npm", ["pack", "--silent", "--pack-destination", packed], { cwd: repo });
  const tarballName = packResult.stdout.trim().split("\n").at(-1);
  if (!tarballName) throw new Error("npm pack did not report a tarball");
  const tarball = join(packed, tarballName);

  const hostlessProject = join(scratch, "hostless");
  mkdirSync(hostlessProject, { recursive: true });
  writeFileSync(join(hostlessProject, "package.json"), JSON.stringify({ private: true }));
  run("npm", ["install", "--silent", tarball], { cwd: hostlessProject });
  assert.equal(existsSync(join(hostlessProject, "node_modules", "tsx")), true);
  assert.equal(existsSync(join(hostlessProject, "node_modules", "fs-native-extensions")), true);
  const hostlessExtension = join(hostlessProject, "node_modules", "pi-parley");
  for (const runtimeFile of ["broker/process-lock.ts", "broker/runtime-claim.ts", "broker/build.ts", "broker/broker.ts"]) {
    assert.equal(existsSync(join(hostlessExtension, runtimeFile)), true, `packed package omitted ${runtimeFile}`);
  }
  for (const hostModule of [
    "@mariozechner/pi-coding-agent",
    "@mariozechner/pi-tui",
    "@earendil-works/pi-coding-agent",
    "@earendil-works/pi-tui",
    "typebox",
  ]) {
    assert.equal(
      existsSync(join(hostlessProject, "node_modules", hostModule)),
      false,
      `hostless install pulled in optional host module ${hostModule}`,
    );
  }

  const hostlessAgent = join(hostlessProject, "agent");
  agentDirs.push(hostlessAgent);
  const coldBrokerCheck = `
    import assert from 'node:assert/strict';
    import { spawn } from 'node:child_process';
    import { once } from 'node:events';
    import { randomUUID } from 'node:crypto';
    import { ParleyClient } from ${JSON.stringify(pathToFileURL(join(hostlessExtension, "broker/client.ts")).href)};
    const broker = spawn(process.execPath, ['--import', 'tsx', ${JSON.stringify(join(hostlessExtension, "broker/broker.ts"))}], {
      env: process.env, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true,
    });
    const exited = once(broker, 'exit');
    let stdout = '', stderr = '';
    broker.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-4000); });
    const client = new ParleyClient();
    try {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => finish(new Error('Cold broker startup timed out: ' + stderr)), 10000);
        function finish(error) {
          clearTimeout(timer); broker.stdout.off('data', onData); broker.off('exit', onExit);
          error ? reject(error) : resolve();
        }
        function onData(chunk) { stdout += chunk; if (stdout.includes('Parley broker started')) finish(); }
        function onExit(code) { finish(new Error('Cold broker exited ' + code + ': ' + stderr)); }
        broker.stdout.on('data', onData); broker.once('exit', onExit);
      });
      const id = randomUUID();
      await client.connect({ cwd: process.cwd(), model: 'cold-smoke', pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, id);
      assert.deepEqual((await client.listSessions()).map(seat => seat.id), [id]);
      assert.equal((await client.reportCompactionCompleted()).generation, 1);
      await client.disconnect();
      const [code, signal] = await exited;
      assert.equal(signal, null); assert.equal(code, 0);
    } finally {
      await client.disconnect();
      if (broker.exitCode === null && broker.signalCode === null) broker.kill('SIGKILL');
      await exited;
    }
  `;
  run(process.execPath, ["--import", "tsx", "--input-type=module", "-e", coldBrokerCheck], {
    cwd: hostlessProject, timeout: 30000,
    env: { ...process.env, PI_CODING_AGENT_DIR: hostlessAgent },
  });
  console.log("✓ hostless packed install: broker started, registered a client, committed state and shut down cleanly");

  for (const host of hosts) {
    const project = join(scratch, host.label);
    const agentDir = join(project, "agent");
    agentDirs.push(agentDir);
    mkdirSync(agentDir, { recursive: true });
    writeFileSync(join(project, "package.json"), JSON.stringify({
      private: true,
      ...(host.overrides ? { overrides: host.overrides } : {}),
    }));
    run("npm", ["install", "--silent", host.spec, tarball], { cwd: project });

    const hostPackage = join(project, "node_modules", host.packagePath);
    if (!existsSync(hostPackage)) throw new Error(`${host.label}: expected host package is missing`);
    for (const forbiddenPackagePath of host.forbiddenPackagePaths) {
      if (existsSync(join(project, "node_modules", forbiddenPackagePath))) {
        throw new Error(`${host.label}: installing pi-parley pulled in ${forbiddenPackagePath}`);
      }
    }

    const hostManifest = JSON.parse(readFileSync(join(hostPackage, "package.json"), "utf8"));
    const extensionRoot = join(project, "node_modules", "pi-parley");
    const extensionManifest = JSON.parse(readFileSync(join(extensionRoot, "package.json"), "utf8"));
    assert.deepEqual(extensionManifest.dependencies, { "fs-native-extensions": "^1.5.1", tsx: "^4.23.13" });
    const extensionPath = join(extensionRoot, "index.ts");
    // Publish the fixture broker before RPC stdin can end the host. Otherwise its
    // asynchronous auto-spawn can finish after cleanup's PID census and recreate
    // a directory while it is being removed. This also exercises packed spawning.
    run(process.execPath, ["--import", "tsx", "--input-type=module", "-e", `
      import { spawnBrokerIfNeeded } from ${JSON.stringify(pathToFileURL(join(extensionRoot, "broker/spawn.ts")).href)};
      await spawnBrokerIfNeeded('npx', ['--no-install', 'tsx']);
    `], { cwd: project, timeout: 15000, env: { ...process.env, PI_CODING_AGENT_DIR: agentDir } });
    const input = [
      { id: "commands", type: "get_commands" },
      { id: "alias", type: "prompt", message: `/alias ${host.label}-compatible` },
      { id: "state", type: "get_state" },
    ].map((command) => JSON.stringify(command)).join("\n") + "\n";
    const pi = run(
      process.execPath,
      [
        join(hostPackage, hostManifest.bin.pi),
        "--mode", "rpc",
        "--no-session",
        "--offline",
        "--provider", "openai",
        "--model", "gpt-4o-mini",
        "--api-key", "host-compat-smoke-only",
        "--no-extensions",
        "-e", extensionPath,
      ],
      {
        cwd: project,
        input,
        env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, PI_OFFLINE: "1" },
      },
    );

    const records = pi.stdout.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
    const commands = records.find((record) => record.type === "response" && record.id === "commands");
    const alias = records.find((record) => record.type === "response" && record.id === "alias");
    const state = records.find((record) => record.type === "response" && record.id === "state");
    const nameEvent = records.find(
      (record) => record.type === "session_info_changed" && record.name === `${host.label}-compatible`,
    );
    const availableCommands = commands?.data?.commands ?? commands?.data ?? [];
    if (!commands?.success || !availableCommands.some((command) => command.name === "parley")) {
      throw new Error(`${host.label}: pi-parley commands were not loaded`);
    }
    if (!alias?.success || !nameEvent || !state?.success || state.data?.sessionName !== `${host.label}-compatible`) {
      throw new Error(`${host.label}: extension command or host session-name event failed`);
    }
    console.log(`✓ ${host.label} ${hostManifest.version}: extension loaded without the other Pi distribution`);
  }
} finally {
  const brokerPids = agentDirs.flatMap((agentDir) => {
    const pidPath = join(agentDir, "parley", "broker.pid");
    if (!existsSync(pidPath)) return [];
    const pid = Number.parseInt(readFileSync(pidPath, "utf8").trim(), 10);
    return Number.isInteger(pid) && pid > 0 ? [pid] : [];
  });
  for (const pid of brokerPids) {
    try { process.kill(pid, "SIGTERM"); } catch { /* already stopped */ }
  }
  for (const pid of brokerPids) {
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      try { process.kill(pid, 0); } catch { break; }
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 25);
    }
    try {
      process.kill(pid, 0);
      process.kill(pid, "SIGKILL");
    } catch { /* stopped after SIGTERM */ }
  }
  // The default tsx launcher is the broker process's detached parent. Give it
  // time to observe the child exit before deleting its cwd and returning.
  if (brokerPids.length > 0) {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
  }
  if (process.env.PI_PARLEY_KEEP_SMOKE_TMP === "1") {
    console.log(`Kept smoke workspace: ${scratch}`);
  } else {
    rmSync(scratch, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  }
}
