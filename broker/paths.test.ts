import test from "node:test";
import assert from "node:assert/strict";
import { chmodSync, closeSync, existsSync, mkdirSync, mkdtempSync, openSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ensureParleyRuntimeDir,
  migrateLegacyRuntimeDir,
  getAgentDirPath,
  getBrokerConnectTarget,
  getBrokerListenTarget,
  getBrokerPortFilePath,
  getBrokerSocketPath,
  getParleyDirPath,
  PARLEY_DIR_MODE,
  PARLEY_RUNTIME_FILE_MODE,
  PARLEY_TCP_HOST,
  restrictParleyRuntimeFile,
  shouldUseWindowsTcpTransport,
} from "./paths.ts";

test("getAgentDirPath defaults to the pi agent directory under home", () => {
  assert.equal(getAgentDirPath({}, "/home/rcroh"), join("/home/rcroh", ".pi/agent"));
});

test("getAgentDirPath honors PI_CODING_AGENT_DIR", () => {
  assert.equal(getAgentDirPath({ PI_CODING_AGENT_DIR: "/tmp/pi-agent" }, "/home/rcroh"), "/tmp/pi-agent");
});

test("getAgentDirPath resolves relative PI_CODING_AGENT_DIR values from the caller cwd", () => {
  const cwd = join(tmpdir(), "workspace", "project");
  assert.equal(
    getAgentDirPath({ PI_CODING_AGENT_DIR: "relative-agent" }, "/home/rcroh", cwd),
    join(cwd, "relative-agent"),
  );
});

test("getParleyDirPath points at the parley runtime directory under the agent dir", () => {
  assert.equal(getParleyDirPath("/tmp/pi-agent"), join("/tmp/pi-agent", "parley"));
});

test("getBrokerSocketPath uses named pipe on Windows", () => {
  const pipePath = getBrokerSocketPath("win32", "C:/Users/rcroh/.pi/agent");
  assert.match(pipePath, /^\\\\\.\\pipe\\pi-parley-/);
  assert.doesNotMatch(pipePath, /broker\.sock$/);
});

test("getBrokerSocketPath uses broker.sock under PI_CODING_AGENT_DIR on non-Windows", () => {
  const socketPath = getBrokerSocketPath("linux", "/tmp/pi-agent");
  assert.equal(socketPath, join("/tmp/pi-agent", "parley", "broker.sock"));
});

test("Windows TCP transport is opt-in", () => {
  assert.equal(shouldUseWindowsTcpTransport("win32", {}), false);
  assert.equal(shouldUseWindowsTcpTransport("win32", { PI_PARLEY_TRANSPORT: "tcp" }), true);
  assert.equal(shouldUseWindowsTcpTransport("win32", { PI_PARLEY_TCP: "1" }), true);
  assert.equal(shouldUseWindowsTcpTransport("linux", { PI_PARLEY_TRANSPORT: "tcp" }), false);
});

test("getBrokerListenTarget uses dynamic localhost TCP only when opted in on Windows", () => {
  assert.deepEqual(getBrokerListenTarget("win32", { PI_PARLEY_TRANSPORT: "tcp" }), {
    transport: "tcp",
    host: PARLEY_TCP_HOST,
    port: 0,
  });
  assert.equal(getBrokerListenTarget("win32", {}), getBrokerSocketPath("win32", getAgentDirPath({})));
});

test("getBrokerConnectTarget reads opt-in Windows TCP endpoint from parley state", () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-paths-"));
  const parleyDir = join(root, "parley");

  try {
    ensureParleyRuntimeDir(parleyDir, "win32");
    writeFileSync(getBrokerPortFilePath(parleyDir), JSON.stringify({
      transport: "tcp",
      host: "127.0.0.1",
      port: 41234,
      stateId: "state-1",
    }));
    assert.deepEqual(getBrokerConnectTarget("win32", { PI_PARLEY_TRANSPORT: "tcp" }, parleyDir), {
      transport: "tcp",
      host: "127.0.0.1",
      port: 41234,
      stateId: "state-1",
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("getBrokerConnectTarget rejects non-local TCP endpoint hosts", () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-paths-"));
  const parleyDir = join(root, "parley");

  try {
    ensureParleyRuntimeDir(parleyDir, "win32");
    writeFileSync(getBrokerPortFilePath(parleyDir), JSON.stringify({
      transport: "tcp",
      host: "10.0.0.5",
      port: 41234,
      stateId: "state-1",
    }));
    assert.throws(
      () => getBrokerConnectTarget("win32", { PI_PARLEY_TRANSPORT: "tcp" }, parleyDir),
      /Invalid parley TCP endpoint/,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("ensureParleyRuntimeDir creates and repairs restrictive Unix directory permissions", { skip: process.platform === "win32" }, () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-paths-"));
  const parleyDir = join(root, "parley");

  try {
    ensureParleyRuntimeDir(parleyDir, "linux");
    assert.equal(statSync(parleyDir).mode & 0o777, PARLEY_DIR_MODE);

    chmodSync(parleyDir, 0o755);
    ensureParleyRuntimeDir(parleyDir, "linux");
    assert.equal(statSync(parleyDir).mode & 0o777, PARLEY_DIR_MODE);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("restrictParleyRuntimeFile applies restrictive Unix file permissions", { skip: process.platform === "win32" }, () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-paths-"));
  const filePath = join(root, "broker.pid");

  try {
    writeFileSync(filePath, "123", { mode: 0o644 });
    restrictParleyRuntimeFile(filePath, "linux");
    assert.equal(statSync(filePath).mode & 0o777, PARLEY_RUNTIME_FILE_MODE);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("runtime permission helpers skip chmod on Windows paths", () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-paths-"));
  const filePath = join(root, "broker.pid");

  try {
    ensureParleyRuntimeDir(root, "win32");
    writeFileSync(filePath, "123");
    assert.doesNotThrow(() => restrictParleyRuntimeFile(filePath, "win32"));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

function legacyRuntimeFixture(setup: (legacyDir: string) => void): string {
  const root = mkdtempSync(join(tmpdir(), "parley-migration-"));
  const agentDir = join(root, "agent");
  const legacyDir = join(agentDir, "intercom");
  ensureParleyRuntimeDir(legacyDir);
  setup(legacyDir);
  return agentDir;
}

test("migrateLegacyRuntimeDir moves a stopped legacy runtime whole", () => {
  const agentDir = legacyRuntimeFixture((legacyDir) => {
    writeFileSync(join(legacyDir, "config.json"), JSON.stringify({ enabled: true }));
    writeFileSync(join(legacyDir, "broker.pid"), "999999");
  });
  try {
    const result = migrateLegacyRuntimeDir(agentDir);
    assert.deepEqual(result, { status: "migrated" });
    const target = getParleyDirPath(agentDir);
    assert.equal(existsSync(join(target, "config.json")), true);
    assert.equal(existsSync(join(agentDir, "intercom")), false);
  } finally {
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("migrateLegacyRuntimeDir blocks while the legacy broker is live", () => {
  const agentDir = legacyRuntimeFixture((legacyDir) => {
    writeFileSync(join(legacyDir, "config.json"), JSON.stringify({ enabled: true }));
    writeFileSync(join(legacyDir, "broker.pid"), String(process.pid));
  });
  try {
    const result = migrateLegacyRuntimeDir(agentDir);
    assert.deepEqual(result, { status: "blocked", liveLegacyBroker: true });
    assert.equal(existsSync(join(agentDir, "intercom", "config.json")), true);
    assert.equal(existsSync(getParleyDirPath(agentDir)), false);
  } finally {
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("migrateLegacyRuntimeDir reports dual state as an explicit conflict", () => {
  const agentDir = legacyRuntimeFixture((legacyDir) => {
    writeFileSync(join(legacyDir, "config.json"), JSON.stringify({ enabled: true }));
  });
  const target = getParleyDirPath(agentDir);
  ensureParleyRuntimeDir(target);
  writeFileSync(join(target, "config.json"), JSON.stringify({ enabled: true }));
  try {
    const result = migrateLegacyRuntimeDir(agentDir);
    assert.equal(result.status, "conflict");
    assert.equal(existsSync(join(agentDir, "intercom", "config.json")), true);
    assert.equal(existsSync(join(target, "config.json")), true);
  } finally {
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("migrateLegacyRuntimeDir adopts legacy state into a fresh target dir", () => {
  const agentDir = legacyRuntimeFixture((legacyDir) => {
    writeFileSync(join(legacyDir, "config.json"), JSON.stringify({ enabled: true }));
    writeFileSync(join(legacyDir, "broker.pid"), "999999");
  });
  const target = getParleyDirPath(agentDir);
  ensureParleyRuntimeDir(target);
  try {
    const result = migrateLegacyRuntimeDir(agentDir);
    assert.deepEqual(result, { status: "migrated" });
    assert.equal(existsSync(join(target, "config.json")), true);
    assert.equal(existsSync(join(agentDir, "intercom")), false);
  } finally {
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("migrateLegacyRuntimeDir removes a stopped process-file husk", () => {
  const agentDir = legacyRuntimeFixture((legacyDir) => {
    writeFileSync(join(legacyDir, "broker.pid"), "999999");
  });
  try {
    const result = migrateLegacyRuntimeDir(agentDir);
    assert.deepEqual(result, { status: "migrated" });
    assert.equal(existsSync(join(agentDir, "intercom")), false);
  } finally {
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("migrateLegacyRuntimeDir blocks on a live legacy spawn lock with no broker pid", () => {
  const agentDir = legacyRuntimeFixture((legacyDir) => {
    writeFileSync(join(legacyDir, "config.json"), JSON.stringify({ enabled: true }));
    writeFileSync(join(legacyDir, "broker.spawn.lock"), `${process.pid}\n${Date.now()}\n`);
  });
  try {
    const result = migrateLegacyRuntimeDir(agentDir);
    assert.deepEqual(result, { status: "blocked", liveLegacyBroker: true });
    assert.equal(existsSync(join(agentDir, "intercom", "config.json")), true);
    assert.equal(existsSync(getParleyDirPath(agentDir)), false);
  } finally {
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("migrateLegacyRuntimeDir preserves a target spawn lock owned by the spawning client", () => {
  const agentDir = legacyRuntimeFixture((legacyDir) => {
    writeFileSync(join(legacyDir, "config.json"), JSON.stringify({ enabled: true }));
  });
  const target = getParleyDirPath(agentDir);
  ensureParleyRuntimeDir(target);
  // The spawning client holds the parley-side spawn lock while migration runs.
  writeFileSync(join(target, "broker.spawn.lock"), `${process.pid}\n${Date.now()}\n`, { flag: "wx" });
  try {
    const result = migrateLegacyRuntimeDir(agentDir);
    assert.deepEqual(result, { status: "migrated" });
    assert.equal(existsSync(join(target, "broker.spawn.lock")), true);
    assert.equal(existsSync(join(target, "config.json")), true);
  } finally {
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("migrateLegacyRuntimeDir treats a freshly created empty legacy lock as a live starter", () => {
  const agentDir = legacyRuntimeFixture((legacyDir) => {
    writeFileSync(join(legacyDir, "config.json"), JSON.stringify({ enabled: true }));
    // Create-before-write window: the lock exists with no readable content.
    const fd = openSync(join(legacyDir, "broker.spawn.lock"), "wx");
    closeSync(fd);
  });
  try {
    const result = migrateLegacyRuntimeDir(agentDir);
    assert.deepEqual(result, { status: "blocked", liveLegacyBroker: true });
    assert.equal(existsSync(join(agentDir, "intercom", "config.json")), true);
  } finally {
    rmSync(agentDir, { recursive: true, force: true });
  }
});
