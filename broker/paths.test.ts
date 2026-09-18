import test from "node:test";
import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ensureParleyRuntimeDir,
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
  shouldUseTcpTransport,
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

test("TCP transport is an explicit runtime choice, independent of platform", () => {
  assert.equal(shouldUseTcpTransport({}), false);
  assert.equal(shouldUseTcpTransport({ PI_PARLEY_TRANSPORT: "tcp" }), true);
  assert.equal(shouldUseTcpTransport({ PI_PARLEY_TCP: "1" }), true);
  for (const platform of ["win32", "darwin", "linux"] as const) {
    assert.deepEqual(getBrokerListenTarget(platform, { PI_PARLEY_TRANSPORT: "tcp" }), {
      transport: "tcp", host: PARLEY_TCP_HOST, port: 0,
    });
    assert.equal(getBrokerListenTarget(platform, {}), getBrokerSocketPath(platform, getAgentDirPath({})));
  }
});

test("getBrokerConnectTarget reads the opted-in TCP endpoint from parley state", () => {
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
