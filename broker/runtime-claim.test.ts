import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { once } from "node:events";
import { randomUUID } from "node:crypto";
import { createMessageReader, writeMessage } from "./framing.ts";
import { PARLEY_PROTOCOL_NAME, PARLEY_PROTOCOL_VERSION } from "./paths.ts";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { claimBrokerRuntime } from "./runtime-claim.ts";
import { tryAcquireProcessLock } from "./process-lock.ts";

const noEndpoint = { host: "127.0.0.1", port: 0 };

test("PID metadata cannot exclude a new runtime owner, even if the PID was reused", async () => {
  const directory = mkdtempSync(join(tmpdir(), "parley-runtime-"));
  try {
    writeFileSync(join(directory, "broker.pid"), `${process.pid}\n`);
    const lease = await claimBrokerRuntime(directory, noEndpoint);
    try { assert.equal(lease.owner.pid, process.pid); }
    finally { lease.release(); }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("runtime ownership excludes a competing starter independent of endpoint choice", async () => {
  const directory = mkdtempSync(join(tmpdir(), "parley-runtime-"));
  const lease = await claimBrokerRuntime(directory, noEndpoint);
  try {
    writeFileSync(join(directory, "collaboration-state.json"), "durable state");
    await assert.rejects(claimBrokerRuntime(directory, { host: "127.0.0.1", port: 1 }), /already owned/);
    assert.equal(readFileSync(join(directory, "collaboration-state.json"), "utf8"), "durable state");
  } finally {
    lease.release();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("an already-serving IPC endpoint is preserved and a failed startup releases its lease", async () => {
  const directory = mkdtempSync(join(tmpdir(), "parley-runtime-"));
  const endpoint = process.platform === "win32"
    ? `\\\\.\\pipe\\parley-test-${process.pid}-${Date.now()}`
    : join(directory, "broker.sock");
  const server = net.createServer((socket) => socket.destroy());
  server.listen(endpoint);
  await once(server, "listening");
  try {
    await assert.rejects(claimBrokerRuntime(directory, endpoint), /already serving/);
    const reusable = tryAcquireProcessLock(join(directory, "broker.ownership"));
    assert.equal(reusable.status, "acquired");
    if (reusable.status === "acquired") reusable.lease.release();
    const connection = net.connect(endpoint);
    await once(connection, "connect");
    connection.destroy();
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a serving published TCP endpoint is preserved; refused or damaged discovery is replaceable", async () => {
  const directory = mkdtempSync(join(tmpdir(), "parley-runtime-"));
  const stateId = randomUUID();
  const sockets = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("data", createMessageReader((value) => {
      const request = value as { type: string; requestId: string; stateId: string };
      assert.equal(request.stateId, stateId);
      writeMessage(socket, {
        type: "health_ok", requestId: request.requestId,
        protocol: PARLEY_PROTOCOL_NAME, version: PARLEY_PROTOCOL_VERSION,
      });
    }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const discovery = `${JSON.stringify({ transport: "tcp", host: "127.0.0.1", port: address.port, stateId })}\n`;
  const discoveryPath = join(directory, "broker.port.json");
  writeFileSync(discoveryPath, discovery);
  try {
    await assert.rejects(claimBrokerRuntime(directory, noEndpoint), /TCP endpoint is already serving/);
    assert.equal(readFileSync(discoveryPath, "utf8"), discovery);
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
  try {
    const replacement = await claimBrokerRuntime(directory, noEndpoint);
    replacement.release();
    writeFileSync(discoveryPath, "{interrupted publication");
    (await claimBrokerRuntime(directory, noEndpoint)).release();
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("a non-socket path is not disposable runtime state", { skip: process.platform === "win32" }, async () => {
  const directory = mkdtempSync(join(tmpdir(), "parley-runtime-"));
  const endpoint = join(directory, "broker.sock");
  try {
    writeFileSync(endpoint, "not a socket");
    await assert.rejects(claimBrokerRuntime(directory, endpoint), /non-socket/);
    assert.equal(readFileSync(endpoint, "utf8"), "not a socket");
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
