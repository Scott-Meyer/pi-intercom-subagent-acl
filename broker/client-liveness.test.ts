import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { ParleyClient } from "./client.ts";
import { createMessageReader, writeMessage } from "./framing.ts";
import { CONVERSATION_CONTRACT_FEATURE, EXACT_SEND_FEATURE, type ClientMessage } from "../types.ts";

/**
 * Unit tests for the half-open socket fix.
 *
 * Bug: when the broker dies without sending a FIN (SIGKILL, crash, host loss),
 * the client's socket stays "writable" indefinitely. isConnected() keeps
 * returning true, no "disconnected" event fires, and the extension's
 * scheduleReconnect() is never called — so the agent silently drops out of the
 * parley roster forever (this is why long-lived headless/RPC pi agents
 * vanish from `parley list` after a broker restart).
 *
 * The fix: (1) a socket "error" after registration destroys the socket so the
 * existing onClose -> "disconnected" path runs, and (2) a liveness heartbeat
 * that round-trips a lightweight request and tears down the socket on timeout
 * or write error, so a half-open connection is detected within a bounded
 * window even when the OS never delivers an error.
 */

const homeDir = mkdtempSync(path.join(tmpdir(), "pi-parley-liveness-unit-"));
const runtimeAgentDir = process.platform === "win32" ? undefined : mkdtempSync("/tmp/piic-");
const previousHome = process.env.HOME;
const previousUserProfile = process.env.USERPROFILE;
const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
const previousInterval = process.env.PI_PARLEY_LIVENESS_INTERVAL_MS;
const previousTimeout = process.env.PI_PARLEY_LIVENESS_TIMEOUT_MS;
process.env.HOME = homeDir;
process.env.USERPROFILE = homeDir;
if (runtimeAgentDir) process.env.PI_CODING_AGENT_DIR = runtimeAgentDir;
process.env.PI_PARLEY_LIVENESS_INTERVAL_MS = "100";
process.env.PI_PARLEY_LIVENESS_TIMEOUT_MS = "200";

test.after(() => {
  process.env.HOME = previousHome;
  process.env.USERPROFILE = previousUserProfile;
  if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
  else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
  if (previousInterval === undefined) delete process.env.PI_PARLEY_LIVENESS_INTERVAL_MS;
  else process.env.PI_PARLEY_LIVENESS_INTERVAL_MS = previousInterval;
  if (previousTimeout === undefined) delete process.env.PI_PARLEY_LIVENESS_TIMEOUT_MS;
  else process.env.PI_PARLEY_LIVENESS_TIMEOUT_MS = previousTimeout;
  rmSync(homeDir, { recursive: true, force: true });
  if (runtimeAgentDir) rmSync(runtimeAgentDir, { recursive: true, force: true });
});

/**
 * Build a registered ParleyClient wired to a fake server socket pair, so we
 * can simulate a half-open connection without spawning a real broker.
 */
async function registeredClientAgainstFakeSocket(): Promise<{
  client: ParleyClient;
  serverSide: net.Socket;
  closeServerSideAbruptly(): void;
  stopResponding(): void;
}> {
  const { getBrokerSocketPath } = await import("./paths.ts");
  const { mkdirSync, unlinkSync } = await import("node:fs");
  const socketPath = getBrokerSocketPath();
  const parleyDir = path.dirname(socketPath);
  mkdirSync(parleyDir, { recursive: true });
  try { unlinkSync(socketPath); } catch { /* no stale socket */ }

  const client = new ParleyClient();
  let resolveReady: (value: { serverSide: net.Socket; closeServerSideAbruptly(): void; stopResponding(): void }) => void;
  let rejectReady: (reason: unknown) => void;
  const ready = new Promise<{ serverSide: net.Socket; closeServerSideAbruptly(): void; stopResponding(): void }>((res, rej) => {
    resolveReady = res;
    rejectReady = rej;
  });

  let responding = true;
  const server = net.createServer((serverSide) => {
    serverSide.on("data", createMessageReader(value => {
      if (!responding) return;
      const frame = value as ClientMessage;
      if (frame.type === "register") {
        writeMessage(serverSide, {
          type: "registered", sessionId: frame.sessionId ?? "stable-test",
          features: [CONVERSATION_CONTRACT_FEATURE, EXACT_SEND_FEATURE],
        });
      }
      if (frame.type === "list") {
        writeMessage(serverSide, { type: "sessions", requestId: frame.requestId, sessions: [] });
      }
    }, error => serverSide.destroy(error)));
    serverSide.on("error", () => undefined);
    server.close();
    resolveReady({
      serverSide,
      closeServerSideAbruptly() {
        // A real socket close exercises passive disconnect detection.
        serverSide.destroy();
      },
      stopResponding() {
        // Simulate a half-open socket: the peer stops reading/replying but the
        // connection is not closed, so no "close"/"error" event reaches the
        // client. Only an active liveness probe can detect this.
        responding = false;
      },
    });
  });
  server.on("error", (err) => rejectReady(err));
  await new Promise<void>((resolve) => server.listen(socketPath, () => resolve()));

  await client.connect(
    {
      name: "liveness-unit",
      cwd: homeDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    },
    "stable-liveness-unit",
  );
  const { serverSide, closeServerSideAbruptly, stopResponding } = await ready;
  return { client, serverSide, closeServerSideAbruptly, stopResponding };
}

test("client emits disconnected when the peer closes the socket", async () => {
  const { client, closeServerSideAbruptly } = await registeredClientAgainstFakeSocket();
  try {
    assert.equal(client.isConnected(), true, "client should be connected after register");

    const disconnected = once(client, "disconnected");
    closeServerSideAbruptly();

    const event = await Promise.race([
      disconnected,
      new Promise<never>((_, reject) =>
        setTimeout(
          () => reject(new Error("client never noticed the abruptly-closed peer (half-open socket)")),
          3000,
        ),
      ),
    ]);
    assert.ok(event, "expected a disconnected event");
    assert.equal(client.isConnected(), false);
  } finally {
    await client.disconnect().catch(() => undefined);
  }
});

test("client liveness heartbeat detects a half-open socket within a bounded window", async () => {
  const { client, stopResponding } = await registeredClientAgainstFakeSocket();
  try {
    assert.equal(client.isConnected(), true);

    const disconnected = once(client, "disconnected");
    // Simulate a half-open socket: the peer stops replying but does NOT close
    // the connection. No "close"/"error" event reaches the client, so passive
    // detection cannot fire — only the liveness heartbeat can notice.
    stopResponding();
    const sending = client.send("colleague", { text: "work", replyTo: "question", messageId: "half-open-send" });
    const cancelling = client.cancelMessage("half-open-send");
    const listing = assert.rejects(client.listSessions(), /List sessions timeout/);

    // The configured timeout is capped at the 100ms interval.
    const event = await Promise.race([
      disconnected,
      new Promise<never>((_, reject) =>
        setTimeout(
          () => reject(new Error("liveness heartbeat did not detect the half-open socket within 3s")),
          3000,
        ),
      ),
    ]);
    assert.ok(event, "expected the heartbeat to surface a disconnected event");
    assert.equal(client.isConnected(), false);
    assert.equal((await sending).delivery, "unknown");
    assert.equal((await cancelling).delivery, "unknown");
    await listing;
  } finally {
    await client.disconnect().catch(() => undefined);
  }
});