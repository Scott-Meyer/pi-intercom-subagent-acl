// Fork: delivery-feedback regressions for the two silences users hit in
// practice:
//   - Duplicate session names used to register fine and only fail lazily at
//     send time (E_AMBIGUOUS_TARGET). Names are now de-duplicated at
//     registration/presence time (auto-suffixed "name-2", "name-3", ...), so
//     every roster name is unambiguously addressable.
//   - A queued mailbox send used to report "delivered" and then quietly die up
//     to 24h later when the target never reconnected. The broker now pushes an
//     "expired" receipt back to the sender. Capacity eviction shares the same
//     notification path, so it is used here as the deterministic trigger.
import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import test from "node:test";
import type { SessionRegistration } from "../types.ts";
import { IntercomClient } from "./client.ts";

const repoDir = process.cwd();
const TSX_BIN = process.env.PI_INTERCOM_TEST_TSX_BIN
  ?? path.join(repoDir, "node_modules", "tsx", "dist", "cli.mjs");

function baseRegistration(name: string): SessionRegistration {
  return {
    name,
    cwd: "/test",
    model: "test-model",
    pid: process.pid,
    startedAt: Date.now(),
    lastActivity: Date.now(),
  };
}

async function startBroker(agentDir: string): Promise<ChildProcessWithoutNullStreams> {
  const broker = spawn(
    process.execPath,
    [TSX_BIN, path.join(repoDir, "broker", "broker.ts")],
    {
      cwd: repoDir,
      env: { ...process.env, PI_CODING_AGENT_DIR: agentDir },
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  const ready = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Broker startup timed out")), 10_000);
    broker.stdout.on("data", (chunk: Buffer) => {
      if (chunk.toString().includes("Intercom broker started")) {
        clearTimeout(timeout);
        resolve();
      }
    });
    broker.once("exit", (code, signal) => {
      clearTimeout(timeout);
      reject(new Error(`Broker exited before startup (${code ?? signal})`));
    });
  });
  await ready;
  return broker;
}

async function stopBroker(broker: ChildProcessWithoutNullStreams): Promise<void> {
  if (broker.exitCode !== null) return;
  broker.kill("SIGTERM");
  await once(broker, "exit");
}

test("name dedup: registering a colliding name auto-suffixes and stays addressable", { concurrency: false, timeout: 30_000 }, async () => {
  const agentDir = mkdtempSync(path.join(tmpdir(), "pi-intercom-namedup-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  const broker = await startBroker(agentDir);
  const clients: IntercomClient[] = [];

  try {
    const first = new IntercomClient();
    const second = new IntercomClient();
    clients.push(first, second);
    await first.connect(baseRegistration("pi"), randomUUID());
    await second.connect(baseRegistration("pi"), randomUUID());

    const roster = await first.listSessions();
    const names = roster.map((session) => session.name).sort();
    assert.deepEqual(names, ["pi", "pi-2"], `colliding register auto-suffixes: got ${JSON.stringify(names)}`);

    // Both names are unambiguously addressable by name, not just by ID.
    const toSecond = await first.send("pi-2", { text: "hello second" });
    assert.equal(toSecond.delivered, true, "suffixed name resolves to the second session");
    assert.equal(toSecond.delivery, "socket_delivered");
    const toFirst = await second.send("pi", { text: "hello first" });
    assert.equal(toFirst.delivered, true, "original name still resolves to the first session");

    // A third collision keeps counting up.
    const third = new IntercomClient();
    clients.push(third);
    await third.connect(baseRegistration("pi"), randomUUID());
    const roster3 = await first.listSessions();
    const names3 = roster3.map((session) => session.name).sort();
    assert.deepEqual(names3, ["pi", "pi-2", "pi-3"]);

    // Presence renames go through the same dedup: renaming a session onto an
    // existing name suffixes instead of creating an ambiguous roster.
    third.updatePresence({ name: "pi" });
    const roster4 = await first.listSessions();
    const thirdEntry = roster4.find((session) => session.id === third.sessionId);
    assert.equal(thirdEntry?.name, "pi-3", "presence rename collision keeps the suffix");

    // When the colliding session leaves, a fresh registration can take the
    // original name again.
    await third.disconnect();
    const fourth = new IntercomClient();
    clients.push(fourth);
    await fourth.connect(baseRegistration("pi-3"), randomUUID());
    const roster5 = await first.listSessions();
    const names5 = roster5.map((session) => session.name).sort();
    assert.deepEqual(names5, ["pi", "pi-2", "pi-3"], `suffix slot freed on disconnect: got ${JSON.stringify(names5)}`);
  } finally {
    for (const client of clients) {
      await client.disconnect().catch(() => undefined);
    }
    await stopBroker(broker);
    process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("mailbox undelivered receipt: sender is notified when a queued message can no longer be delivered", { concurrency: false, timeout: 60_000 }, async () => {
  const agentDir = mkdtempSync(path.join(tmpdir(), "pi-intercom-expiry-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  const broker = await startBroker(agentDir);
  const clients: IntercomClient[] = [];

  try {
    const sender = new IntercomClient();
    clients.push(sender);
    await sender.connect(baseRegistration("sender"), randomUUID());

    const target = new IntercomClient();
    await target.connect(baseRegistration("target"), randomUUID());
    const targetId = target.sessionId!;
    await target.disconnect();

    // Send to the now-disconnected target: accepted into its mailbox.
    const queued = await sender.send(targetId, { text: "while you were away" });
    assert.equal(queued.delivered, true, "send to disconnected session is queued");
    assert.equal(queued.delivery, "queued");

    const receipts: { messageId: string; status: string; detail?: string }[] = [];
    sender.onMessageReceipt((_from, receipt) => {
      receipts.push({ messageId: receipt.messageId, status: receipt.status, detail: receipt.detail });
    });

    // Capacity eviction runs the same undelivered-notification path as time
    // expiry but is deterministic in a test: overflow the mailbox and the
    // oldest queued message (ours) is evicted with a receipt to the sender.
    // The broker token-bucket rate limits each connection (240 burst, 120/s
    // refill), so pace the filler just under the refill rate.
    for (let index = 0; index < 256; index += 1) {
      const result = await sender.send(targetId, { text: `filler ${index}` });
      if (!result.delivered) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Timed out waiting for undelivered receipt")), 10_000);
      const check = setInterval(() => {
        const hit = receipts.find((receipt) => receipt.messageId === queued.id && receipt.status === "expired");
        if (hit) {
          clearInterval(check);
          clearTimeout(timeout);
          resolve();
        }
      }, 50);
    });

    const receipt = receipts.find((candidate) => candidate.messageId === queued.id);
    assert.ok(receipt, "sender received a receipt for the queued message");
    assert.equal(receipt.status, "expired");
    assert.match(receipt.detail ?? "", /evicted/i, "receipt detail explains the undelivered reason");
  } finally {
    for (const client of clients) {
      await client.disconnect().catch(() => undefined);
    }
    await stopBroker(broker);
    process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    rmSync(agentDir, { recursive: true, force: true });
  }
});
