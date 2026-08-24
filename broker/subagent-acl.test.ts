// ACL fork: end-to-end regression coverage for subagent visibility scoping.
//
// Policy under test:
//   - A main session sees every other main session, plus only the subagent
//     children it personally supervises.
//   - A subagent session sees only its own supervisor -- never siblings,
//     never unrelated mains.
// Coverage includes both supervisorSessionId matching and the supervisorName
// fallback (the stableId mismatch case), plus send-path enforcement, not just
// the list response.
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

function idsOf(sessions: { id: string }[]): Set<string> {
  return new Set(sessions.map((s) => s.id));
}

test("subagent ACL: list/send scoping by supervisorSessionId and supervisorName fallback", { concurrency: false, timeout: 30_000 }, async () => {
  const agentDir = mkdtempSync(path.join(tmpdir(), "pi-intercom-acl-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  // The client library resolves the broker connect target from THIS
  // process's env, independent of the env passed only to the spawned broker
  // subprocess. Without this, clients silently connect to any real live
  // broker at the default location instead of the isolated test broker.
  process.env.PI_CODING_AGENT_DIR = agentDir;
  const broker = await startBroker(agentDir);
  const clients: IntercomClient[] = [];

  try {
    const mainAId = randomUUID();
    const mainBId = randomUUID();
    const mainA = new IntercomClient();
    const mainB = new IntercomClient();
    clients.push(mainA, mainB);
    await mainA.connect(baseRegistration("main-a"), mainAId);
    await mainB.connect(baseRegistration("main-b"), mainBId);

    // childOfA matches its supervisor by session ID.
    const childOfA = new IntercomClient();
    clients.push(childOfA);
    await childOfA.connect({
      ...baseRegistration("child-of-a"),
      isSubagent: true,
      supervisorSessionId: mainAId,
      supervisorName: "main-a",
    });

    // childOfB deliberately carries a WRONG supervisorSessionId (simulating
    // the stableId-mismatch case: pi-subagents passed the parent's raw pi
    // session id, but the parent registered with pi-intercom under a
    // different stable id) and must still resolve via supervisorName.
    const childOfB = new IntercomClient();
    clients.push(childOfB);
    await childOfB.connect({
      ...baseRegistration("child-of-b"),
      isSubagent: true,
      supervisorSessionId: randomUUID(),
      supervisorName: "main-b",
    });

    // --- list scoping ---
    const mainASees = idsOf(await mainA.listSessions());
    const mainBSees = idsOf(await mainB.listSessions());
    const childASees = idsOf(await childOfA.listSessions());
    const childBSees = idsOf(await childOfB.listSessions());

    assert.ok(mainASees.has(mainA.sessionId!), "main A sees itself");
    assert.ok(mainASees.has(mainB.sessionId!), "main A sees main B");
    assert.ok(mainASees.has(childOfA.sessionId!), "main A sees its own child");
    assert.ok(!mainASees.has(childOfB.sessionId!), "main A does NOT see main B's child");

    assert.ok(mainBSees.has(mainA.sessionId!), "main B sees main A");
    assert.ok(mainBSees.has(childOfB.sessionId!), "main B sees its own child (via name fallback)");
    assert.ok(!mainBSees.has(childOfA.sessionId!), "main B does NOT see main A's child");

    assert.deepEqual(childASees, new Set([childOfA.sessionId!, mainA.sessionId!]), "child of A sees only itself + its supervisor");
    assert.deepEqual(childBSees, new Set([childOfB.sessionId!, mainB.sessionId!]), "child of B sees only itself + its supervisor (name-fallback matched)");

    // --- send-path enforcement (not just list) ---
    // send() never rejects on a broker-side delivery failure; it resolves
    // with { delivered: false, delivery: "failed", ... }. A hidden target
    // must fail exactly like a nonexistent one ("Session not found"), never
    // with a distinguishable ACL-denied reason.

    // Sibling children cannot reach each other, even by exact session ID.
    const siblingSend = await childOfA.send(childOfB.sessionId!, { text: "hi sibling" });
    assert.equal(siblingSend.delivered, false, "child of A cannot send to child of B by ID");
    assert.match(siblingSend.reason ?? "", /not found/i);

    // A main cannot reach another main's child, even by exact session ID.
    const crossMainSend = await mainA.send(childOfB.sessionId!, { text: "hi other child" });
    assert.equal(crossMainSend.delivered, false, "main A cannot send to main B's child by ID");
    assert.match(crossMainSend.reason ?? "", /not found/i);

    // A child can reach its own supervisor.
    const toSupervisor = await childOfA.send(mainA.sessionId!, { text: "status update" });
    assert.equal(toSupervisor.delivered, true);
    assert.equal(toSupervisor.delivery, "socket_delivered");

    // A main can reach its own child.
    const toOwnChild = await mainB.send(childOfB.sessionId!, { text: "go do X" });
    assert.equal(toOwnChild.delivered, true);
    assert.equal(toOwnChild.delivery, "socket_delivered");

    // Mains still see and can reach each other normally.
    const mainToMain = await mainA.send(mainB.sessionId!, { text: "peer to peer" });
    assert.equal(mainToMain.delivered, true);
    assert.equal(mainToMain.delivery, "socket_delivered");
  } finally {
    for (const client of clients) {
      try {
        await client.disconnect();
      } catch {
        // best-effort cleanup
      }
    }
    await stopBroker(broker);
    rmSync(agentDir, { recursive: true, force: true });
    if (previousAgentDir === undefined) {
      delete process.env.PI_CODING_AGENT_DIR;
    } else {
      process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    }
  }
});
