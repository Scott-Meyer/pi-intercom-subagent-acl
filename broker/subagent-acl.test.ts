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

    // --- advertise: self-promotion to full main-level visibility ---

    // A main has nothing to gain from advertising; reject rather than no-op.
    const mainAdvertiseRejected = await mainA.advertise("main-a-public");
    assert.equal(mainAdvertiseRejected.ok, false);
    assert.equal(mainAdvertiseRejected.code, "E_NOT_ELIGIBLE");

    // Name collisions are rejected: against another live session's name...
    const nameCollision = await childOfA.advertise("main-b");
    assert.equal(nameCollision.ok, false);
    assert.equal(nameCollision.code, "E_NAME_TAKEN");
    // ...and against another live session's raw ID (findSessions resolves an
    // exact ID before it ever checks names, so this name would be unreachable).
    const idCollision = await childOfA.advertise(mainB.sessionId!);
    assert.equal(idCollision.ok, false);
    assert.equal(idCollision.code, "E_NAME_TAKEN");
    // Control characters are rejected (roster-row / rendering injection guard).
    const controlCharRejected = await childOfA.advertise("good\n\u2022 fake-main");
    assert.equal(controlCharRejected.ok, false);
    assert.equal(controlCharRejected.code, "E_INVALID_NAME");

    // Successful advertise: childOfA self-promotes.
    const advertised = await childOfA.advertise("child-a-public");
    assert.equal(advertised.ok, true);
    assert.equal(advertised.name, "child-a-public");

    // Re-advertising (not eligible a second time) is rejected, not a silent no-op.
    const reAdvertise = await childOfA.advertise("child-a-public-2");
    assert.equal(reAdvertise.ok, false);
    assert.equal(reAdvertise.code, "E_NOT_ELIGIBLE");

    // Two-way promotion: mainB (previously blind to childOfA) now sees it...
    const mainBSeesAfter = idsOf(await mainB.listSessions());
    assert.ok(mainBSeesAfter.has(childOfA.sessionId!), "main B now sees the advertised former-child");
    // ...and childOfA (previously blind to everyone but mainA) now sees mainB too.
    const childASeesAfter = idsOf(await childOfA.listSessions());
    assert.ok(childASeesAfter.has(mainB.sessionId!), "advertised child now sees main B");
    assert.ok(childASeesAfter.has(childOfB.sessionId!) === false, "still does not see main B's un-advertised child");

    // Reachable both ways post-promotion, and specifically by the newly
    // claimed public NAME (not just the underlying session ID) -- that's the
    // whole point of advertising under a chosen name.
    const toAdvertisedById = await mainB.send(childOfA.sessionId!, { text: "hi, saw you on the roster" });
    assert.equal(toAdvertisedById.delivered, true);
    const toAdvertisedByName = await mainB.send("child-a-public", { text: "hi by your new public name" });
    assert.equal(toAdvertisedByName.delivered, true, "reachable by the advertised public name while still live");
    const fromAdvertised = await childOfA.send(mainB.sessionId!, { text: "hi back" });
    assert.equal(fromAdvertised.delivered, true);

    // Ordinary presence sync (fired on every real intercom tool call) must
    // never silently revert the advertised identity back toward a fallback
    // name/alias while `advertised` stays true.
    (childOfA as any).updatePresence({ name: "subagent-chat-fallback-should-not-apply", runtimeFallbackAlias: true });
    await new Promise((resolve) => setTimeout(resolve, 50));
    const afterPresenceSpoof = (await mainB.listSessions()).find((s) => s.id === childOfA.sessionId);
    assert.equal(afterPresenceSpoof?.name, "child-a-public", "advertised name survives a routine presence sync");
    assert.equal(afterPresenceSpoof?.runtimeFallbackAlias, false, "advertised runtimeFallbackAlias survives a routine presence sync");

    // Advertising is a live-connection promotion only. Once the advertised
    // child disconnects, an unrelated main must lose the ability to
    // list/send/queue to it under its former public name -- the promotion
    // must not survive into the disconnected-mailbox snapshot.
    const advertisedChildId = childOfA.sessionId!;
    await childOfA.disconnect();
    clients.splice(clients.indexOf(childOfA), 1);
    await new Promise((resolve) => setTimeout(resolve, 100));

    const mainBAfterDisconnect = idsOf(await mainB.listSessions());
    assert.ok(!mainBAfterDisconnect.has(advertisedChildId), "main B no longer sees the disconnected former child at all");

    const queueToFormerPublicName = await mainB.send("child-a-public", { text: "are you still there?" });
    assert.equal(queueToFormerPublicName.delivered, false, "unrelated main cannot queue mail to the former public name after disconnect");
    assert.match(queueToFormerPublicName.reason ?? "", /not found/i);
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
