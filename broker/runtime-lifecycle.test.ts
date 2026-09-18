import test, { type TestContext } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import net from "node:net";
import { fileURLToPath } from "node:url";
import { ParleyClient } from "./client.ts";
import { getBrokerConnectTarget, getParleyDirPath } from "./paths.ts";
import { createMessageReader, writeMessage } from "./framing.ts";
import type { Message, SessionInfo } from "../types.ts";

const extensionDir = fileURLToPath(new URL("../", import.meta.url));
const brokerUrl = new URL("./broker.ts", import.meta.url).href;
const bootstrap = `
  process.once('message', async () => {
    try { await import(${JSON.stringify(brokerUrl)}); }
    catch (error) { console.error(error); process.exit(1); }
  });
  process.send({ready: true});
`;

function runtime(t: TestContext) {
  const directory = mkdtempSync(join(tmpdir(), "parley-life-"));
  const previous = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = directory;
  const resources: Array<() => void | Promise<void>> = [];
  t.after(async () => {
    try {
      for (const release of resources.reverse()) await release();
    } finally {
      if (previous === undefined) delete process.env.PI_CODING_AGENT_DIR;
      else process.env.PI_CODING_AGENT_DIR = previous;
      rmSync(directory, { recursive: true, force: true });
    }
  });
  return { directory, resources };
}
type Runtime = ReturnType<typeof runtime>;

async function candidate(ctx: Runtime) {
  const child = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "-e", bootstrap], {
    cwd: extensionDir,
    env: { ...process.env, PI_CODING_AGENT_DIR: ctx.directory },
    detached: true,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  let stdout = "";
  let stderr = "";
  child.stderr!.on("data", (chunk) => { stderr = `${stderr}${chunk}`.slice(-4000); });
  const exited = new Promise<number | null>((resolve) => child.once("exit", resolve));
  ctx.resources.push(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await exited;
  });
  await once(child, "message");
  return {
    child, exited,
    start(): Promise<boolean> {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => finish(new Error(`Broker failed to start: ${stderr}`)), 15_000);
        function finish(error?: Error, running = false) {
          clearTimeout(timer);
          child.stdout!.off("data", onData);
          child.off("exit", onExit);
          child.off("error", onError);
          if (error) reject(error);
          else resolve(running);
        }
        function onData(chunk: Buffer) {
          stdout = `${stdout}${chunk}`.slice(-4000);
          if (stdout.includes("Parley broker started")) finish(undefined, true);
        }
        function onExit() { finish(undefined, false); }
        function onError(error: Error) { finish(error); }
        child.stdout!.on("data", onData);
        child.once("exit", onExit);
        child.once("error", onError);
        child.send({ start: true });
      });
    },
    diagnostics: () => stderr,
  };
}

async function connect(ctx: Runtime, name: string, id = randomUUID()) {
  const client = new ParleyClient();
  ctx.resources.push(() => client.disconnect());
  client.on("error", () => undefined);
  await client.connect({
    name, cwd: ctx.directory, model: "lifecycle-test", pid: process.pid,
    startedAt: Date.now(), lastActivity: Date.now(),
  }, id);
  return { client, id };
}

interface Health {
  type: string;
  requestId: string;
  broker: { pid: number; instanceId: string; packageVersion: string; sourceId: string };
}
async function health(): Promise<Health> {
  const target = getBrokerConnectTarget();
  const requestId = randomUUID();
  return new Promise((resolve, reject) => {
    const socket = typeof target === "string" ? net.connect(target) : net.connect(target.port, target.host);
    let settled = false;
    const finish = (error?: Error, response?: Health) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      if (error) reject(error);
      else resolve(response!);
    };
    const timer = setTimeout(() => finish(new Error("Broker health timed out")), 5000);
    socket.once("error", (error) => finish(error));
    socket.once("connect", () => writeMessage(socket, {
      type: "health", requestId,
      ...(typeof target !== "string" ? { stateId: target.stateId } : {}),
    }));
    socket.on("data", createMessageReader((value) => {
      const response = value as Health;
      if (response.type === "health_ok" && response.requestId === requestId) finish(undefined, response);
      else finish(new Error("Invalid broker health response"));
    }, (error) => finish(error)));
  });
}

async function exchange(sender: ParleyClient, receiver: ParleyClient, receiverId: string) {
  const roster = await sender.listSessions();
  const target = roster.find((seat) => seat.id === receiverId);
  assert.ok(target);
  const incoming = once(receiver, "message") as Promise<[SessionInfo, Message]>;
  const receipt = await sender.sendToSession(target, { text: "Only one broker owns this conversation." });
  assert.equal(receipt.delivered, true);
  assert.equal(receipt.delivery, "socket_delivered");
  const [, message] = await incoming;
  assert.equal(message.id, receipt.id);
  assert.equal(message.content.text, "Only one broker owns this conversation.");
}

test("simultaneous direct starts have one broker; losing starts cannot disrupt live messaging", { timeout: 30_000 }, async (t) => {
  const ctx = runtime(t);
  const peers = await Promise.all(Array.from({ length: 8 }, () => candidate(ctx)));
  const starts = peers.map((peer) => peer.start());
  const winner = await Promise.any(starts.map(async (result, index) => {
    if (!await result) throw new Error(peers[index]!.diagnostics());
    return peers[index]!;
  }));
  // Register immediately; an idle broker may legitimately exit before a slow
  // contender finishes importing. This keeps the actual winning owner alive.
  const sender = await connect(ctx, "lifecycle-sender");
  const results = await Promise.all(starts);
  assert.equal(results.filter(Boolean).length, 1, results.join(","));
  const receiver = await connect(ctx, "lifecycle-receiver");
  const before = (await health()).broker;
  assert.equal(before.pid, winner.child.pid);
  assert.match(before.instanceId, /^[0-9a-f-]{36}$/);
  assert.match(before.sourceId, /^[0-9a-f]{64}$/);
  assert.equal(before.packageVersion, JSON.parse(readFileSync(join(extensionDir, "package.json"), "utf8")).version);
  await exchange(sender.client, receiver.client, receiver.id);
  const late = await candidate(ctx);
  assert.equal(await late.start(), false);
  assert.notEqual(await late.exited, 0);
  assert.deepEqual((await health()).broker, before);
  await exchange(sender.client, receiver.client, receiver.id);
});

test("idle shutdown and restart retain collaboration state and publish a distinct owner", { timeout: 30_000 }, async (t) => {
  const ctx = runtime(t);
  const original = await candidate(ctx);
  assert.equal(await original.start(), true, original.diagnostics());
  const seat = await connect(ctx, "lifecycle-persistent");
  const eventId = randomUUID();
  const recorded = await seat.client.reportCompactionCompleted(eventId);
  assert.equal(recorded.generation, 1);
  const before = (await health()).broker;
  await seat.client.disconnect();
  assert.equal(await original.exited, 0);
  assert.equal(existsSync(join(getParleyDirPath(ctx.directory), "broker.pid")), false);
  const replacement = await candidate(ctx);
  assert.equal(await replacement.start(), true, replacement.diagnostics());
  const resumed = await connect(ctx, "lifecycle-persistent", seat.id);
  assert.deepEqual(await resumed.client.reportCompactionCompleted(eventId), recorded);
  const after = (await health()).broker;
  assert.equal(after.pid, replacement.child.pid);
  assert.notEqual(after.instanceId, before.instanceId);
  assert.equal(readFileSync(join(getParleyDirPath(ctx.directory), "broker.pid"), "utf8"), String(replacement.child.pid));
});

test("abrupt broker death is recoverable despite stale endpoints and a reused discovery PID", { timeout: 30_000 }, async (t) => {
  const ctx = runtime(t);
  const original = await candidate(ctx);
  assert.equal(await original.start(), true, original.diagnostics());
  const seat = await connect(ctx, "lifecycle-crash");
  const before = (await health()).broker;
  original.child.kill("SIGKILL");
  await original.exited;
  await seat.client.disconnect();
  writeFileSync(join(getParleyDirPath(ctx.directory), "broker.pid"), String(process.pid));
  const replacement = await candidate(ctx);
  assert.equal(await replacement.start(), true, replacement.diagnostics());
  const resumed = await connect(ctx, "lifecycle-crash", seat.id);
  assert.equal(resumed.client.sessionId, seat.id);
  const after = (await health()).broker;
  assert.equal(after.pid, replacement.child.pid);
  assert.notEqual(after.instanceId, before.instanceId);
  assert.deepEqual((await resumed.client.listSessions()).map((entry) => entry.id), [seat.id]);
});
