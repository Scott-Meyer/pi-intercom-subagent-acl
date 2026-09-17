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
import { existsSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import test from "node:test";
import net from "node:net";
import type { BrokerMessage, Message, MessageControl, SessionRegistration } from "../types.ts";
import { IntercomClient } from "./client.ts";
import { createMessageReader, writeMessage } from "./framing.ts";
import { getBrokerSocketPath } from "./paths.ts";

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

async function startBroker(agentDir: string, env: NodeJS.ProcessEnv = {}): Promise<ChildProcessWithoutNullStreams> {
  const broker = spawn(
    process.execPath,
    [TSX_BIN, path.join(repoDir, "broker", "broker.ts")],
    {
      cwd: repoDir,
      env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, ...env },
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
    assert.equal((await sender.cancelMessage(queued.id)).cancellation, "not_delivered", "expiry establishes that the queued work never reached an endpoint");
  } finally {
    for (const client of clients) {
      await client.disconnect().catch(() => undefined);
    }
    await stopBroker(broker);
    process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    rmSync(agentDir, { recursive: true, force: true });
  }
});


async function withConversationBroker(run: (agentDir: string, connect: (name: string, id?: string, beforeConnect?: (client: IntercomClient) => void) => Promise<IntercomClient>) => Promise<void>) {
  const agentDir = mkdtempSync(path.join(process.platform === "win32" ? tmpdir() : "/tmp", "pi-conv-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  const clients: IntercomClient[] = [];
  const broker = await startBroker(agentDir);
  try {
    await run(agentDir, async (name, id = randomUUID(), beforeConnect) => {
      const client = new IntercomClient();
      clients.push(client);
      beforeConnect?.(client);
      await client.connect(baseRegistration(name), id);
      return client;
    });
  } finally {
    for (const client of clients) await client.disconnect().catch(() => undefined);
    await stopBroker(broker);
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    rmSync(agentDir, { recursive: true, force: true });
  }
}

test("colleagues can thread ordinary replies, clarify an open ask and consult a third peer without completing it", { timeout: 30_000 }, async () => {
  await withConversationBroker(async (agentDir, connect) => {
    const planner = await connect("planner");
    const reviewer = await connect("reviewer");
    const specialist = await connect("specialist");
    const ordinary = await planner.send("reviewer", { text: "The migration branch is ready" });
    assert.equal(ordinary.recipient?.id, reviewer.sessionId);
    assert.equal(ordinary.recipient?.name, "reviewer");
    const thanks = await reviewer.send("planner", { text: "Thanks, reviewing now", replyTo: ordinary.id, completesAsk: false });
    assert.equal(thanks.delivered, true, "ordinary notifications are valid thread anchors");
    assert.equal((await specialist.send("planner", { text: "forged reply", replyTo: ordinary.id })).code, "E_REPLY_TARGET", "correlation does not bypass participant validation");
    assert.equal((await reviewer.send("specialist", { text: "misaddressed", replyTo: ordinary.id })).code, "E_REPLY_TARGET");

    const incomingAsk = once(reviewer, "message");
    const question = await planner.send("reviewer", { text: "Is it safe to deploy?", expectsReply: true, senderWaitMode: "nonblocking", completesAsk: false });
    const [, received] = await incomingAsk as [unknown, Message];
    assert.equal(received.senderWaitMode, "nonblocking");
    assert.ok(received.replyDeadline! > received.brokerReceivedAt!);
    const pendingRecord = path.join(agentDir, "intercom", "pending-asks", `${encodeURIComponent(question.id)}.json`);
    assert.equal(existsSync(pendingRecord), true, "the recovery record represents the open question");
    const progress = await reviewer.send("planner", { text: "Still checking the migration lock", replyTo: question.id, completesAsk: false });
    assert.equal(progress.delivered, true);
    assert.equal(existsSync(pendingRecord), true, "correlated progress is not a final answer");
    const clarification = await reviewer.send("planner", { text: "Which database version?", expectsReply: true, replyTo: question.id, senderWaitMode: "nonblocking" });
    assert.equal(clarification.delivered, true, "legacy clarification asks do not complete the parent question");
    assert.equal(existsSync(pendingRecord), true);
    assert.equal((await reviewer.send("specialist", { text: "Could you verify this lock behavior?", expectsReply: true })).delivered, true);
    assert.equal((await reviewer.send("planner", { text: "Another clarification", expectsReply: true })).delivered, true, "reverse asks do not assume the colleague is blocked");
    const answerOptions = { text: "Yes, the lock is safe", replyTo: question.id, completesAsk: true, messageId: randomUUID() };
    assert.equal((await reviewer.send("planner", answerOptions)).delivered, true);
    assert.equal(existsSync(pendingRecord), false, "only an answer completes the recovery record");
    assert.equal((await reviewer.send("planner", answerOptions)).delivered, true, "a repeated answer acknowledgement still deduplicates after completion");
    assert.equal((await reviewer.send("planner", { text: "One later observation", replyTo: question.id, completesAsk: false })).delivered, true, "conversation survives ask completion");

    const originalReviewerId = reviewer.sessionId!;
    await reviewer.disconnect();
    const replacement = await connect("reviewer");
    const rebound = await planner.send(originalReviewerId, { text: "Continue in the replacement runtime" });
    assert.equal(rebound.delivery, "socket_delivered");
    assert.equal(rebound.recipient?.id, replacement.sessionId, "mailbox rebound reports the actual endpoint, not the stale requested ID");
    const replayedRebound = await planner.send(originalReviewerId, { text: "Continue in the replacement runtime", messageId: rebound.id });
    assert.equal(replayedRebound.recipient?.id, replacement.sessionId);
  });
});

test("withdrawal distinguishes undelivered mail from live work and aborting an ask notifies its recipient", { timeout: 30_000 }, async () => {
  await withConversationBroker(async (_agentDir, connect) => {
    const sender = await connect("sender");
    const receiver = await connect("receiver");
    const receiverId = receiver.sessionId!;
    const controls: MessageControl[] = [];
    receiver.onMessageControl((_from, control) => controls.push(control));
    const live = await sender.send("receiver", { text: "Please check the service" });
    const control = once(receiver, "message_control");
    const withdrawn = await sender.cancelMessage(live.id);
    await control;
    assert.equal(withdrawn.cancellation, "withdrawal_requested");
    assert.equal(withdrawn.recipient?.id, receiverId);
    assert.equal(withdrawn.outcomeKnown, true);
    assert.equal(controls.length, 1);
    assert.equal((await sender.cancelMessage(live.id)).cancellation, "withdrawal_requested");
    const withdrawnReplay = await sender.send("receiver", { text: "Please check the service", messageId: live.id });
    assert.equal(withdrawnReplay.delivery, "socket_delivered", "withdrawal cannot rewrite accepted delivery as nondelivery");
    assert.equal(withdrawnReplay.cancellation, "withdrawal_requested", "recovery exposes that this already-accepted message was subsequently withdrawn");
    await receiver.listSessions();
    assert.equal(controls.length, 1, "retrying withdrawal does not repeat the notice");
    assert.equal((await receiver.cancelMessage(live.id)).delivered, false, "only the original sender can withdraw");

    const ask = await sender.send("receiver", { text: "Should we proceed?", expectsReply: true });
    const abortNotice = once(receiver, "message_control");
    sender.cancelAsk(ask.id);
    const [, aborted] = await abortNotice as [unknown, MessageControl];
    assert.equal(aborted.messageId, ask.id);
    assert.equal(aborted.action, "cancel");
    assert.match(aborted.detail!, /withdrew/);
    assert.equal((await receiver.send("sender", { text: "I had already checked it", replyTo: ask.id })).delivered, true, "withdrawn asks can still be discussed");

    await receiver.disconnect();
    const queued = await sender.send(receiverId, { text: "Do not perform this obsolete work" });
    assert.equal(queued.delivery, "queued");
    const removed = await sender.cancelMessage(queued.id);
    assert.equal(removed.cancellation, "removed_from_mailbox");
    assert.equal(removed.recipient?.id, receiverId);
    assert.equal((await sender.cancelMessage(queued.id)).cancellation, "removed_from_mailbox");
    const received: Message[] = [];
    const reconnected = await connect("receiver", receiverId, client => {
      client.on("message", (_from, message) => received.push(message));
    });
    await sender.send(receiverId, { text: "Fresh work only" });
    await reconnected.listSessions();
    assert.deepEqual(received.map(message => message.content.text), ["Fresh work only"]);
    const failed = await sender.send("missing-colleague", { text: "Never accepted" });
    assert.equal(failed.delivery, "failed");
  });
});

test("lost local acknowledgements preserve unknown outcomes and IDs, cancellation correlation, and deduplicated recovery", { timeout: 30_000 }, async () => {
  await withConversationBroker(async (agentDir, connect) => {
    const receiver = await connect("receiver");
    const inbound: Message[] = [];
    receiver.on("message", (_from, message) => inbound.push(message));
    const proxyDir = path.join(agentDir, "proxy");
    const proxyPath = getBrokerSocketPath(process.platform, proxyDir);
    mkdirSync(path.dirname(proxyPath), { recursive: true });
    const sockets: net.Socket[] = [];
    let dropAcks = true;
    let dropCancellationAcks = false;
    const proxy = net.createServer(source => {
      const destination = net.connect(getBrokerSocketPath(process.platform, agentDir));
      sockets.push(source, destination);
      source.on("error", () => undefined);
      destination.on("error", () => undefined);
      source.pipe(destination);
      destination.on("data", createMessageReader(value => {
        const frame = value as { type: string; requestId?: string };
        if (frame.type === "delivered" || frame.type === "delivery_failed") {
          if (frame.requestId ? dropCancellationAcks : dropAcks) return;
        }
        writeMessage(source, value);
      }, error => source.destroy(error)));
    });
    proxy.listen(proxyPath);
    await once(proxy, "listening");
    process.env.PI_CODING_AGENT_DIR = proxyDir;
    const sender = await connect("sender");
    process.env.PI_CODING_AGENT_DIR = agentDir;
    try {
      const abort = new AbortController();
      const receipt = once(receiver, "message");
      const pending = sender.send("receiver", { text: "Accepted but ACK lost", messageId: "local-lost-ack", timeoutMs: 100, signal: abort.signal });
      await receipt;
      abort.abort();
      const uncertain = await pending;
      assert.equal(uncertain.id, "local-lost-ack");
      assert.equal(uncertain.delivery, "unknown", "aborting after write is not known cancellation");
      assert.equal(uncertain.outcomeKnown, false);
      assert.equal(uncertain.retryable, false);
      dropAcks = false;
      const replay = await sender.send("receiver", { text: "Accepted but ACK lost", messageId: uncertain.id });
      assert.equal(replay.delivery, "socket_delivered");
      assert.equal(replay.recipient?.id, receiver.sessionId);
      await receiver.listSessions();
      assert.equal(inbound.length, 1, "same-ID recovery does not repeat work");

      dropAcks = true;
      const acceptedThenCancelled = once(receiver, "message");
      const sending = sender.send("receiver", { text: "Send and cancellation have independent waiters", messageId: "concurrent-cancel", timeoutMs: 100 });
      await acceptedThenCancelled;
      const cancelled = await sender.cancelMessage("concurrent-cancel");
      assert.equal(cancelled.cancellation, "withdrawal_requested");
      assert.equal((await sending).delivery, "unknown", "cancel ACK cannot falsely acknowledge the original send");
      dropCancellationAcks = true;
      const cancelTimeout = await sender.cancelMessage(uncertain.id, { timeoutMs: 100 });
      assert.equal(cancelTimeout.id, uncertain.id);
      assert.equal(cancelTimeout.delivery, "unknown");
      assert.equal(cancelTimeout.outcomeKnown, false);
      assert.equal(cancelTimeout.cancellation, undefined);
      dropCancellationAcks = false;
      assert.equal((await sender.cancelMessage(uncertain.id)).cancellation, "withdrawal_requested");

      const finalInbound = once(receiver, "message");
      const disconnecting = sender.send("receiver", { text: "Link lost after delivery", messageId: "disconnect-after-write" });
      await finalInbound;
      for (const socket of sockets) socket.destroy();
      const disconnected = await disconnecting;
      assert.equal(disconnected.id, "disconnect-after-write");
      assert.equal(disconnected.delivery, "unknown");
      assert.equal(disconnected.retryable, false);
      const beforeWrite = await sender.send("receiver", { text: "Not connected" });
      assert.equal(beforeWrite.delivery, "failed");
      assert.equal(beforeWrite.outcomeKnown, true);
      assert.ok(beforeWrite.id);
    } finally {
      process.env.PI_CODING_AGENT_DIR = agentDir;
      for (const socket of sockets) socket.destroy();
      proxy.close();
    }
  });
});


test("caller-owned snapshots refuse replacement endpoints while ordinary sends can address the replacement", { timeout: 30_000 }, async () => {
  await withConversationBroker(async (_agentDir, connect) => {
    const sender = await connect("sender");
    const original = await connect("reviewer");
    const snapshot = (await sender.listSessions()).find(session => session.id === original.sessionId)!;
    const received: Message[] = [];
    const replacement = await connect("reviewer", snapshot.id, client => {
      client.on("message", (_from, message) => received.push(message));
    });
    assert.notEqual(replacement.getSelfSession()?.endpointEpoch, snapshot.endpointEpoch);
    const stale = await sender.sendToSession(snapshot, { text: "Approved only for the original endpoint" });
    assert.equal(stale.code, "E_TARGET_REBOUND");
    assert.equal(stale.delivery, "failed");
    assert.equal(stale.outcomeKnown, true);
    await replacement.listSessions();
    assert.equal(received.length, 0, "a caller's recipient snapshot is never replaced by a fresh discovery");
    const current = await sender.send(snapshot.id, { text: "Contact the current endpoint" });
    assert.equal(current.delivery, "socket_delivered");
    await replacement.listSessions();
    assert.deepEqual(received.map(message => message.content.text), ["Contact the current endpoint"]);
  });
});

test("accepted supersession closes the old ask, preserves its replacement, and legacy clients can still answer", { timeout: 30_000 }, async () => {
  await withConversationBroker(async (agentDir, connect) => {
    const sender = await connect("sender");
    const receiver = await connect("receiver");
    const recoveryRecord = (id: string) => path.join(agentDir, "intercom", "pending-asks", `${encodeURIComponent(id)}.json`);
    const original = await sender.send("receiver", { text: "Review the first plan", expectsReply: true });
    assert.equal(existsSync(recoveryRecord(original.id)), true);
    const rejected = await sender.send("sender", { text: "Wrong recipient", supersedes: original.id });
    assert.equal(rejected.code, "E_SUPERSEDE_TARGET");
    assert.equal(existsSync(recoveryRecord(original.id)), true, "a rejected replacement does not close its original ask");
    const withdrawal = once(receiver, "message_control");
    const replacement = await sender.send("receiver", { text: "Review the revised plan instead", expectsReply: true, supersedes: original.id });
    assert.equal(replacement.delivery, "socket_delivered");
    const [, control] = await withdrawal as [unknown, MessageControl];
    assert.equal(control.action, "supersede");
    assert.equal(control.messageId, original.id);
    assert.equal(control.supersededBy, replacement.id);
    assert.equal(existsSync(recoveryRecord(original.id)), false, "accepted supersession ends the superseded request");
    const replay = await sender.send("receiver", { messageId: original.id, text: "Review the first plan", expectsReply: true });
    assert.equal(replay.delivery, "socket_delivered", "supersession does not rewrite accepted delivery as nondelivery");
    assert.equal(existsSync(recoveryRecord(original.id)), false, "replay does not reopen the superseded request");
    assert.equal(existsSync(recoveryRecord(replacement.id)), true, "the replacement request remains open");
    assert.equal((await receiver.send("sender", { text: "I already reviewed the first version", replyTo: original.id })).delivered, true);
    assert.equal(existsSync(recoveryRecord(replacement.id)), true, "late replies to the old question do not complete the replacement");

    // An old client advertises no conversation-contract feature and omits
    // completesAsk. Its ordinary correlated answer retains legacy semantics.
    const legacy = net.connect(getBrokerSocketPath());
    legacy.on("data", createMessageReader(frame => legacy.emit("broker_frame", frame), error => legacy.destroy(error)));
    legacy.on("error", () => undefined);
    const registered = once(legacy, "broker_frame");
    try {
      await once(legacy, "connect");
      writeMessage(legacy, { type: "register", sessionId: "legacy-client", session: baseRegistration("legacy-client") });
      assert.equal(((await registered)[0] as BrokerMessage).type, "registered");
      const legacyAsk = await sender.send("legacy-client", { text: "Can the old runtime answer?", expectsReply: true });
      assert.equal(existsSync(recoveryRecord(legacyAsk.id)), true);
      const answer = once(sender, "message");
      writeMessage(legacy, { type: "send", to: sender.sessionId, message: {
        id: "legacy-answer", timestamp: Date.now(), replyTo: legacyAsk.id, content: { text: "Yes" },
      } });
      const [, message] = await answer as [unknown, Message];
      assert.equal(message.id, "legacy-answer");
      await sender.listSessions();
      assert.equal(existsSync(recoveryRecord(legacyAsk.id)), false);
    } finally {
      legacy.destroy();
    }
  });
});

test("authorized conversation replies survive a real ask timeout and endpoint reconnect", { concurrency: false, timeout: 10_000 }, async () => {
  const agentDir = mkdtempSync(path.join(tmpdir(), "pi-intercom-reply-recovery-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  const broker = await startBroker(agentDir, { PI_INTERCOM_ASK_TIMEOUT_MS: "40" });
  const asker = new IntercomClient();
  const original = new IntercomClient();
  const restored = new IntercomClient();
  const stranger = new IntercomClient();
  try {
    await asker.connect(baseRegistration("planner"), "recovery-planner");
    await original.connect(baseRegistration("reviewer"), "recovery-reviewer");
    await stranger.connect(baseRegistration("unrelated"), "recovery-unrelated");
    const question = await asker.send("recovery-reviewer", { text: "Is the migration safe?", expectsReply: true });
    assert.equal(question.delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 70));
    await original.disconnect();
    await restored.connect(baseRegistration("reviewer"), "recovery-reviewer");
    const answerReceived = once(asker, "message") as Promise<[unknown, Message]>;
    const answer = await restored.send("recovery-planner", { text: "The migration is safe", replyTo: question.id, completesAsk: true });
    assert.equal(answer.delivered, true, "expiry of the waiting window and a new socket do not revoke conversational ownership");
    assert.equal((await answerReceived)[1].replyTo, question.id);
    const forged = await stranger.send("recovery-planner", { text: "Pretend to be the reviewer", replyTo: question.id, completesAsk: true });
    assert.equal(forged.delivered, false, "retained relationships still validate both participants");

    await asker.disconnect();
    await asker.connect(baseRegistration("planner"), "recovery-planner");
    const followUpReceived = once(restored, "message") as Promise<[unknown, Message]>;
    const followUp = await asker.send("recovery-reviewer", { text: "Thanks, which region?", replyTo: answer.id, completesAsk: false });
    assert.equal(followUp.delivered, true, "ordinary replies remain threadable after reconnect too");
    assert.equal((await followUpReceived)[1].replyTo, answer.id);
  } finally {
    await Promise.all([asker.disconnect(), original.disconnect(), restored.disconnect(), stranger.disconnect()]);
    await stopBroker(broker);
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    rmSync(agentDir, { recursive: true, force: true });
  }
});
