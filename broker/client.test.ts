import test from "node:test";
import assert from "node:assert/strict";
import { ParleyClient } from "./client.ts";
import net from "node:net";
import path from "node:path";
import { once } from "node:events";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { COMPACTION_AWARENESS_FEATURE, CONVERSATION_CONTRACT_FEATURE, EXACT_SEND_FEATURE, type ClientMessage } from "../types.ts";
import { createMessageReader, writeMessage } from "./framing.ts";
import { getBrokerSocketPath } from "./paths.ts";

import { encodeOriginQualifiedSessionIdentity } from "./federation-protocol.ts";
import { isSessionId } from "./protocol.ts";

const REQUIRED_FEATURES = [CONVERSATION_CONTRACT_FEATURE, EXACT_SEND_FEATURE];
const TEST_REGISTRATION = { name: "test-client", cwd: "/test", model: "test", pid: process.pid, startedAt: 1, lastActivity: 1 };
const accepted = (messageId: string) => ({ type: "delivered", messageId, delivery: "socket_delivered", retryable: false, outcomeKnown: true });

test("validated session lifecycle messages reach broker-message subscribers", () => {
  const client = new ParleyClient();
  (client as any)._sessionId = "session-1";
  const received: unknown[] = [];
  client.onBrokerMessage((message) => received.push(message));
  const session = {
    id: "session-2",
    cwd: "/test",
    model: "test",
    pid: 2,
    startedAt: 1,
    lastActivity: 1,
  };

  (client as any).handleBrokerMessage({ type: "session_joined", session });
  (client as any).handleBrokerMessage({ type: "presence_update", session });
  (client as any).handleBrokerMessage({ type: "session_left", sessionId: "session-2" });

  assert.deepEqual(received, [
    { type: "session_joined", session },
    { type: "presence_update", session },
    { type: "session_left", sessionId: "session-2" },
  ]);
});

test("federated session lifecycle metadata is accepted only for canonical provenance", () => {
  const client = new ParleyClient();
  (client as any)._sessionId = "session-1";
  const federation = {
    originId: "host:penguin",
    originLabel: "Penguin",
    remoteScopeAlias: "mistfall-remote",
    remoteStableSessionId: "remote / session",
  };
  const session = {
    id: encodeOriginQualifiedSessionIdentity({
      originId: federation.originId,
      remoteScopeAlias: federation.remoteScopeAlias,
      remoteStableSessionId: federation.remoteStableSessionId,
    }),
    name: "Remote Specialist",
    cwd: "/remote/test",
    model: "test",
    pid: 2,
    startedAt: 1,
    lastActivity: 1,
    trustedLocal: false,
    federation,
  };
  assert.doesNotThrow(() => (client as any).handleBrokerMessage({ type: "session_joined", session }));
  for (const forged of [
    { ...session, federation: { ...session.federation, linkId: "transient-link" } },
    { ...session, trustedLocal: true },
    { ...session, id: "oqs1.not-the-canonical-tuple" },
  ]) {
    assert.throws(
      () => (client as any).handleBrokerMessage({ type: "session_joined", session: forged }),
      /Invalid session_joined/,
    );
  }
});

test("local registration IDs cannot collide with the federated identity namespace", () => {
  assert.equal(isSessionId("ordinary stable / id"), true);
  assert.equal(isSessionId("oqs1.attacker-controlled"), false);
  assert.equal(isSessionId(`id-${"x".repeat(510)}`), false);
  assert.equal(isSessionId("unsafe\nidentity"), false);
});

test("registered feature negotiation rejects non-string feature entries", () => {
  const client = new ParleyClient();
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "registered", sessionId: "session-1", features: ["valid", 123] }),
    /Invalid registered features/,
  );
});

test("registered handshake exposes the broker-owned self projection", () => {
  const client = new ParleyClient();
  const session = {
    id: "session-1",
    name: "worker-2",
    description: "Reviewing broker owned self profile projection",
    cwd: "/test",
    model: "test",
    pid: 1,
    startedAt: 1,
    lastActivity: 1,
  };

  (client as any).handleBrokerMessage({
    type: "registered",
    sessionId: "session-1",
    features: [...REQUIRED_FEATURES, "session-profile-v1"],
    session,
  });
  assert.deepEqual(client.getSelfSession(), session);
  assert.equal(client.supportsFeature("session-profile-v1"), true);

  const invalidClient = new ParleyClient();
  assert.throws(
    () => (invalidClient as any).handleBrokerMessage({
      type: "registered",
      sessionId: "session-1",
      features: REQUIRED_FEATURES,
      session: { ...session, id: "different-session" },
    }),
    /Invalid registered session/,
  );
});

test("malformed extension broker messages are rejected", () => {
  const client = new ParleyClient();
  (client as any)._sessionId = "session-1";

  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "extension_owner", namespace: "test/v1", ownerId: "owner" }),
    /Invalid extension_owner/,
  );
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "extension_owner", namespace: "test/v1", ownerEpoch: "epoch" }),
    /Invalid extension_owner/,
  );
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "extension_message", namespace: "test/v1" }),
    /Invalid extension_message/,
  );
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "extension_state", namespace: "test/v1", revision: -1 }),
    /Invalid extension_state/,
  );
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "extension_state_result", namespace: "test/v1", committed: "yes", revision: 1 }),
    /Invalid extension_state_result/,
  );
  assert.throws(
    () => (client as any).handleBrokerMessage({
      type: "delivered",
      messageId: "message-1",
      delivery: "socket_delivered",
      retryable: false,
      outcomeKnown: true,
      peerCompaction: {
        peerSessionId: "peer-1",
        generation: 1,
        previousGeneration: 1,
        compactedAt: 1,
      },
    }),
    /Invalid delivered message/,
  );
  assert.doesNotThrow(() => (client as any).handleBrokerMessage({
    type: "extension_message",
    namespace: "test/v1",
    fromSessionId: "session-2",
    payload: { peerOnly: true },
  }));
});

test("cancelAsk ignores synchronous socket write failures", () => {
  const client = new ParleyClient();
  (client as any)._sessionId = "session-1";
  (client as any).socket = {
    destroyed: false,
    writableEnded: false,
    writable: true,
    write() {
      throw new Error("write failed");
    },
  };

  assert.doesNotThrow(() => client.cancelAsk("ask-1"));
});


async function withScriptedBroker(
  features: string[] | undefined,
  handle: (socket: net.Socket, frame: ClientMessage) => boolean | void,
  run: (client: ParleyClient) => Promise<void>,
  options: { connect?: boolean } = {},
): Promise<void> {
  const agentDir = mkdtempSync(path.join(process.platform === "win32" ? tmpdir() : "/tmp", "pi-wire-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  const socketPath = getBrokerSocketPath();
  mkdirSync(path.dirname(socketPath), { recursive: true });
  const sockets: net.Socket[] = [];
  const server = net.createServer(socket => {
    sockets.push(socket);
    socket.on("data", createMessageReader(value => {
      const frame = value as ClientMessage;
      if (frame.type === "register") {
        writeMessage(socket, { type: "registered", sessionId: "test-client", features });
      } else if (frame.type === "list") {
        if (!handle(socket, frame)) writeMessage(socket, { type: "sessions", requestId: frame.requestId, sessions: [] });
      } else if (frame.type === "unregister") {
        socket.end();
      } else {
        handle(socket, frame);
      }
    }, error => socket.destroy(error)));
    socket.on("error", () => undefined);
  });
  const client = new ParleyClient();
  try {
    server.listen(socketPath);
    await once(server, "listening");
    if (options.connect !== false) await client.connect(TEST_REGISTRATION);
    await run(client);
  } finally {
    await client.disconnect().catch(() => undefined);
    for (const socket of sockets) socket.destroy();
    await new Promise<void>(resolve => server.close(() => resolve()));
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    rmSync(agentDir, { recursive: true, force: true });
  }
}

test("connection requires the current contract but optional features remain negotiable", async () => {
  for (const features of [undefined, [], [EXACT_SEND_FEATURE], [CONVERSATION_CONTRACT_FEATURE]]) {
    await withScriptedBroker(features, () => undefined, async client => {
      await assert.rejects(client.connect(TEST_REGISTRATION), /Invalid registered features|Missing required broker feature/);
      assert.equal(client.isConnected(), false);
      assert.equal(client.sessionId, null);
    }, { connect: false });
  }
  await withScriptedBroker(REQUIRED_FEATURES, () => undefined, async client => {
    assert.equal(client.supportsFeature(COMPACTION_AWARENESS_FEATURE), false);
    await assert.rejects(client.reportCompactionCompleted("compaction"), /unavailable/);
    client.updateExtensionCapabilities([]);
  });
});

test("late send ACKs cannot settle cancellation, and cancellation acceptance is not original nondelivery", async () => {
  let finishCancellation: (() => void) | undefined;
  await withScriptedBroker(REQUIRED_FEATURES, (socket, frame) => {
    if (frame.type !== "cancel_message") return;
    writeMessage(socket, accepted(frame.messageId));
    finishCancellation = () => writeMessage(socket, {
      ...accepted(frame.messageId), requestId: frame.requestId, cancellation: "withdrawal_requested",
    });
  }, async client => {
    const sent = await client.send("colleague", { text: "original work", timeoutMs: 30 });
    assert.equal(sent.delivery, "unknown");
    let cancellationSettled = false;
    const cancelling = client.cancelMessage(sent.id, { timeoutMs: 1000 }).then(result => {
      cancellationSettled = true;
      return result;
    });
    await client.listSessions();
    assert.equal(cancellationSettled, false, "original send acceptance is not cancellation acceptance");
    assert.ok(finishCancellation);
    finishCancellation();
    const cancelled = await cancelling;
    assert.equal(cancelled.id, sent.id);
    assert.equal(cancelled.delivered, true, "withdrawal notice accepted, not earlier work undone");
    assert.equal(cancelled.delivery, "socket_delivered");
    assert.equal(cancelled.outcomeKnown, true);
    assert.equal(cancelled.cancellation, "withdrawal_requested");
  });
});

test("late cancellation ACK cannot settle a later same-ID send or another cancellation", async () => {
  let lateCancellation: (() => void) | undefined;
  let finishSend: (() => void) | undefined;
  let finishCancellation: (() => void) | undefined;
  await withScriptedBroker(REQUIRED_FEATURES, (socket, frame) => {
    if (frame.type === "cancel_message") {
      const finish = () => writeMessage(socket, {
        ...accepted(frame.messageId), requestId: frame.requestId, cancellation: "withdrawal_requested",
      });
      if (!lateCancellation) lateCancellation = finish;
      else finishCancellation = finish;
    }
    if (frame.type === "send") finishSend = () => writeMessage(socket, accepted(frame.message.id));
  }, async client => {
    const timedOut = await client.cancelMessage("reused-id", { timeoutMs: 30 });
    assert.equal(timedOut.delivery, "unknown");
    assert.equal(timedOut.code, "E_CANCELLATION_UNKNOWN");
    assert.equal(timedOut.outcomeKnown, false);
    let sendSettled = false;
    let cancelSettled = false;
    const sending = client.send("colleague", { text: "work", messageId: "reused-id", timeoutMs: 1000 })
      .then(result => { sendSettled = true; return result; });
    const cancelling = client.cancelMessage("reused-id", { timeoutMs: 1000 })
      .then(result => { cancelSettled = true; return result; });
    await client.listSessions();
    assert.ok(lateCancellation);
    lateCancellation();
    await client.listSessions();
    assert.equal(sendSettled, false);
    assert.equal(cancelSettled, false);
    assert.ok(finishSend);
    assert.ok(finishCancellation);
    finishSend();
    finishCancellation();
    assert.equal((await sending).delivered, true);
    assert.equal((await cancelling).cancellation, "withdrawal_requested");
  });
});

test("current conversation intent is authored without reinterpretation", async () => {
  const sent: Extract<ClientMessage, { type: "send" }>[] = [];
  await withScriptedBroker(REQUIRED_FEATURES, (socket, frame) => {
    if (frame.type !== "send") return;
    sent.push(frame);
    writeMessage(socket, accepted(frame.message.id));
  }, async client => {
    const intents = [
      { text: "Progress, not an answer", replyTo: "question", completesAsk: false },
      { text: "Which version?", replyTo: "question", expectsReply: true },
      { text: "Replacement request", supersedes: "question" },
      { text: "Explicit answer", replyTo: "question", completesAsk: true },
    ];
    for (const intent of intents) assert.equal((await client.send("colleague", intent)).delivered, true);
    assert.equal(sent.length, intents.length);
    for (let i = 0; i < intents.length; i++) {
      const { text, ...metadata } = intents[i]!;
      assert.equal(sent[i]!.message.content.text, text);
      for (const [key, value] of Object.entries(metadata)) assert.equal((sent[i]!.message as any)[key], value);
    }
  });
});

test("ordinary send may re-resolve an endpoint rebound but sendToSession never retries its caller-owned snapshot", async () => {
  const snapshot = { id: "reviewer", name: "reviewer", endpointEpoch: "original-epoch", cwd: "/test", model: "test", pid: 2, startedAt: 1, lastActivity: 1 };
  const epochs: string[] = [];
  let lists = 0;
  await withScriptedBroker([EXACT_SEND_FEATURE, CONVERSATION_CONTRACT_FEATURE], (socket, frame) => {
    if (frame.type === "list") {
      lists += 1;
      writeMessage(socket, { type: "sessions", requestId: frame.requestId, sessions: [{ ...snapshot, endpointEpoch: lists === 1 ? "original-epoch" : "replacement-epoch" }] });
      return true;
    }
    if (frame.type === "send") {
      epochs.push(frame.targetEpoch!);
      writeMessage(socket, frame.targetEpoch === "original-epoch"
        ? { type: "delivery_failed", messageId: frame.message.id, delivery: "failed", outcomeKnown: true, retryable: true, code: "E_TARGET_REBOUND", reason: "Endpoint was replaced" }
        : accepted(frame.message.id));
    }
  }, async client => {
    assert.equal((await client.sendToSession(snapshot, { text: "Snapshot contact" })).code, "E_TARGET_REBOUND");
    assert.deepEqual(epochs, ["original-epoch"]);
    assert.equal(lists, 0, "caller-owned snapshots do not cause discovery, even after rejection");
    assert.equal((await client.send("reviewer", { text: "Current endpoint contact" })).delivered, true);
    assert.equal(lists, 2, "ordinary send refreshes a stale discovery once");
    assert.deepEqual(epochs, ["original-epoch", "original-epoch", "replacement-epoch"]);
  });
});


test("invalid ACKs close the connection without accepting unrelated operations", async t => {
  const cases: [string, Record<string, unknown>, RegExp][] = [
    ["missing delivery", { delivery: undefined }, /Invalid delivered/],
    ["invalid retryability", { retryable: true }, /Invalid delivered/],
    ["invalid known outcome", { outcomeKnown: false }, /Invalid delivered/],
    ["invalid request ID", { requestId: 7 }, /Invalid delivery requestId/],
    ["empty request ID", { requestId: "" }, /Invalid delivery requestId/],
    ["wrong operation", { cancellation: undefined }, /Invalid cancellation acknowledgement/],
    ["queued cancellation", { delivery: "queued" }, /Invalid cancellation acknowledgement/],
    ["failed but confirmed cancellation", { type: "delivery_failed", reason: "failed", delivery: "failed" }, /Invalid cancellation acknowledgement/],
    ["wrong message correlation", { messageId: "another-message" }, /does not match requestId/],
    ["invalid cancellation", { cancellation: "undone" }, /Invalid cancellation state/],
    ["invalid recipient", { recipient: { id: "peer" } }, /Invalid delivery recipient/],
    ["send-only metadata", { contactToken: "contact" }, /Invalid cancellation acknowledgement/],
    ["invalid code", { code: 123 }, /Invalid delivery code/],
    ["retryable uncertainty", { type: "delivery_failed", reason: "uncertain", delivery: "unknown", retryable: true, outcomeKnown: false }, /Invalid delivery_failed/],
    ["false certainty", { type: "delivery_failed", reason: "uncertain", delivery: "unknown", outcomeKnown: true }, /Invalid delivery_failed/],
  ];
  for (const [name, patch, expected] of cases) {
    await t.test(name, async () => {
      await withScriptedBroker(REQUIRED_FEATURES, (socket, frame) => {
        if (frame.type === "cancel_message") writeMessage(socket, {
          ...accepted(frame.messageId), requestId: frame.requestId, cancellation: "withdrawal_requested", ...patch,
        });
      }, async client => {
        const errors: Error[] = [];
        client.on("error", error => errors.push(error));
        const sending = client.send("colleague", { text: "work", replyTo: "question", messageId: "pending-send" });
        const cancelling = client.cancelMessage("pending-send");
        const listing = assert.rejects(client.listSessions(), expected);
        const [send, cancel] = await Promise.all([sending, cancelling]);
        await listing;
        assert.equal(send.delivery, "unknown");
        assert.equal(send.outcomeKnown, false);
        assert.equal(cancel.delivery, "unknown");
        assert.equal(cancel.outcomeKnown, false);
        assert.equal(client.isConnected(), false);
        assert.match(errors[0]!.message, expected);
      });
    });
  }
});

test("connection failure settles every pending call, including without an error subscriber", async t => {
  for (const failure of ["close", "protocol"] as const) {
    await t.test(failure, async () => {
      let frames = 0;
      await withScriptedBroker([...REQUIRED_FEATURES, COMPACTION_AWARENESS_FEATURE], (socket) => {
        if (++frames === 5) {
          if (failure === "close") socket.destroy();
          else writeMessage(socket, { type: "not-a-broker-operation" });
        }
        return true;
      }, async client => {
        const sending = client.send("colleague", { text: "work", replyTo: "question", messageId: "pending-send" });
        const cancelling = client.cancelMessage("pending-send");
        const settled = Promise.allSettled([
          client.listSessions(), client.advertise("new-name"), client.reportCompactionCompleted("event"),
        ]);
        const [send, cancel, calls] = await Promise.all([sending, cancelling, settled]);
        assert.equal(send.delivery, "unknown");
        assert.equal(send.id, "pending-send");
        assert.equal(cancel.delivery, "unknown");
        assert.equal(cancel.id, "pending-send");
        assert.equal(calls.every(result => result.status === "rejected"), true);
        assert.equal(client.isConnected(), false);
      });
    });
  }
});

test("receipts and controls preserve their authored message correlation without settling send acceptance", async () => {
  const peer = { id: "peer", cwd: "/test", model: "test", pid: 2, startedAt: 1, lastActivity: 1 };
  const receipt = { messageId: "controlled-message", status: "cancellation_requested", timestamp: 1 } as const;
  const control = { messageId: "controlled-message", action: "supersede", supersededBy: "replacement-message", timestamp: 2 } as const;
  let acceptSend: (() => void) | undefined;
  await withScriptedBroker(REQUIRED_FEATURES, (socket, frame) => {
    if (frame.type !== "send") return;
    writeMessage(socket, { type: "message_receipt", from: peer, receipt });
    writeMessage(socket, { type: "message_control", from: peer, control });
    acceptSend = () => writeMessage(socket, { ...accepted(frame.message.id), recipient: peer, cancellation: "withdrawal_requested" });
  }, async client => {
    const received: unknown[] = [];
    client.onMessageReceipt((from, value) => received.push([from, value]));
    client.onMessageControl((from, value) => received.push([from, value]));
    let sendSettled = false;
    const sending = client.send("peer", { text: "work", replyTo: "question", messageId: receipt.messageId })
      .then(result => { sendSettled = true; return result; });
    await client.listSessions();
    assert.deepEqual(received, [[peer, receipt], [peer, control]]);
    assert.equal(sendSettled, false);
    assert.ok(acceptSend);
    acceptSend();
    const result = await sending;
    assert.equal(result.delivered, true, "stored withdrawal metadata does not reverse original send acceptance");
    assert.equal(result.cancellation, "withdrawal_requested");
    assert.deepEqual(result.recipient, peer);
  });
});

test("correlated cancellation failure preserves its own uncertainty and cannot fail an overlapping send", async () => {
  for (const delivery of ["failed", "unknown"] as const) {
    let acceptSend: (() => void) | undefined;
    await withScriptedBroker(REQUIRED_FEATURES, (socket, frame) => {
      if (frame.type === "send") acceptSend = () => writeMessage(socket, accepted(frame.message.id));
      if (frame.type === "cancel_message") writeMessage(socket, {
        type: "delivery_failed", messageId: frame.messageId, requestId: frame.requestId,
        reason: "Withdrawal unavailable", code: "E_CANCELLATION_UNAVAILABLE",
        delivery, outcomeKnown: delivery === "failed", retryable: false,
      });
    }, async client => {
      let sendSettled = false;
      const sending = client.send("peer", { text: "work", replyTo: "question", messageId: "overlapping-send" })
        .then(result => { sendSettled = true; return result; });
      const cancelled = await client.cancelMessage("overlapping-send");
      assert.equal(cancelled.id, "overlapping-send");
      assert.equal(cancelled.delivered, false);
      assert.equal(cancelled.delivery, delivery);
      assert.equal(cancelled.outcomeKnown, delivery === "failed");
      assert.equal(cancelled.retryable, false);
      assert.equal(sendSettled, false);
      assert.ok(acceptSend);
      acceptSend();
      assert.equal((await sending).delivered, true);
    });
  }
});
