import test from "node:test";
import assert from "node:assert/strict";
import { ParleyClient } from "./client.ts";
import net from "node:net";
import path from "node:path";
import { once } from "node:events";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { CONVERSATION_CONTRACT_FEATURE, EXACT_SEND_FEATURE, type ClientMessage } from "../types.ts";
import { createMessageReader, writeMessage } from "./framing.ts";
import { getBrokerSocketPath } from "./paths.ts";

import { encodeOriginQualifiedSessionIdentity } from "./federation-protocol.ts";
import { isSessionId } from "./protocol.ts";

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
    features: ["session-profile-v1"],
    session,
  });
  assert.deepEqual(client.getSelfSession(), session);
  assert.equal(client.supportsFeature("session-profile-v1"), true);

  const invalidClient = new ParleyClient();
  assert.throws(
    () => (invalidClient as any).handleBrokerMessage({
      type: "registered",
      sessionId: "session-1",
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
  features: string[],
  handle: (socket: net.Socket, frame: ClientMessage) => boolean | void,
  run: (client: ParleyClient) => Promise<void>,
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
    await client.connect({ name: "test-client", cwd: "/test", model: "test", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() });
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

test("late send ACKs cannot settle cancellation; legacy cancellation ACKs remain unknown without request correlation", async () => {
  for (const modern of [false, true]) {
    let finishCancellation: (() => void) | undefined;
    await withScriptedBroker(modern ? [CONVERSATION_CONTRACT_FEATURE] : [], (socket, frame) => {
      if (frame.type !== "cancel_message") return;
      // The earlier send succeeded, but its acknowledgement arrives only after
      // cancellation starts. It says nothing about whether withdrawal happened.
      writeMessage(socket, { type: "delivered", messageId: frame.messageId });
      finishCancellation = () => writeMessage(socket, {
        type: "delivered", messageId: frame.messageId,
        ...(modern ? { requestId: frame.requestId, cancellation: "withdrawal_requested" } : {}),
      });
    }, async client => {
      const sent = await client.send("colleague", { text: "original work", timeoutMs: 30 });
      assert.equal(sent.delivery, "unknown");
      let cancellationSettled = false;
      const cancelling = client.cancelMessage(sent.id, { timeoutMs: 100 }).then(result => {
        cancellationSettled = true;
        return result;
      });
      await client.listSessions(); // ordered barrier after the original ACK
      assert.equal(cancellationSettled, false, "original send acceptance is not cancellation acceptance");
      assert.ok(finishCancellation);
      finishCancellation();
      await client.listSessions();
      assert.equal(cancellationSettled, modern, "only requestId identifies a cancellation response");
      const cancelled = await cancelling;
      assert.equal(cancelled.id, sent.id);
      assert.equal(cancelled.delivered, modern);
      assert.equal(cancelled.delivery, modern ? "socket_delivered" : "unknown");
      assert.equal(cancelled.outcomeKnown, modern);
      assert.equal(cancelled.cancellation, modern ? "withdrawal_requested" : undefined);
      if (!modern) {
        const retry = await client.send("colleague", { text: "original work", messageId: sent.id });
        assert.equal(retry.delivery, "unknown", "a late untagged cancellation ACK cannot acknowledge a later same-ID send");
        assert.equal(retry.retryable, false);
      }
    });
  }
});

test("old brokers receive ordinary messages and legacy answers, but never silently reinterpret new conversation intent", async () => {
  const sent: Extract<ClientMessage, { type: "send" }>[] = [];
  await withScriptedBroker([], (socket, frame) => {
    if (frame.type !== "send") return;
    sent.push(frame);
    writeMessage(socket, { type: "delivered", messageId: frame.message.id });
  }, async client => {
    for (const options of [
      { text: "Progress, not an answer", replyTo: "question", completesAsk: false },
      { text: "Which version?", replyTo: "question", expectsReply: true },
      { text: "Replacement request", supersedes: "question" },
    ]) {
      const result = await client.send("colleague", options);
      assert.equal(result.code, "E_CONVERSATION_CONTRACT_UNSUPPORTED");
      assert.equal(result.delivery, "failed");
      assert.equal(result.outcomeKnown, true);
      assert.equal(result.retryable, false);
      assert.ok(result.id);
    }
    const snapshot = { id: "colleague", endpointEpoch: "epoch", cwd: "/test", model: "test", pid: 1, startedAt: 1, lastActivity: 1 };
    assert.equal((await client.sendToSession(snapshot, { text: "Snapshot contact" })).code, "E_EXACT_SEND_UNSUPPORTED");
    assert.equal(sent.length, 0, "unsupported intent fails before writing any send frame");
    assert.equal((await client.send("colleague", { text: "Ordinary notification", completesAsk: false })).delivered, true);
    assert.equal((await client.send("colleague", { text: "Legacy answer", replyTo: "question" })).delivered, true);
    assert.equal((await client.send("colleague", { text: "Explicit answer", replyTo: "question", completesAsk: true })).delivered, true);
    assert.equal(sent.length, 3);
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
        : { type: "delivered", messageId: frame.message.id });
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


test("legacy cancellation makes an overlapping send unknown rather than accepting an ambiguous ACK", async () => {
  await withScriptedBroker([], (socket, frame) => {
    if (frame.type === "cancel_message") writeMessage(socket, { type: "delivered", messageId: frame.messageId });
  }, async client => {
    const sending = client.send("colleague", { text: "Original work", messageId: "overlapping-legacy", timeoutMs: 1000 });
    await client.listSessions();
    const cancelling = client.cancelMessage("overlapping-legacy", { timeoutMs: 50 });
    assert.equal((await sending).delivery, "unknown");
    assert.equal((await cancelling).delivery, "unknown");
  });
});
