import test from "node:test";
import assert from "node:assert/strict";
import { IntercomClient } from "./client.ts";

test("validated session lifecycle messages reach broker-message subscribers", () => {
  const client = new IntercomClient();
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

test("registered feature negotiation rejects non-string feature entries", () => {
  const client = new IntercomClient();
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "registered", sessionId: "session-1", features: ["valid", 123] }),
    /Invalid registered features/,
  );
});

test("registered handshake exposes the broker-owned self projection", () => {
  const client = new IntercomClient();
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

  const invalidClient = new IntercomClient();
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
  const client = new IntercomClient();
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
  const client = new IntercomClient();
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
