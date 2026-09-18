import test from "node:test";
import assert from "node:assert/strict";
import { encodeConversationMessageId } from "./federation-conversation.ts";
import {
  FEDERATION_SEND_TEXT_MAX_LENGTH,
  PeerSendDedup,
  PendingPeerSendTracker,
  isPeerSendRequest,
  isPeerSendResult,
} from "./federation-send.ts";

function validRequest(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: "peer_send",
    protocol: "pi-parley-peer",
    version: 1,
    originId: "host:penguin",
    sendId: "send_0001_ABCDEFGH",
    senderScopeAlias: "mistfall-remote",
    senderStableSessionId: "remote / stable session",
    targetScopeAlias: "macbook-local",
    targetStableSessionId: "0199-stable-target",
    message: {
      id: "msg_0001_ABCDEFGH",
      timestamp: 1789580000000,
      text: "hello from the other broker",
    },
    ...overrides,
  };
}

function validResult(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: "peer_send_result",
    protocol: "pi-parley-peer",
    version: 1,
    originId: "host:penguin",
    sendId: "send_0001_ABCDEFGH",
    ok: true,
    deliveredAt: 1789580000123,
    ...overrides,
  };
}

test("peer send requests accept the canonical frame and reject every mutation of it", () => {
  assert.equal(isPeerSendRequest(validRequest()), true, "legacy ordinary send frames remain valid");
  assert.equal(isPeerSendRequest(validRequest({ targetEndpointEpoch: "epoch_12345678" })), true);

  for (const mutation of [
    { protocol: "other" },
    { version: 2 },
    { originId: "Penguin" },
    { originId: "not canonical!" },
    { sendId: "short" },
    { sendId: 12 },
    { senderScopeAlias: "Bad Alias" },
    { senderScopeAlias: "" },
    { senderStableSessionId: "" },
    { senderStableSessionId: `id-${"x".repeat(600)}` },
    { senderStableSessionId: "padded " },
    { targetScopeAlias: "Bad Alias" },
    { targetStableSessionId: "unsafe\nid" },
    { targetEndpointEpoch: "short" },
    { targetEndpointEpoch: 12 },
    { message: { id: "msg_0001_ABCDEFGH", timestamp: 1 } },
    { message: { id: "msg_0001_ABCDEFGH", timestamp: -1, text: "x" } },
    { message: { id: "msg_0001_ABCDEFGH", timestamp: 1.5, text: "x" } },
    { message: { id: "short", timestamp: 1, text: "x" } },
    { message: { id: "msg_0001_ABCDEFGH", timestamp: 1, text: `x`.repeat(FEDERATION_SEND_TEXT_MAX_LENGTH + 1) } },
    { message: { id: "msg_0001_ABCDEFGH", timestamp: 1, text: "x", extra: true } },
    { extra: true },
  ]) {
    assert.equal(
      isPeerSendRequest(validRequest(mutation)),
      false,
      `expected rejection for ${JSON.stringify(mutation)}`,
    );
  }
});

test("peer send results require correlated identity, a verdict, and strict failure payloads", () => {
  assert.equal(isPeerSendResult(validResult()), true);
  assert.equal(isPeerSendResult(validResult({ ok: false, code: "E_SEND_TARGET_REBOUND", error: "Target endpoint changed before delivery" })), true);
  assert.equal(isPeerSendResult(validResult({ ok: false, code: "E_SEND_TARGET_NOT_FOUND", error: "Session not found" })), true);

  for (const mutation of [
    { ok: true, deliveredAt: -1 },
    { ok: true, deliveredAt: 1.5 },
    { ok: false },
    { ok: false, code: "E_SEND_TARGET_NOT_FOUND" },
    { ok: false, code: "E_NOT_A_SEND_CODE", error: "x" },
    { ok: false, code: "E_SEND_INVALID", error: "" },
    { ok: false, code: "E_SEND_INVALID", error: `x`.repeat(257) },
    { ok: false, code: "E_SEND_INVALID", error: "bad\nerror" },
    { ok: "yes" },
  ]) {
    assert.equal(
      isPeerSendResult(validResult(mutation)),
      false,
      `expected rejection for ${JSON.stringify(mutation)}`,
    );
  }
});

test("peer send dedup observes each id once and evicts beyond capacity", () => {
  const dedup = new PeerSendDedup(3);
  assert.equal(dedup.observe("a"), true);
  assert.equal(dedup.observe("b"), true);
  assert.equal(dedup.observe("c"), true);
  assert.equal(dedup.size, 3);
  assert.equal(dedup.observe("a"), false, "replayed ids are duplicates");
  assert.equal(dedup.observe("d"), true, "new ids are accepted");
  assert.equal(dedup.size, 3, "capacity is enforced");
  assert.equal(dedup.observe("a"), true, "the oldest id left the bounded window");
});

test("pending peer sends resolve once, drop with their link, and expire on deadline", () => {
  const tracker = new PendingPeerSendTracker(2, 1000);
  const now = 1789580000000;
  assert.ok(tracker.add("send_0001_ABCDEFGH", { linkId: "link-1", messageId: "msg-1", senderKey: "sender-1", fingerprint: "fp-1" }, now));
  assert.ok(tracker.add("send_0002_ABCDEFGH", { linkId: "link-1", messageId: "msg-2", senderKey: "sender-1", fingerprint: "fp-2" }, now));
  assert.equal(tracker.add("send_0001_ABCDEFGH", { linkId: "link-1", messageId: "msg-3", senderKey: "sender-1", fingerprint: "fp-3" }, now), undefined, "duplicate correlation ids are refused");
  assert.equal(tracker.add("send_0003_ABCDEFGH", { linkId: "link-2", messageId: "msg-3", senderKey: "sender-2", fingerprint: "fp-3" }, now), undefined, "capacity is enforced");

  const resolved = tracker.resolve("send_0001_ABCDEFGH");
  assert.equal(resolved?.messageId, "msg-1");
  assert.equal(resolved?.fingerprint, "fp-1");
  assert.equal(resolved?.linkId, "link-1");
  assert.equal(tracker.resolve("send_0001_ABCDEFGH"), undefined, "entries resolve exactly once");

  const dropped = tracker.dropLink("link-1");
  assert.equal(dropped.length, 1);
  assert.equal(dropped[0]?.messageId, "msg-2");
  assert.equal(tracker.size, 0);

  assert.ok(tracker.add("send_0004_ABCDEFGH", { linkId: "link-3", messageId: "msg-4", senderKey: "sender-3", fingerprint: "fp-4" }, now));
  assert.equal(tracker.expire(now + 999).length, 0, "not yet expired");
  const expired = tracker.expire(now + 1001);
  assert.equal(expired.length, 1);
  assert.equal(expired[0]?.messageId, "msg-4");
  assert.equal(tracker.size, 0);
});


test("conversation envelopes require canonical retained IDs, both endpoint pins, and independently typed reply intent", () => {
  const id = encodeConversationMessageId({ originId: "host:penguin", originEpoch: "origin_epoch_1234",
    scopeAlias: "mistfall-remote", stableSessionId: "remote / stable session", endpointEpoch: "sender_epoch_1234", nonce: "message_nonce_1234" });
  const request = validRequest({ senderOriginEpoch: "origin_epoch_1234", senderEndpointEpoch: "sender_epoch_1234",
    targetOriginEpoch: "target_origin_1234", targetEndpointEpoch: "target_endpoint_1234",
    message: { id, timestamp: 1789580000000, text: "threaded question", replyTo: id, expectsReply: true, completesAsk: false, senderWaitMode: "nonblocking" },
  });
  assert.equal(isPeerSendRequest(request), true);
  for (const key of ["senderOriginEpoch", "senderEndpointEpoch", "targetOriginEpoch", "targetEndpointEpoch"]) {
    const missing = { ...request };
    delete missing[key];
    assert.equal(isPeerSendRequest(missing), false, `missing ${key}`);
  }
  for (const message of [
    { ...(request.message as object), id: "scalar_nonce_1234" },
    { ...(request.message as object), replyTo: "scalar_nonce_1234" },
    { ...(request.message as object), expectsReply: "true" },
    { ...(request.message as object), completesAsk: 1 },
    { ...(request.message as object), senderWaitMode: "waiting" },
  ]) assert.equal(isPeerSendRequest({ ...request, message }), false);
  assert.equal(isPeerSendRequest(validRequest({ message: { id: "scalar_nonce_1234", timestamp: 1, text: "legacy", expectsReply: false } })), false);
});
