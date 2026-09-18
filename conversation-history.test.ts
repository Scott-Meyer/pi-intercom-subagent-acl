import test from "node:test";
import assert from "node:assert/strict";
import { messageControlKey, restoreConversationHistory } from "./conversation-history.ts";
import type { Message, MessageControl, SessionInfo } from "./types.ts";

function peer(id = "peer-1"): SessionInfo {
  return { id, cwd: "/w", model: "m", pid: 1, startedAt: 0, lastActivity: 0, name: "planner" };
}

function message(id: string, extra: Partial<Message> = {}): Message {
  return { id, content: { text: "A question with\n  context" }, timestamp: 1, ...extra };
}

function received(value: Message, from = peer()) {
  return { type: "custom", customType: "parley_inbound_received", data: { from, message: value, receivedAt: 2 } };
}

function persisted(value: Message, from = peer()) {
  return { type: "custom_message", customType: "parley_message", details: { from, message: value } };
}

function pending(id: string) {
  return { type: "custom", customType: "parley_ask_pending", data: { messageId: id, to: peer().id, targetDisplay: "planner", sentAt: 3, message: message(id).content } };
}

test("Parley journals restore received context, host persistence, and outstanding asks", () => {
  const delivered = message("delivered", { receiverReceivedAt: 4 });
  const state = restoreConversationHistory([
    received(message("queued")), persisted(delivered), pending("ask"),
  ]);
  assert.deepEqual(state.incoming.get("queued"), received(message("queued")).data);
  assert.deepEqual(state.incoming.get("delivered"), { from: peer(), message: delivered, receivedAt: 4 });
  assert.deepEqual([...state.persistedIncoming], ["delivered"]);
  assert.deepEqual(state.outgoing.get("ask"), {
    to: peer().id, targetDisplay: "planner", sentAt: 3,
    preview: "A question with context", message: message("ask").content,
  });
});

test("Parley settlements and host-persisted controls restore conversation outcomes", () => {
  const cancel: MessageControl = { messageId: "cancelled", action: "cancel", timestamp: 5 };
  const supersede: MessageControl = { messageId: "superseded", action: "supersede", supersededBy: "replacement", timestamp: 6 };
  const state = restoreConversationHistory([
    received(message("cancelled")), received(message("superseded")),
    { type: "custom", customType: "parley_inbound_control", data: { from: peer(), control: cancel } },
    { type: "custom_message", customType: "parley_message_control", details: { from: peer(), control: supersede } },
    { type: "custom", customType: "parley_inbound_settled", data: { messageId: "settled" } },
    { type: "custom", customType: "parley_sent", data: { messageId: "answer", message: { text: "Answer", replyTo: "answered", completesAsk: true } } },
    { type: "custom", customType: "parley_sent", data: { messageId: "followup", message: { text: "More context", replyTo: "still-open", completesAsk: false } } },
    pending("outgoing"),
    { type: "custom", customType: "parley_ask_settled", data: { messageId: "outgoing" } },
  ]);
  assert.deepEqual([...state.settledIncoming], ["cancelled", "superseded", "settled", "answered"]);
  assert.deepEqual([...state.controls.values()], [{ from: peer(), control: cancel }, { from: peer(), control: supersede }]);
  assert.deepEqual([...state.persistedControls], [messageControlKey(supersede)]);
  assert.equal(state.outgoing.size, 0);
});

test("only host-persisted answers from the asked peer reconcile outstanding asks", () => {
  const answer = (id: string, extra: Partial<Message> = {}) => message(`answer-${id}`, { replyTo: id, completesAsk: true, ...extra });
  const toolAnswer = answer("tool");
  const state = restoreConversationHistory([
    ...["delivered", "queued", "tool", "foreign", "followup", "question"].map(pending),
    persisted(answer("delivered")),
    received(answer("queued")),
    received(toolAnswer),
    { type: "message", message: { role: "toolResult", details: { replyMessageId: toolAnswer.id } } },
    persisted(answer("foreign"), peer("another-peer")),
    persisted(answer("followup", { completesAsk: false })),
    persisted(answer("question", { expectsReply: true })),
  ]);
  assert.deepEqual([...state.outgoing.keys()], ["queued", "foreign", "followup", "question"]);
});

test("unrelated custom entry types cannot populate or settle Parley conversations", () => {
  const value = message("foreign", { replyTo: "ask", completesAsk: true });
  const control: MessageControl = { messageId: "foreign", action: "cancel", timestamp: 5 };
  const foreignEntries = [
    received(value), persisted(value), pending("foreign-ask"),
    { type: "custom_message", customType: "parley_message_control", details: { from: peer(), control } },
    { type: "custom", customType: "parley_inbound_control", data: { from: peer(), control } },
    { type: "custom", customType: "parley_inbound_settled", data: { messageId: "foreign" } },
    { type: "custom", customType: "parley_sent", data: { message: value } },
    { type: "custom", customType: "parley_ask_settled", data: { messageId: "ask" } },
  ].flatMap((entry) => ["unrelated_", "other_"].map((prefix) => ({
    ...entry, customType: entry.customType.replace("parley_", prefix),
  })));
  const state = restoreConversationHistory([pending("ask"), ...foreignEntries]);
  assert.equal(state.incoming.size, 0);
  assert.equal(state.controls.size, 0);
  assert.equal(state.persistedIncoming.size, 0);
  assert.equal(state.persistedControls.size, 0);
  assert.equal(state.settledIncoming.size, 0);
  assert.deepEqual([...state.outgoing.keys()], ["ask"]);
});

test("recovery retains exact counterpart bindings and cannot settle an ask using a replacement incarnation", () => {
  const original = { ...peer(), endpointEpoch: "original-endpoint", federation: {
    originId: "host:planner", remoteScopeAlias: "shared", remoteStableSessionId: peer().id, originEpoch: "original-broker", conversation: true,
  } };
  const entry = pending("retained-author-qualified-handle");
  const boundPending = { ...entry, data: { ...entry.data, endpointEpoch: original.endpointEpoch, originEpoch: original.federation.originEpoch } };
  const answer = message("retained-answer-handle", { replyTo: entry.data.messageId, completesAsk: true });
  for (const other of [
    { ...original, endpointEpoch: "replacement-endpoint" },
    { ...original, federation: { ...original.federation, originEpoch: "replacement-broker" } },
    peer(), { ...original, id: "other-author" },
  ]) {
    const state = restoreConversationHistory([boundPending, persisted(answer, other)]);
    assert.equal(state.outgoing.get(entry.data.messageId)?.endpointEpoch, original.endpointEpoch);
    assert.equal(state.outgoing.get(entry.data.messageId)?.originEpoch, original.federation.originEpoch);
    assert.equal(state.outgoing.size, 1, "same thread ID alone is not authority to complete the ask");
    assert.equal(state.incoming.get(answer.id)?.message.replyTo, entry.data.messageId);
  }
  const state = restoreConversationHistory([boundPending, persisted(answer, original)]);
  assert.equal(state.outgoing.size, 0, "a host-persisted completing answer from the recorded incarnation settles the ask");
});
