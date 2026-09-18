import test from "node:test";
import assert from "node:assert/strict";
import { normalizeEntryType, restoreConversationHistory, type ConversationHistory } from "./conversation-history.ts";
import type { SessionInfo } from "./types.ts";

function peer(): SessionInfo {
  return { id: "peer-1", cwd: "/w", model: "m", pid: 1, startedAt: 0, lastActivity: 0, name: "planner" };
}

test("normalizeEntryType maps pre-1.1 journal entry types", () => {
  assert.equal(normalizeEntryType("intercom_message"), "parley_message");
  assert.equal(normalizeEntryType("intercom_ask_pending"), "parley_ask_pending");
  assert.equal(normalizeEntryType("parley_message"), "parley_message");
  assert.equal(normalizeEntryType(undefined), undefined);
  assert.equal(normalizeEntryType("unrelated"), "unrelated");
});

test("legacy custom_message journal entries replay like parley entries", () => {
  const message = { id: "m-1", content: "one", timestamp: 1 };
  const entries = [
    {
      type: "custom_message",
      customType: "intercom_message",
      details: { from: peer(), message: { ...message, receiverReceivedAt: 1 } },
    },
  ];
  const state = restoreConversationHistory(entries);
  const recovered = state.incoming.get("m-1");
  assert.equal(recovered?.message.content, "one");
  assert.equal(state.persistedIncoming.has("m-1"), true);
});

test("legacy inbound and outstanding-ask journal entries replay", () => {
  const entries = [
    {
      type: "custom",
      customType: "intercom_inbound_received",
      data: { from: peer(), message: { id: "m-2", content: "two", timestamp: 2 }, receivedAt: 2 },
    },
    {
      type: "custom",
      customType: "intercom_ask_pending",
      data: { messageId: "m-3", to: "planner", targetDisplay: "planner", sentAt: 3, message: { text: "question" } },
    },
  ];
  const state: ConversationHistory = restoreConversationHistory(entries);
  const incoming = state.incoming.get("m-2");
  assert.equal(incoming?.message.content, "two");
  const outstanding = state.outgoing.get("m-3");
  assert.equal(outstanding?.preview, "question");
});
