import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, readdirSync, appendFileSync, statSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FederationConversations, ConversationStoreError, decodeConversationMessageId, encodeConversationMessageId,
  type ConversationEndpoint } from "./federation-conversation.ts";

const author: ConversationEndpoint = { originId: "host:author", originEpoch: "author_broker_1234", scopeAlias: "a",
  stableSessionId: "same-session", endpointEpoch: "author_endpoint_1234" };
const recipient: ConversationEndpoint = { originId: "host:recipient", originEpoch: "recipient_broker_1234", scopeAlias: "b",
  stableSessionId: "same-session", endpointEpoch: "recipient_endpoint_1234" };

test("retained handle codec authenticates its complete canonical author tuple, not a scalar nonce", () => {
  const identities = [author, recipient, { ...author, scopeAlias: "other-scope" },
    { ...author, stableSessionId: "different-author" }, { ...author, stableSessionId: " padded / α " }, { ...author, originEpoch: "new_broker_1234" },
    { ...author, endpointEpoch: "new_endpoint_1234" }].map(endpoint => ({ ...endpoint, nonce: "same_nonce_1234" }));
  const handles = identities.map(encodeConversationMessageId);
  assert.equal(new Set(handles).size, identities.length);
  for (let i = 0; i < handles.length; i++) assert.deepEqual(decodeConversationMessageId(handles[i]), identities[i]);
  for (const invalid of ["same_nonce_1234", handles[0]! + "=", "oqm1.e30", "oqm1." + "A".repeat(8192)]) {
    assert.equal(decodeConversationMessageId(invalid), undefined);
  }
});

test("durable dispatch admission never evicts unknown outcomes and fails closed at capacity across recovery", () => {
  const dir = mkdtempSync(join(tmpdir(), "conversation-barriers-"));
  try {
    const store = new FederationConversations(dir, 2);
    const first = store.prepare(author, recipient, "first_nonce_1234");
    const second = store.prepare(author, recipient, "second_nonce_1234");
    assert.equal(store.beginDispatch(first), "new");
    assert.equal(store.beginDispatch(second), "new");
    const recovered = new FederationConversations(dir, 2);
    assert.equal(recovered.hasDispatched(first), true);
    assert.equal(recovered.beginDispatch(first), "existing");
    assert.throws(() => recovered.beginDispatch("third_nonce_1234"), error => error instanceof ConversationStoreError && error.code === "E_CONVERSATION_CAPACITY");
    assert.deepEqual(readdirSync(dir), ["dispatch.log"]);
    assert.equal(recovered.beginDispatch(second), "existing");
    recovered.settleNotDelivered(first);
    const knownNegative = new FederationConversations(dir, 2);
    assert.equal(knownNegative.hasDispatched(first), false);
    assert.equal(knownNegative.beginDispatch(first), "new", "a correlated nondelivery verdict may rearm the same admitted slot");
    assert.equal(new FederationConversations(dir, 2).beginDispatch(first), "existing", "rearmed attempts are durably unknown again before any transport write");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});


test("append journal bounds known-negative rearm bytes and preserves passive dispositions while torn recovery blocks admission", () => {
  const dir = mkdtempSync(join(tmpdir(), "conversation-journal-"));
  try {
    const store = new FederationConversations(dir, 2, 400);
    store.beginDispatch("first_message");
    store.settleNotDelivered("first_message");
    assert.throws(() => store.beginDispatch("first_message"), error => error instanceof ConversationStoreError && error.code === "E_CONVERSATION_CAPACITY");
    assert.ok(statSync(join(dir, "dispatch.log")).size <= 400);
    appendFileSync(join(dir, "dispatch.log"), "torn record");
    const corrupt = new FederationConversations(dir, 2, 400);
    assert.throws(() => corrupt.beginDispatch("fresh_message"), error => error instanceof ConversationStoreError && error.code === "E_CONVERSATION_STATE_FAILURE");
    assert.equal(corrupt.hasDispatched("first_message"), false, "a torn suffix does not erase the prior confirmed nondelivery");
    assert.equal(corrupt.hasDispatched("fresh_message"), false, "passive absence does not authorize appending to a torn journal");
    assert.equal(readdirSync(dir).length, 1, "corrupt recovery never replaces the journal");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("passive lookup creates nothing and an interrupted settlement cannot erase a committed unknown", () => {
  const root = mkdtempSync(join(tmpdir(), "conversation-passive-"));
  const dir = join(root, "not-created");
  try {
    const fresh = new FederationConversations(dir);
    assert.equal(fresh.hasDispatched("original_message"), false);
    assert.equal(existsSync(dir), false, "local history checks do not enable federation storage");
    fresh.beginDispatch("original_message");
    fresh.settleNotDelivered("original_message");
    const path = join(dir, "dispatch.log");
    const complete = readFileSync(path);
    const interrupted = complete.subarray(0, complete.length - 1);
    writeFileSync(path, interrupted);
    const recovered = new FederationConversations(dir);
    assert.equal(recovered.hasDispatched("original_message"), true, "only the complete verified prefix is evidence; a torn settlement leaves unknown sticky");
    assert.equal(recovered.hasDispatched("unrelated_message"), false);
    assert.throws(() => recovered.beginDispatch("unrelated_message"), error => error instanceof ConversationStoreError && error.code === "E_CONVERSATION_STATE_FAILURE");
    assert.deepEqual(readFileSync(path), interrupted, "passive recovery never repairs or truncates evidence");
    appendFileSync(path, "\nnot-a-valid-complete-record\n");
    assert.throws(() => new FederationConversations(dir).hasDispatched("unrelated_message"),
      error => error instanceof ConversationStoreError && error.code === "E_CONVERSATION_STATE_FAILURE",
      "middle corruption or unreadable history cannot be reclassified as absence");
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("scalar associations outlive preparation eviction and recover without admitting a dispatch", () => {
  const dir = mkdtempSync(join(tmpdir(), "conversation-associations-"));
  try {
    const store = new FederationConversations(dir, 4);
    const handle = store.prepare(author, recipient, "stable_scalar_1234");
    store.bindDispatchAlias(handle, "stable_scalar_1234");
    assert.equal(store.hasDispatched(handle), false);
    assert.equal(store.hasDispatched("stable_scalar_1234"), false, "binding alone is never a dispatched attempt");
    for (let i = 0; i < 4100; i++) store.prepare(author, recipient);
    assert.equal(store.prepare(author, recipient, handle), handle);
    assert.equal(store.beginDispatch(handle), "new", "first dispatch recovers the durable association despite unrelated preparation pressure");
    assert.equal(store.hasDispatched("stable_scalar_1234"), true);
    const recovered = new FederationConversations(dir, 4);
    recovered.settleNotDelivered(handle);
    assert.equal(recovered.hasDispatched("stable_scalar_1234"), false);
    const retried = new FederationConversations(dir, 4);
    assert.equal(retried.beginDispatch(handle), "new", "canonical retry does not drop its scalar partner after confirmed nondelivery");
    assert.equal(new FederationConversations(dir, 4).hasDispatched("stable_scalar_1234"), true);
    assert.equal(retried.beginDispatch("new_author_handle", "stable_scalar_1234"), "existing", "a new author incarnation cannot re-admit an unknown caller identity");
    assert.throws(() => retried.bindDispatchAlias(handle, "different_scalar_1234"), /association cannot be changed/);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
