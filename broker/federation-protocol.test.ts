import test from "node:test";
import assert from "node:assert/strict";
import {
  decodeOriginQualifiedSessionIdentity,
  encodeOriginQualifiedSessionIdentity,
  isBrokerAcceptPeerRequest,
  isBrokerAcceptPeerResult,
  isBrokerDialPeerRequest,
  isBrokerDialPeerResult,
  isFederationBridgeAttach,
  isPeerHello,
  isPeerHelloAck,
  scopeMappingsFromBindings,
} from "./federation-protocol.ts";
import {
  FEDERATION_IDENTITY_FEATURE,
  FEDERATION_PROTOCOL_NAME,
  FEDERATION_PROTOCOL_VERSION,
  FEDERATION_ROSTER_FEATURE,
  FEDERATION_SESSION_ID_MAX_LENGTH,
  FEDERATION_SINGLE_HOP_FEATURE,
} from "./federation-types.ts";

const capability = "A".repeat(32);
const features = [FEDERATION_IDENTITY_FEATURE, FEDERATION_SINGLE_HOP_FEATURE];
const localOrigin = { id: "host:macbook", label: "Scott's MacBook" };
const remoteOrigin = { id: "host:penguin", label: "Penguin" };
const scopeBindings = [{ localScopeId: "mistfall-private", localScopeAlias: "mistfall-local", remoteScopeAlias: "mistfall-remote" }];
const scopeMappings = scopeMappingsFromBindings(scopeBindings);

function validDialRequest() {
  return {
    type: "broker_dial_peer",
    requestId: "request_12345678",
    endpoint: { transport: "tcp", host: "127.0.0.1", port: 43123 },
    capability,
    localOrigin,
    remoteOrigin,
    scopeBindings,
  };
}

function validAcceptRequest() {
  return {
    type: "broker_accept_peer",
    requestId: "request_87654321",
    linkId: "link_12345678",
    localOrigin: remoteOrigin,
    remoteOrigin: localOrigin,
    scopeBindings: [{ localScopeId: "remote-private", localScopeAlias: "mistfall-remote", remoteScopeAlias: "mistfall-local" }],
  };
}

function validHello() {
  return {
    type: "peer_hello",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    linkId: "link_12345678",
    origin: localOrigin,
    expectedPeerOrigin: remoteOrigin,
    scopeMappings,
    features,
  };
}

test("origin-qualified session identities round-trip arbitrary bounded stable IDs", () => {
  const first = { originId: "host:a", remoteScopeAlias: "bc", remoteStableSessionId: "unicode / session α" };
  const second = { originId: "host:ab", remoteScopeAlias: "c", remoteStableSessionId: "unicode / session α" };
  const firstEncoded = encodeOriginQualifiedSessionIdentity(first);
  const secondEncoded = encodeOriginQualifiedSessionIdentity(second);
  assert.notEqual(firstEncoded, secondEncoded);
  assert.match(firstEncoded, /^oqs1\.[A-Za-z0-9_-]+$/);
  assert.deepEqual(decodeOriginQualifiedSessionIdentity(firstEncoded), first);
  assert.deepEqual(decodeOriginQualifiedSessionIdentity(secondEncoded), second);

  const escapedMaximum = {
    originId: "host:penguin",
    remoteScopeAlias: "mistfall-remote",
    remoteStableSessionId: "\ud800".repeat(FEDERATION_SESSION_ID_MAX_LENGTH),
  };
  const escapedEncoded = encodeOriginQualifiedSessionIdentity(escapedMaximum);
  assert.deepEqual(decodeOriginQualifiedSessionIdentity(escapedEncoded), escapedMaximum);
});

test("origin-qualified identity decoding rejects noncanonical and unsafe values", () => {
  const valid = encodeOriginQualifiedSessionIdentity({
    originId: "host:penguin",
    remoteScopeAlias: "mistfall-remote",
    remoteStableSessionId: "mistfall-remote:game:t133",
  });
  assert.equal(decodeOriginQualifiedSessionIdentity(`${valid}=`), undefined);
  assert.equal(decodeOriginQualifiedSessionIdentity(valid.replace("oqs1.", "oqs2.")), undefined);
  assert.equal(decodeOriginQualifiedSessionIdentity("oqs1.bm90LWpzb24"), undefined);
  assert.throws(() => encodeOriginQualifiedSessionIdentity({
    originId: "HOST:Penguin",
    remoteScopeAlias: "mistfall-remote",
    remoteStableSessionId: "session-1",
  }), /Invalid origin-qualified session identity/);
  assert.throws(() => encodeOriginQualifiedSessionIdentity({
    originId: "host:penguin",
    remoteScopeAlias: "mistfall-remote",
    remoteStableSessionId: "s".repeat(FEDERATION_SESSION_ID_MAX_LENGTH + 1),
  }), /Invalid origin-qualified session identity/);
});

test("dial and accept controls bind local raw scopes but expose only aliases to peers", () => {
  const dial = validDialRequest();
  const accept = validAcceptRequest();
  assert.equal(isBrokerDialPeerRequest(dial), true);
  assert.equal(isBrokerAcceptPeerRequest(accept), true);
  assert.deepEqual(scopeMappings, [{ localScopeAlias: "mistfall-local", remoteScopeAlias: "mistfall-remote" }]);
  assert.equal(JSON.stringify(scopeMappings).includes("mistfall-private"), false);
  assert.equal(isBrokerDialPeerRequest({ ...dial, endpoint: { ...dial.endpoint, host: "::1" } }), true);
  for (const host of ["localhost", "0.0.0.0", "192.168.1.5"]) {
    assert.equal(isBrokerDialPeerRequest({ ...dial, endpoint: { ...dial.endpoint, host } }), false);
  }
  assert.equal(isBrokerDialPeerRequest({ ...dial, capability: "too-short" }), false);
  assert.equal(isBrokerDialPeerRequest({ ...dial, remoteOrigin: localOrigin }), false);
  assert.equal(isBrokerAcceptPeerRequest({ ...accept, localOrigin: localOrigin }), false);
  assert.equal(isBrokerAcceptPeerRequest({ ...accept, unexpected: true }), false);
});

test("scope authority rejects duplicate IDs, aliases, controls, and oversized values", () => {
  const request = validDialRequest();
  assert.equal(isBrokerDialPeerRequest({
    ...request,
    scopeBindings: [...scopeBindings, { localScopeId: "mistfall-private", localScopeAlias: "other", remoteScopeAlias: "another" }],
  }), false);
  assert.equal(isBrokerDialPeerRequest({
    ...request,
    scopeBindings: [...scopeBindings, { localScopeId: "other", localScopeAlias: "mistfall-local", remoteScopeAlias: "another" }],
  }), false);
  assert.equal(isBrokerDialPeerRequest({ ...request, localOrigin: { ...localOrigin, label: "MacBook\u001b[2J" } }), false);
  assert.equal(isBrokerDialPeerRequest({ ...request, scopeBindings: [{ ...scopeBindings[0], localScopeId: "bad\u202e" }] }), false);
});

test("bridge attachment preface is strict and carries only attachment authority", () => {
  const attach = { type: "bridge_attach", protocol: FEDERATION_PROTOCOL_NAME, version: FEDERATION_PROTOCOL_VERSION, linkId: "link_12345678", capability };
  assert.equal(isFederationBridgeAttach(attach), true);
  assert.equal(isFederationBridgeAttach({ ...attach, capability: "short" }), false);
  assert.equal(isFederationBridgeAttach({ ...attach, version: 2 }), false);
  assert.equal(isFederationBridgeAttach({ ...attach, origin: localOrigin }), false);
});

test("peer hello negotiates explicit v1 single-hop identity between distinct origins", () => {
  const hello = validHello();
  assert.equal(isPeerHello(hello), true);
  assert.equal(isPeerHello({ ...hello, features: [...features, FEDERATION_ROSTER_FEATURE] }), true);
  assert.equal(isPeerHello({ ...hello, version: 2 }), false);
  assert.equal(isPeerHello({ ...hello, expectedPeerOrigin: localOrigin }), false);
  assert.equal(isPeerHello({ ...hello, features: [FEDERATION_IDENTITY_FEATURE] }), false);
  assert.equal(isPeerHello({ ...hello, features: [...features, FEDERATION_IDENTITY_FEATURE] }), false);
  assert.equal(isPeerHello({ ...hello, scopeMappings: [{ ...scopeMappings[0], localScopeId: "leak" }] }), false);
  assert.equal(isPeerHello({ ...hello, forwardedSessions: [] }), false);
});

test("peer hello acknowledgements distinguish accepted and rejected links", () => {
  const accepted = {
    type: "peer_hello_ack", protocol: FEDERATION_PROTOCOL_NAME, version: FEDERATION_PROTOCOL_VERSION,
    linkId: "link_12345678", accepted: true, origin: remoteOrigin,
    acceptedPeerOriginId: localOrigin.id, scopeMappings, features,
  };
  const rejected = {
    type: "peer_hello_ack", protocol: FEDERATION_PROTOCOL_NAME, version: FEDERATION_PROTOCOL_VERSION,
    linkId: "link_12345678", accepted: false, code: "E_ORIGIN_MISMATCH", error: "Expected a different peer origin",
  };
  assert.equal(isPeerHelloAck(accepted), true);
  assert.equal(isPeerHelloAck(rejected), true);
  assert.equal(isPeerHelloAck({ ...accepted, acceptedPeerOriginId: remoteOrigin.id }), false);
  assert.equal(isPeerHelloAck({ ...rejected, origin: remoteOrigin }), false);
  assert.equal(isPeerHelloAck({ ...rejected, error: "Unsafe\nerror" }), false);
});

test("dial and accept results have strict success and failure payloads", () => {
  const dialSuccess = { type: "broker_dial_peer_result", requestId: "request_12345678", ok: true, linkId: "link_12345678" };
  const acceptSuccess = { type: "broker_accept_peer_result", requestId: "request_87654321", ok: true, linkId: "link_12345678" };
  const failure = { type: "broker_dial_peer_result", requestId: "request_12345678", ok: false, code: "E_DIAL_FAILED", error: "Loopback endpoint refused connection" };
  assert.equal(isBrokerDialPeerResult(dialSuccess), true);
  assert.equal(isBrokerAcceptPeerResult(acceptSuccess), true);
  assert.equal(isBrokerDialPeerResult(failure), true);
  assert.equal(isBrokerDialPeerResult({ ...dialSuccess, error: "contradiction" }), false);
});
