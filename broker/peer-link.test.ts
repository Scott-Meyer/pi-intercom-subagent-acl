import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { once } from "node:events";
import { createMessageReader, writeMessage } from "./framing.ts";
import { PeerLinkManager } from "./peer-link.ts";
import {
  FEDERATION_PROTOCOL_NAME,
  FEDERATION_PROTOCOL_VERSION,
  FEDERATION_REQUIRED_FEATURES,
  type BrokerAcceptPeerRequest,
  type BrokerDialPeerRequest,
  type PeerHello,
} from "./federation-types.ts";
import { isFederationBridgeAttach, isPeerHello } from "./federation-protocol.ts";

const localOrigin = { id: "host:macbook", label: "MacBook" };
const remoteOrigin = { id: "host:penguin", label: "Penguin" };
const localBindings = [{ localScopeId: "local-private", localScopeAlias: "mistfall-local", remoteScopeAlias: "mistfall-remote" }];
const remoteBindings = [{ localScopeId: "remote-private", localScopeAlias: "mistfall-remote", remoteScopeAlias: "mistfall-local" }];
const scopeMappings = [{ localScopeAlias: "mistfall-local", remoteScopeAlias: "mistfall-remote" }];

function hello(linkId = "link_12345678"): PeerHello {
  return {
    type: "peer_hello",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    linkId,
    origin: localOrigin,
    expectedPeerOrigin: remoteOrigin,
    scopeMappings,
    features: [...FEDERATION_REQUIRED_FEATURES],
  };
}

function preparation(linkId = "link_12345678"): BrokerAcceptPeerRequest {
  return {
    type: "broker_accept_peer",
    requestId: "prepare_12345678",
    linkId,
    localOrigin: remoteOrigin,
    remoteOrigin: localOrigin,
    scopeBindings: remoteBindings,
  };
}

test("inbound hello must match independently prepared origin and local scope authority", () => {
  const manager = new PeerLinkManager();
  const socket = new net.Socket();
  const prepared = manager.prepareInbound(preparation());
  const accepted = manager.acceptInbound(socket, hello(), prepared);

  assert.equal(accepted.ack.accepted, true);
  assert.equal(accepted.link?.direction, "inbound");
  assert.deepEqual(accepted.link?.localOrigin, remoteOrigin);
  assert.deepEqual(accepted.link?.remoteOrigin, localOrigin);
  assert.deepEqual(accepted.link?.scopeBindings, remoteBindings);
  assert.equal(manager.size, 1);

  const mismatchedScopes = manager.acceptInbound(
    new net.Socket(),
    { ...hello("different_12345678"), scopeMappings: [{ localScopeAlias: "wrong", remoteScopeAlias: "mistfall-remote" }] },
    manager.prepareInbound(preparation("different_12345678")),
  );
  assert.equal(mismatchedScopes.ack.accepted, false);
  if (!mismatchedScopes.ack.accepted) assert.equal(mismatchedScopes.ack.code, "E_SCOPE_MISMATCH");

  assert.equal(manager.removeInbound(accepted.link!.linkId, socket)?.linkId, accepted.link?.linkId);
  assert.equal(manager.size, 0);
  assert.throws(() => manager.prepareInbound({
    ...preparation("new_link_12345678"),
    localOrigin: { id: "host:someone-else" },
  }), /does not match this broker/);
  socket.destroy();
});

async function createAckServer(
  origin = remoteOrigin,
  ackFeatures: string[] = [...FEDERATION_REQUIRED_FEATURES],
): Promise<{ server: net.Server; port: number; received: unknown[] }> {
  const received: unknown[] = [];
  const server = net.createServer((socket) => {
    const reader = createMessageReader((value) => {
      received.push(value);
      if (!isPeerHello(value)) return;
      writeMessage(socket, {
        type: "peer_hello_ack",
        protocol: FEDERATION_PROTOCOL_NAME,
        version: FEDERATION_PROTOCOL_VERSION,
        linkId: value.linkId,
        accepted: true,
        origin,
        acceptedPeerOriginId: localOrigin.id,
        scopeMappings: value.scopeMappings,
        features: ackFeatures,
      });
    }, (error) => socket.destroy(error));
    socket.on("data", reader);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  return { server, port: address.port, received };
}

function dialRequest(port: number, overrides: Partial<BrokerDialPeerRequest> = {}): BrokerDialPeerRequest {
  return {
    type: "broker_dial_peer",
    requestId: "request_12345678",
    endpoint: { transport: "tcp", host: "127.0.0.1", port },
    capability: "A".repeat(32),
    localOrigin,
    remoteOrigin,
    scopeBindings: localBindings,
    ...overrides,
  };
}

test("outbound dial writes attachment before hello, sends no raw scope ID, and waits for matching ack", async () => {
  const { server, port, received } = await createAckServer();
  const manager = new PeerLinkManager();
  try {
    const link = await manager.dial(dialRequest(port));
    assert.equal(link.direction, "outbound");
    assert.equal(link.remoteOrigin.id, remoteOrigin.id);
    assert.deepEqual(link.scopeBindings, localBindings);
    assert.equal(isFederationBridgeAttach(received[0]), true);
    assert.equal(isPeerHello(received[1]), true);
    assert.equal((received[0] as { linkId: string }).linkId, (received[1] as { linkId: string }).linkId);
    assert.equal(JSON.stringify(received[1]).includes("local-private"), false);
  } finally {
    manager.close();
    server.close();
    await once(server, "close");
  }
});

test("failed outbound dial does not poison the broker local origin", async () => {
  const unavailable = net.createServer();
  unavailable.listen(0, "127.0.0.1");
  await once(unavailable, "listening");
  const unavailableAddress = unavailable.address();
  assert.ok(unavailableAddress && typeof unavailableAddress !== "string");
  const deadPort = unavailableAddress.port;
  unavailable.close();
  await once(unavailable, "close");

  const manager = new PeerLinkManager();
  await assert.rejects(manager.dial(dialRequest(deadPort)), /Failed to connect/);

  const otherLocal = { id: "host:corrected" };
  const received: unknown[] = [];
  const server = net.createServer((socket) => {
    const reader = createMessageReader((value) => {
      received.push(value);
      if (!isPeerHello(value)) return;
      writeMessage(socket, {
        type: "peer_hello_ack", protocol: FEDERATION_PROTOCOL_NAME, version: FEDERATION_PROTOCOL_VERSION,
        linkId: value.linkId, accepted: true, origin: remoteOrigin,
        acceptedPeerOriginId: otherLocal.id, scopeMappings: value.scopeMappings,
        features: [...FEDERATION_REQUIRED_FEATURES],
      });
    }, (error) => socket.destroy(error));
    socket.on("data", reader);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  try {
    const link = await manager.dial(dialRequest(address.port, { localOrigin: otherLocal, capability: "B".repeat(32) }));
    assert.equal(link.localOrigin.id, otherLocal.id);
  } finally {
    manager.close();
    server.close();
    await once(server, "close");
  }
});

test("abandoning a dial control tears down the pending peer connection", async () => {
  const server = net.createServer((socket) => socket.on("data", () => undefined));
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const manager = new PeerLinkManager();
  const controller = new AbortController();
  try {
    const pending = manager.dial(dialRequest(address.port), controller.signal);
    controller.abort();
    await assert.rejects(pending, /abandoned/);
    assert.equal(manager.size, 0);
  } finally {
    manager.close();
    server.close();
    await once(server, "close");
  }
});

test("outbound dial rejects acknowledgement features it did not offer", async () => {
  const { server, port } = await createAckServer(remoteOrigin, [...FEDERATION_REQUIRED_FEATURES, "future-roster-v2"]);
  const manager = new PeerLinkManager();
  try {
    await assert.rejects(manager.dial(dialRequest(port)), /not offered/);
    assert.equal(manager.size, 0);
  } finally {
    manager.close();
    server.close();
    await once(server, "close");
  }
});

test("outbound dial rejects a peer acknowledgement for another origin", async () => {
  const { server, port } = await createAckServer({ id: "host:impostor" });
  const manager = new PeerLinkManager();
  try {
    await assert.rejects(manager.dial(dialRequest(port)), /requested origins/);
    assert.equal(manager.size, 0);
  } finally {
    manager.close();
    server.close();
    await once(server, "close");
  }
});
