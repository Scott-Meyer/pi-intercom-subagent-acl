import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import path from "node:path";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { once } from "node:events";
import { spawn, type ChildProcess } from "node:child_process";
import { createMessageReader, writeMessage } from "./framing.ts";
import { isBrokerAcceptPeerResult, isBrokerDialPeerResult, isFederationBridgeAttach } from "./federation-protocol.ts";
import type { SessionInfo } from "../types.ts";
import type { BrokerDialPeerRequest, BrokerDialPeerResult, FederationBridgeAttach } from "./federation-types.ts";
import { getBrokerSocketPath } from "./paths.ts";
import { getTsxCliPath } from "./spawn.ts";

const repoDir = process.cwd();

async function startBroker(agentDir: string): Promise<ChildProcess> {
  const broker = spawn(process.execPath, [getTsxCliPath(), path.join(repoDir, "broker", "broker.ts")], {
    cwd: repoDir,
    env: { ...process.env, PI_CODING_AGENT_DIR: agentDir },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const stdout = broker.stdout;
  const stderr = broker.stderr;
  if (!stdout || !stderr) throw new Error("Broker output unavailable");
  let stderrText = "";
  stderr.on("data", (chunk: Buffer) => { stderrText += chunk.toString(); });
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`Broker startup timed out: ${stderrText}`));
    }, 10_000);
    const cleanup = () => {
      clearTimeout(timeout);
      stdout.off("data", onData);
      broker.off("exit", onExit);
    };
    const onData = (chunk: Buffer) => {
      if (!chunk.toString().includes("Parley broker started")) return;
      cleanup();
      resolve();
    };
    const onExit = (code: number | null) => {
      cleanup();
      reject(new Error(`Broker exited during startup: ${code}: ${stderrText}`));
    };
    stdout.on("data", onData);
    broker.once("exit", onExit);
  });
  return broker;
}

async function stopBroker(broker: ChildProcess): Promise<void> {
  if (broker.exitCode !== null || broker.signalCode !== null) return;
  broker.kill("SIGTERM");
  await once(broker, "exit").catch(() => undefined);
}

async function readFirstFrame(socket: net.Socket): Promise<{ value: unknown; leftover: Buffer }> {
  return await new Promise((resolve, reject) => {
    let buffered = Buffer.alloc(0);
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const onData = (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      if (buffered.length < 4) return;
      const length = buffered.readUInt32BE(0);
      if (length > 1024 * 1024) {
        onError(new Error("Oversized bridge attachment"));
        return;
      }
      if (buffered.length < 4 + length) return;
      socket.pause();
      cleanup();
      try {
        resolve({
          value: JSON.parse(buffered.subarray(4, 4 + length).toString("utf8")),
          leftover: buffered.subarray(4 + length),
        });
      } catch (error) {
        reject(error);
      }
    };
    const cleanup = () => {
      socket.off("data", onData);
      socket.off("error", onError);
    };
    socket.on("data", onData);
    socket.on("error", onError);
  });
}

async function readOneMessage(socket: net.Socket): Promise<unknown> {
  return await new Promise((resolve, reject) => {
    const reader = createMessageReader((value) => {
      socket.off("data", reader);
      resolve(value);
    }, reject);
    socket.on("data", reader);
  });
}

async function listScopes(socketPath: string, requestId: string): Promise<unknown> {
  const socket = net.connect(socketPath);
  await once(socket, "connect");
  const response = readOneMessage(socket);
  writeMessage(socket, { type: "broker_list_scopes", requestId });
  const result = await response;
  socket.end();
  return result;
}

async function dialPeer(socketPath: string, request: BrokerDialPeerRequest): Promise<BrokerDialPeerResult> {
  const socket = net.connect(socketPath);
  await once(socket, "connect");
  return await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      socket.destroy();
      reject(new Error("Dial control timed out"));
    }, 10_000);
    const reader = createMessageReader((value) => {
      if (!isBrokerDialPeerResult(value)) return;
      clearTimeout(timeout);
      socket.off("data", reader);
      socket.end();
      resolve(value);
    }, reject);
    socket.on("data", reader);
    writeMessage(socket, request);
  });
}

async function registerOrdinaryClient(
  socketPath: string,
  sessionId: string,
  sessionOverrides: Partial<SessionInfo> = {},
): Promise<net.Socket> {
  const socket = net.connect(socketPath);
  await once(socket, "connect");
  await new Promise<void>((resolve, reject) => {
    const reader = createMessageReader((value) => {
      if (typeof value !== "object" || value === null || !("type" in value) || value.type !== "registered") return;
      socket.off("data", reader);
      resolve();
    }, reject);
    socket.on("data", reader);
    writeMessage(socket, {
      type: "register",
      sessionId,
      session: {
        name: sessionId,
        cwd: repoDir,
        model: "federation-test",
        pid: process.pid,
        startedAt: Date.now(),
        lastActivity: Date.now(),
        ...sessionOverrides,
      },
    });
  });
  return socket;
}

async function waitForBrokerMessage(
  socket: net.Socket,
  predicate: (value: Record<string, unknown>) => boolean,
  timeoutMs = 5_000,
): Promise<Record<string, unknown>> {
  return await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      socket.off("data", reader);
      reject(new Error("Timed out waiting for broker push message"));
    }, timeoutMs);
    const reader = createMessageReader((value) => {
      if (typeof value !== "object" || value === null || Array.isArray(value) || !predicate(value as Record<string, unknown>)) return;
      clearTimeout(timeout);
      socket.off("data", reader);
      resolve(value as Record<string, unknown>);
    }, reject);
    socket.on("data", reader);
  });
}

async function advertiseSession(socket: net.Socket, requestId: string, name: string): Promise<{ ok: boolean; name?: string }> {
  const response = new Promise<{ ok: boolean; name?: string }>((resolve, reject) => {
    const reader = createMessageReader((value) => {
      if (typeof value !== "object" || value === null || !("type" in value) || value.type !== "advertise_result") return;
      if (!("requestId" in value) || value.requestId !== requestId) return;
      socket.off("data", reader);
      const result = value as { ok: boolean; name?: string };
      resolve({ ok: result.ok, ...(result.name ? { name: result.name } : {}) });
    }, reject);
    socket.on("data", reader);
  });
  writeMessage(socket, { type: "advertise", requestId, name });
  return await response;
}

async function sendDirect(socket: net.Socket, to: string, messageId: string, timeoutMs = 5_000): Promise<Record<string, unknown>> {
  const response = waitForBrokerMessage(socket, (value) =>
    (value.type === "delivered" || value.type === "delivery_failed") && value.messageId === messageId, timeoutMs);
  writeMessage(socket, {
    type: "send",
    to,
    message: {
      id: messageId,
      timestamp: Date.now(),
      content: { text: "roster-only route probe" },
    },
  });
  return await response;
}

async function listSessions(socket: net.Socket, requestId: string): Promise<SessionInfo[]> {
  const response = new Promise<SessionInfo[]>((resolve, reject) => {
    const reader = createMessageReader((value) => {
      if (typeof value !== "object" || value === null || !("type" in value) || value.type !== "sessions") return;
      if (!("requestId" in value) || value.requestId !== requestId || !("sessions" in value) || !Array.isArray(value.sessions)) return;
      socket.off("data", reader);
      resolve(value.sessions as SessionInfo[]);
    }, reject);
    socket.on("data", reader);
  });
  writeMessage(socket, { type: "list", requestId });
  return await response;
}

async function waitForImportedSession(socket: net.Socket, stableId: string): Promise<SessionInfo> {
  let lastSessions: SessionInfo[] = [];
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const sessions = await listSessions(socket, `roster_${stableId}_${attempt}`);
    lastSessions = sessions;
    const imported = sessions.find((session) => session.federation?.remoteStableSessionId === stableId);
    if (imported) return imported;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`Timed out waiting for imported session ${stableId}; roster=${JSON.stringify(lastSessions)}`);
}

test("two real brokers establish an explicit peer role through a FlightDeck-style opaque proxy", { concurrency: false }, async () => {
  // Unix-domain socket paths are short on macOS; keep both broker endpoints
  // well below the platform limit rather than nesting under the long temp root.
  const root = mkdtempSync(path.join(process.platform === "win32" ? tmpdir() : "/tmp", "pi-fed-"));
  const localDir = path.join(root, "local-agent");
  const remoteDir = path.join(root, "remote-agent");
  const localSocketPath = getBrokerSocketPath(process.platform, localDir);
  const remoteSocketPath = getBrokerSocketPath(process.platform, remoteDir);
  const proxiedSockets: net.Socket[] = [];
  const clientSockets: net.Socket[] = [];
  const attachments: FederationBridgeAttach[] = [];
  const expectedCapabilities = new Set(["A".repeat(32), "C".repeat(32), "D".repeat(32)]);
  let delayFirstPreparedHello = true;
  let dropPeerAcks = false;
  /** Assigned from broker_list_scopes once the brokers are up; the proxy
   * closures below capture it and only run during dials after assignment. */
  let localOriginId = "";
  let localBroker: ChildProcess | undefined;
  let remoteBroker: ChildProcess | undefined;
  let reverseProxy: net.Server | undefined;

  const proxy = net.createServer((source) => {
    source.on("error", () => undefined);
    proxiedSockets.push(source);
    void (async () => {
      const first = await readFirstFrame(source);
      assert.equal(isFederationBridgeAttach(first.value), true);
      const validatedAttach = first.value as FederationBridgeAttach;
      assert.equal(expectedCapabilities.delete(validatedAttach.capability), true, "FlightDeck bridge capability must be exact and single-use");
      attachments.push(validatedAttach);
      const destination = net.connect(remoteSocketPath);
      destination.on("error", () => undefined);
      proxiedSockets.push(destination);
      await once(destination, "connect");
      const attach = first.value as FederationBridgeAttach;
      writeMessage(destination, {
        type: "broker_accept_peer",
        requestId: `prepare_${attach.linkId}`,
        linkId: attach.linkId,
        localOrigin: { id: "host:penguin", label: "Penguin" },
        remoteOrigin: { id: localOriginId, label: "MacBook" },
        scopeBindings: [{ localScopeId: null, localScopeAlias: "mistfall-remote", remoteScopeAlias: "mistfall-local" }],
      });
      const prepared = await readFirstFrame(destination);
      assert.equal(isBrokerAcceptPeerResult(prepared.value), true);
      assert.equal((prepared.value as { ok: boolean }).ok, true);
      assert.equal(prepared.leftover.length, 0);
      if (delayFirstPreparedHello) {
        delayFirstPreparedHello = false;
        await new Promise((resolve) => setTimeout(resolve, 1_200));
      }
      if (first.leftover.length > 0) destination.write(first.leftover);
      destination.on("data", createMessageReader(value => {
        if (dropPeerAcks && (value as { type?: string }).type === "peer_send_result") return;
        writeMessage(source, value);
      }, error => source.destroy(error)));
      destination.resume();
      source.pipe(destination);
      source.resume();
})().catch((error) => source.destroy(error));
  });

  try {
    remoteBroker = await startBroker(remoteDir);
    localBroker = await startBroker(localDir);
    proxy.listen(0, "127.0.0.1");
    await once(proxy, "listening");
    const address = proxy.address();
    assert.ok(address && typeof address !== "string");

    // The controller reads the canonical origin before dialing; the broker
    // owns and persists it, and later dials must present exactly it.
    const originProbe = await listScopes(localSocketPath, "list_scopes_origin_0001");
    assert.equal((originProbe as { ok?: boolean }).ok, true);
    assert.equal((originProbe as { localOrigin?: { id?: string } }).localOrigin?.id !== undefined, true);
    localOriginId = (originProbe as { localOrigin: { id: string } }).localOrigin.id;
    assert.match(localOriginId, /^install:[0-9a-f-]+$/);
    assert.deepEqual((originProbe as { scopes?: unknown[] }).scopes, []);

    const request = (requestId: string, capability: string): BrokerDialPeerRequest => ({
      type: "broker_dial_peer",
      requestId,
      endpoint: { transport: "tcp", host: "127.0.0.1", port: address.port },
      capability,
      localOrigin: { id: localOriginId, label: "MacBook" },
      remoteOrigin: { id: "host:penguin", label: "Penguin" },
      scopeBindings: [{ localScopeId: null, localScopeAlias: "mistfall-local", remoteScopeAlias: "mistfall-remote" }],
    });

    const malformedSocket = net.connect(localSocketPath);
    await once(malformedSocket, "connect");
    const malformedResponse = readOneMessage(malformedSocket);
    writeMessage(malformedSocket, {
      ...request("request_malformed", "Z".repeat(32)),
      endpoint: { transport: "tcp", host: "localhost", port: address.port },
    });
    const malformed = await malformedResponse;
    assert.equal(isBrokerDialPeerResult(malformed), true);
    assert.deepEqual(
      { ok: (malformed as BrokerDialPeerResult).ok, code: (malformed as { code?: string }).code },
      { ok: false, code: "E_INVALID_REQUEST" },
    );
    malformedSocket.end();

    const negotiationSocket = net.connect(remoteSocketPath);
    await once(negotiationSocket, "connect");
    const preparationResponse = readOneMessage(negotiationSocket);
    writeMessage(negotiationSocket, {
      type: "broker_accept_peer",
      requestId: "prepare_badversion",
      linkId: "link_badversion",
      localOrigin: { id: "host:penguin", label: "Penguin" },
      remoteOrigin: { id: localOriginId, label: "MacBook" },
      scopeBindings: [{ localScopeId: null, localScopeAlias: "mistfall-remote", remoteScopeAlias: "mistfall-local" }],
    });
    const preparationResult = await preparationResponse;
    assert.equal(isBrokerAcceptPeerResult(preparationResult), true);
    const versionResponse = readOneMessage(negotiationSocket);
    writeMessage(negotiationSocket, {
      type: "peer_hello",
      protocol: "pi-parley-peer",
      version: 2,
      linkId: "link_badversion",
    });
    const versionResult = await versionResponse as { accepted: boolean; code: string };
    assert.deepEqual({ accepted: versionResult.accepted, code: versionResult.code }, { accepted: false, code: "E_VERSION_UNSUPPORTED" });
    negotiationSocket.end();

    const connected = await dialPeer(localSocketPath, request("request_12345678", "A".repeat(32)));
    assert.equal(connected.ok, true);
    assert.equal(attachments.length, 1);
    if (connected.ok) assert.equal(attachments[0]?.linkId, connected.linkId);

    // A controller presenting a different origin id is refused; the
    // canonical origin is durable for this broker install.
    const mismatch = await dialPeer(localSocketPath, {
      ...request("request_origin_mismatch", "F".repeat(32)),
      localOrigin: { id: "host:pretender", label: "Pretender" },
    });
    assert.equal(mismatch.ok, false);
    if (!mismatch.ok) assert.equal(mismatch.code, "E_ORIGIN_MISMATCH");

    const localClient = await registerOrdinaryClient(localSocketPath, "local-client");
    const remoteClient = await registerOrdinaryClient(remoteSocketPath, "remote-client");
    clientSockets.push(localClient, remoteClient);
    const importedRemote = await waitForImportedSession(localClient, "remote-client");
    const importedLocal = await waitForImportedSession(remoteClient, "local-client");
    assert.equal(importedRemote.trustedLocal, false);
    assert.equal(importedRemote.federation?.originId, "host:penguin");
    assert.equal(importedRemote.federation?.remoteScopeAlias, "mistfall-remote");
    assert.equal(importedLocal.federation?.originId, localOriginId);
    const scopesWithSessions = await listScopes(localSocketPath, "list_scopes_sessions_01");
    assert.deepEqual((scopesWithSessions as { scopes?: unknown[] }).scopes, [{ scopeId: null, liveSessions: 1 }]);
    // Routed direct send: the remote client receives the message with the
    // broker-authoritative imported sender identity, and the local sender
    // sees delivery feedback only from the correlated destination result.
    const remoteInbound = waitForBrokerMessage(remoteClient, (value) =>
      value.type === "message" && (value.message as { id?: string } | undefined)?.id === "federated_direct_1");
    const federatedSend = await sendDirect(localClient, importedRemote.id, "federated_direct_1");
    assert.equal(federatedSend.type, "delivered");
    assert.equal(federatedSend.delivery, "socket_delivered");
    assert.equal((federatedSend.recipient as SessionInfo).id, importedRemote.id);
    const remoteMessage = await remoteInbound;
    assert.equal((remoteMessage.from as SessionInfo).id, importedLocal.id);
    assert.equal((remoteMessage.from as SessionInfo).trustedLocal, false);
    assert.equal((remoteMessage.from as SessionInfo).federation?.originId, localOriginId);
    assert.equal(((remoteMessage.message as { content?: { text?: string } }).content)?.text, "roster-only route probe");
    // An identical retry of the delivered message replays without a second
    // remote delivery.
    const replayed = await sendDirect(localClient, importedRemote.id, "federated_direct_1");
    assert.equal(replayed.type, "delivered");

    // The destination receives work even when its ACK is lost. Expiry is an
    // unknown outcome, and replaying the same handle must not repeat the work.
    dropPeerAcks = true;
    const timeoutDeliveries: unknown[] = [];
    const timeoutCollector = createMessageReader(value => {
      if ((value as { message?: { id?: string } }).message?.id === "lost_peer_ack") timeoutDeliveries.push(value);
    }, error => { throw error; });
    remoteClient.on("data", timeoutCollector);
    const uncertain = await sendDirect(localClient, importedRemote.id, "lost_peer_ack", 12_000);
    assert.equal(timeoutDeliveries.length, 1, "recipient accepted the work before ACK loss");
    assert.equal(uncertain.messageId, "lost_peer_ack");
    assert.equal(uncertain.delivery, "unknown");
    assert.equal(uncertain.outcomeKnown, false);
    assert.equal(uncertain.retryable, false);
    assert.equal((uncertain.recipient as SessionInfo).id, importedRemote.id);
    dropPeerAcks = false;
    const uncertainReplay = await sendDirect(localClient, importedRemote.id, "lost_peer_ack");
    assert.equal(uncertainReplay.delivery, "unknown");
    await listSessions(remoteClient, "timeout_replay_barrier");
    assert.equal(timeoutDeliveries.length, 1, "uncertain outcome is retained for duplicate suppression");
    remoteClient.off("data", timeoutCollector);

    // A second send that races the first delivery with the same message id
    // is refused while the first is still in flight; the first still delivers.
    const inFlightResults: Record<string, unknown>[] = [];
    const inFlightCollector = createMessageReader((value) => {
      if ((value as { type?: string }).type === "delivered"
        || (value as { type?: string }).type === "delivery_failed") {
        if ((value as { messageId?: string }).messageId === "federated_inflight_1") inFlightResults.push(value as Record<string, unknown>);
      }
    }, () => undefined);
    localClient.on("data", inFlightCollector);
    try {
      for (let attempt = 0; attempt < 2; attempt += 1) {
        writeMessage(localClient, {
          type: "send",
          to: importedRemote.id,
          message: {
            id: "federated_inflight_1",
            timestamp: Date.now(),
            content: { text: "race the first delivery" },
          },
        });
      }
      const raceDeadline = Date.now() + 5_000;
      while (inFlightResults.length < 2 && Date.now() < raceDeadline) {
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
    } finally {
      localClient.off("data", inFlightCollector);
    }
    assert.equal(inFlightResults.length, 2, `both racing sends report an outcome: ${JSON.stringify(inFlightResults)}`);
    assert.equal(
      inFlightResults.filter((value) => value.type === "delivery_failed" && value.code === "E_MESSAGE_ID_REUSE").length,
      1,
      "exactly the racing duplicate is refused while the first is in flight",
    );
    assert.equal(
      inFlightResults.some((value) => value.type === "delivered"),
      true,
      "the first racing send is still delivered",
    );

    const unknownRemote = await sendDirect(localClient, `${importedRemote.id.slice(0, 8)}not-a-real-identity`, "federated_unknown_1");
    assert.equal(unknownRemote.type, "delivery_failed");
    assert.equal(unknownRemote.code, "E_TARGET_NOT_FOUND");
    assert.match(String(unknownRemote.reason), /not present in the federated roster/);

    const blockingResponse = waitForBrokerMessage(localClient, (value) =>
      (value.type === "delivered" || value.type === "delivery_failed") && value.messageId === "federated_ask_1");
    writeMessage(localClient, {
      type: "send",
      to: importedRemote.id,
      message: {
        id: "federated_ask_1",
        timestamp: Date.now(),
        expectsReply: true,
        content: { text: "remote asks arrive in a later slice" },
      },
    });
    const blockingProbe = await blockingResponse;
    assert.equal(blockingProbe.type, "delivery_failed");
    assert.equal(blockingProbe.code, "E_INVALID_MESSAGE");
    assert.match(String(blockingProbe.reason), /later federation slice/);

    const localChild = await registerOrdinaryClient(localSocketPath, "local-child", {
      isSubagent: true,
      supervisorSessionId: "local-client",
      supervisorName: "local-client",
    });
    clientSockets.push(localChild);
    const restrictedView = await listSessions(localChild, "roster_restricted_view");
    assert.equal(restrictedView.some((session) => session.federation !== undefined), false);
    const remoteViewBeforeAdvertise = await listSessions(remoteClient, "roster_hidden_local_child");
    assert.equal(remoteViewBeforeAdvertise.some((session) => session.federation?.remoteStableSessionId === "local-child"), false);

    const remoteChild = await registerOrdinaryClient(remoteSocketPath, "remote-child", {
      isSubagent: true,
      supervisorSessionId: "remote-client",
      supervisorName: "remote-client",
    });
    clientSockets.push(remoteChild);
    const beforeAdvertise = await listSessions(localClient, "roster_hidden_child");
    assert.equal(beforeAdvertise.some((session) => session.federation?.remoteStableSessionId === "remote-child"), false);
    const joinedPush = waitForBrokerMessage(localClient, (value) => {
      const pushed = value.session as SessionInfo | undefined;
      return value.type === "session_joined" && pushed?.federation?.remoteStableSessionId === "remote-child";
    });
    const advertised = await advertiseSession(remoteChild, "advertise_remote_child", "Remote Specialist");
    assert.deepEqual(advertised, { ok: true, name: "Remote Specialist" });
    const joined = await joinedPush;
    assert.equal((joined.session as SessionInfo).trustedLocal, false);
    const importedChild = await waitForImportedSession(localClient, "remote-child");
    assert.equal(importedChild.name, "Remote Specialist");
    const updatedPush = waitForBrokerMessage(localClient, (value) => {
      const pushed = value.session as SessionInfo | undefined;
      return value.type === "presence_update"
        && pushed?.federation?.remoteStableSessionId === "remote-child"
        && pushed.status === "thinking";
    });
    writeMessage(remoteChild, { type: "presence", status: "thinking" });
    await updatedPush;

    const duplicate = await dialPeer(localSocketPath, request("request_87654321", "B".repeat(32)));
    assert.equal(duplicate.ok, false);
    if (!duplicate.ok) assert.equal(duplicate.code, "E_ALREADY_CONNECTED");

    dropPeerAcks = true;
    const beforeLinkLoss = waitForBrokerMessage(remoteClient, value =>
      value.type === "message" && (value.message as { id?: string })?.id === "link_lost_after_delivery");
    const linkLossResult = sendDirect(localClient, importedRemote.id, "link_lost_after_delivery");
    await beforeLinkLoss;
    const leftPush = waitForBrokerMessage(localClient, (value) => value.type === "session_left" && value.sessionId === importedRemote.id);
    for (const socket of proxiedSockets.splice(0)) socket.destroy();
    await leftPush;
    const linkLoss = await linkLossResult;
    assert.equal(linkLoss.delivery, "unknown");
    assert.equal(linkLoss.outcomeKnown, false);
    assert.equal(linkLoss.retryable, false);
    dropPeerAcks = false;
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal((await sendDirect(localClient, importedRemote.id, "link_lost_after_delivery")).delivery, "unknown", "loss of roster cannot rewrite an uncertain delivery as nondelivery");
    const pruned = await listSessions(localClient, "roster_pruned_1");
    assert.equal(pruned.some((session) => session.federation?.originId === "host:penguin"), false);

    const reconnected = await dialPeer(localSocketPath, request("request_abcdefgh", "C".repeat(32)));
    assert.equal(reconnected.ok, true);
    assert.equal(attachments.length, 2);
    await waitForImportedSession(localClient, "remote-client");

    // Tear down and then race reciprocal dials. Canonical origin ordering picks
    // MacBook's outbound link, so overlap can transiently succeed but converges
    // to exactly one stable physical link rather than collapsing to zero.
    for (const socket of proxiedSockets.splice(0)) socket.destroy();
    await new Promise((resolve) => setTimeout(resolve, 100));
    reverseProxy = net.createServer((source) => {
      source.on("error", () => undefined);
      proxiedSockets.push(source);
      void (async () => {
        const first = await readFirstFrame(source);
        assert.equal(isFederationBridgeAttach(first.value), true);
        const attach = first.value as FederationBridgeAttach;
        assert.equal(attach.capability, "E".repeat(32));
        const destination = net.connect(localSocketPath);
        destination.on("error", () => undefined);
        proxiedSockets.push(destination);
        await once(destination, "connect");
        writeMessage(destination, {
          type: "broker_accept_peer",
          requestId: `prepare_${attach.linkId}`,
          linkId: attach.linkId,
          localOrigin: { id: localOriginId, label: "MacBook" },
          remoteOrigin: { id: "host:penguin", label: "Penguin" },
          scopeBindings: [{ localScopeId: null, localScopeAlias: "mistfall-local", remoteScopeAlias: "mistfall-remote" }],
        });
        const prepared = await readFirstFrame(destination);
        assert.equal(isBrokerAcceptPeerResult(prepared.value), true);
        assert.equal((prepared.value as { ok: boolean }).ok, true);
        if (first.leftover.length > 0) destination.write(first.leftover);
        source.pipe(destination);
        destination.pipe(source);
        source.resume();
      })().catch((error) => source.destroy(error));
    });
    reverseProxy.listen(0, "127.0.0.1");
    await once(reverseProxy, "listening");
    const reverseAddress = reverseProxy.address();
    assert.ok(reverseAddress && typeof reverseAddress !== "string");
    const reverseRequest = (requestId: string, capability: string): BrokerDialPeerRequest => ({
      type: "broker_dial_peer",
      requestId,
      endpoint: { transport: "tcp", host: "127.0.0.1", port: reverseAddress.port },
      capability,
      localOrigin: { id: "host:penguin", label: "Penguin" },
      remoteOrigin: { id: localOriginId, label: "MacBook" },
      scopeBindings: [{ localScopeId: null, localScopeAlias: "mistfall-remote", remoteScopeAlias: "mistfall-local" }],
    });
    const raceResults = await Promise.all([
      dialPeer(localSocketPath, request("request_raceleft", "D".repeat(32))),
      dialPeer(remoteSocketPath, reverseRequest("request_raceright", "E".repeat(32))),
    ]);
    assert.equal(raceResults.some((result) => result.ok), true, "at least one reciprocal dial must establish a link");
    assert.equal(expectedCapabilities.size, 0);
    await waitForImportedSession(localClient, "remote-client");
    await waitForImportedSession(remoteClient, "local-client");
    await new Promise((resolve) => setTimeout(resolve, 100));
    const stableFromLocal = await dialPeer(localSocketPath, request("request_checkleft", "F".repeat(32)));
    const stableFromRemote = await dialPeer(remoteSocketPath, reverseRequest("request_checkright", "G".repeat(32)));
    assert.equal(stableFromLocal.ok, false);
    assert.equal(stableFromRemote.ok, false);
    if (!stableFromLocal.ok) assert.equal(stableFromLocal.code, "E_ALREADY_CONNECTED");
    if (!stableFromRemote.ok) assert.equal(stableFromRemote.code, "E_ALREADY_CONNECTED");

    for (const socket of clientSockets.splice(0)) socket.end();
    await new Promise((resolve) => setTimeout(resolve, 100));
    for (const socket of proxiedSockets.splice(0)) socket.destroy();
    const exited = await Promise.race([
      Promise.all([once(localBroker, "exit"), once(remoteBroker, "exit")]).then(() => true),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 8_000)),
    ]);
    assert.equal(exited, true, "both brokers should auto-shutdown after the final peer disconnects");
  } finally {
    for (const socket of clientSockets) socket.destroy();
    for (const socket of proxiedSockets) socket.destroy();
    proxy.close();
    reverseProxy?.close();
    await Promise.all([
      localBroker ? stopBroker(localBroker) : Promise.resolve(),
      remoteBroker ? stopBroker(remoteBroker) : Promise.resolve(),
    ]);
    rmSync(root, { recursive: true, force: true });
  }
});
