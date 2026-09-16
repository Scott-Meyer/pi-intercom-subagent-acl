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
      if (!chunk.toString().includes("Intercom broker started")) return;
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

async function registerOrdinaryClient(socketPath: string, sessionId: string): Promise<void> {
  const socket = net.connect(socketPath);
  await once(socket, "connect");
  await new Promise<void>((resolve, reject) => {
    const reader = createMessageReader((value) => {
      if (typeof value !== "object" || value === null || !("type" in value) || value.type !== "registered") return;
      socket.off("data", reader);
      writeMessage(socket, { type: "unregister" });
      socket.end();
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
      },
    });
  });
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
  const attachments: FederationBridgeAttach[] = [];
  const expectedCapabilities = new Set(["A".repeat(32), "C".repeat(32), "D".repeat(32)]);
  let delayFirstPreparedHello = true;
  let localBroker: ChildProcess | undefined;
  let remoteBroker: ChildProcess | undefined;
  let reverseProxy: net.Server | undefined;

  const proxy = net.createServer((source) => {
    proxiedSockets.push(source);
    void (async () => {
      const first = await readFirstFrame(source);
      assert.equal(isFederationBridgeAttach(first.value), true);
      const validatedAttach = first.value as FederationBridgeAttach;
      assert.equal(expectedCapabilities.delete(validatedAttach.capability), true, "FlightDeck bridge capability must be exact and single-use");
      attachments.push(validatedAttach);
      const destination = net.connect(remoteSocketPath);
      proxiedSockets.push(destination);
      await once(destination, "connect");
      const attach = first.value as FederationBridgeAttach;
      writeMessage(destination, {
        type: "broker_accept_peer",
        requestId: `prepare_${attach.linkId}`,
        linkId: attach.linkId,
        localOrigin: { id: "host:penguin", label: "Penguin" },
        remoteOrigin: { id: "host:macbook", label: "MacBook" },
        scopeBindings: [{ localScopeId: "remote-private", localScopeAlias: "mistfall-remote", remoteScopeAlias: "mistfall-local" }],
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
      source.pipe(destination);
      destination.pipe(source);
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

    const request = (requestId: string, capability: string): BrokerDialPeerRequest => ({
      type: "broker_dial_peer",
      requestId,
      endpoint: { transport: "tcp", host: "127.0.0.1", port: address.port },
      capability,
      localOrigin: { id: "host:macbook", label: "MacBook" },
      remoteOrigin: { id: "host:penguin", label: "Penguin" },
      scopeBindings: [{ localScopeId: "local-private", localScopeAlias: "mistfall-local", remoteScopeAlias: "mistfall-remote" }],
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
      remoteOrigin: { id: "host:macbook", label: "MacBook" },
      scopeBindings: [{ localScopeId: "remote-private", localScopeAlias: "mistfall-remote", remoteScopeAlias: "mistfall-local" }],
    });
    const preparationResult = await preparationResponse;
    assert.equal(isBrokerAcceptPeerResult(preparationResult), true);
    const versionResponse = readOneMessage(negotiationSocket);
    writeMessage(negotiationSocket, {
      type: "peer_hello",
      protocol: "pi-intercom-peer",
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

    await registerOrdinaryClient(localSocketPath, "local-client");
    await registerOrdinaryClient(remoteSocketPath, "remote-client");

    const duplicate = await dialPeer(localSocketPath, request("request_87654321", "B".repeat(32)));
    assert.equal(duplicate.ok, false);
    if (!duplicate.ok) assert.equal(duplicate.code, "E_ALREADY_CONNECTED");

    for (const socket of proxiedSockets.splice(0)) socket.destroy();
    await new Promise((resolve) => setTimeout(resolve, 100));

    const reconnected = await dialPeer(localSocketPath, request("request_abcdefgh", "C".repeat(32)));
    assert.equal(reconnected.ok, true);
    assert.equal(attachments.length, 2);

    // Tear down and then race reciprocal dials. Canonical origin ordering picks
    // MacBook's outbound link, so overlap can transiently succeed but converges
    // to exactly one stable physical link rather than collapsing to zero.
    for (const socket of proxiedSockets.splice(0)) socket.destroy();
    await new Promise((resolve) => setTimeout(resolve, 100));
    reverseProxy = net.createServer((source) => {
      proxiedSockets.push(source);
      void (async () => {
        const first = await readFirstFrame(source);
        assert.equal(isFederationBridgeAttach(first.value), true);
        const attach = first.value as FederationBridgeAttach;
        assert.equal(attach.capability, "E".repeat(32));
        const destination = net.connect(localSocketPath);
        proxiedSockets.push(destination);
        await once(destination, "connect");
        writeMessage(destination, {
          type: "broker_accept_peer",
          requestId: `prepare_${attach.linkId}`,
          linkId: attach.linkId,
          localOrigin: { id: "host:macbook", label: "MacBook" },
          remoteOrigin: { id: "host:penguin", label: "Penguin" },
          scopeBindings: [{ localScopeId: "local-private", localScopeAlias: "mistfall-local", remoteScopeAlias: "mistfall-remote" }],
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
      remoteOrigin: { id: "host:macbook", label: "MacBook" },
      scopeBindings: [{ localScopeId: "remote-private", localScopeAlias: "mistfall-remote", remoteScopeAlias: "mistfall-local" }],
    });
    const raceResults = await Promise.all([
      dialPeer(localSocketPath, request("request_raceleft", "D".repeat(32))),
      dialPeer(remoteSocketPath, reverseRequest("request_raceright", "E".repeat(32))),
    ]);
    assert.equal(raceResults.some((result) => result.ok), true, "at least one reciprocal dial must establish a link");
    assert.equal(expectedCapabilities.size, 0);
    await new Promise((resolve) => setTimeout(resolve, 100));
    const stableFromLocal = await dialPeer(localSocketPath, request("request_checkleft", "F".repeat(32)));
    const stableFromRemote = await dialPeer(remoteSocketPath, reverseRequest("request_checkright", "G".repeat(32)));
    assert.equal(stableFromLocal.ok, false);
    assert.equal(stableFromRemote.ok, false);
    if (!stableFromLocal.ok) assert.equal(stableFromLocal.code, "E_ALREADY_CONNECTED");
    if (!stableFromRemote.ok) assert.equal(stableFromRemote.code, "E_ALREADY_CONNECTED");

    for (const socket of proxiedSockets.splice(0)) socket.destroy();
    const exited = await Promise.race([
      Promise.all([once(localBroker, "exit"), once(remoteBroker, "exit")]).then(() => true),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 8_000)),
    ]);
    assert.equal(exited, true, "both brokers should auto-shutdown after the final peer disconnects");
  } finally {
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
