import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import path from "node:path";
import { Duplex, PassThrough } from "node:stream";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { attachPeerStream, PEER_STREAM_COPY_BYTES, PeerStreamAttachmentError, PeerStreamController, type AttachPeerStreamOptions } from "./attachment.ts";
import { createMessageReader, writeMessage } from "./framing.ts";
import { getBrokerSocketPath, readBrokerTcpEndpoint, getParleyDirPath, type BrokerConnectTarget } from "./paths.ts";
import { getTsxCliPath } from "./spawn.ts";
import { FEDERATION_PROTOCOL_NAME, FEDERATION_PROTOCOL_VERSION, FEDERATION_SUPPORTED_FEATURES } from "./federation-types.ts";
import type { BrokerAcceptPeerRequest } from "./federation-types.ts";
import type { SessionInfo } from "../types.ts";

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
}
function encode(value: unknown): Buffer {
  const payload = Buffer.from(JSON.stringify(value));
  const bytes = Buffer.allocUnsafe(payload.length + 4);
  bytes.writeUInt32BE(payload.length, 0);
  payload.copy(bytes, 4);
  return bytes;
}
function connect(target: BrokerConnectTarget): net.Socket {
  const socket = typeof target === "string" ? net.connect(target) : net.connect(target.port, target.host);
  socket.on("error", () => undefined);
  return socket;
}
function auth(target: BrokerConnectTarget) {
  return typeof target === "string" ? {} : { stateId: target.stateId };
}

class Inbox {
  private queue: Record<string, unknown>[] = [];
  private pending: { predicate: (v: Record<string, unknown>) => boolean; resolve: (v: Record<string, unknown>) => void }[] = [];
  constructor(readonly socket: net.Socket) {
    socket.on("data", createMessageReader((value) => {
      const v = value as Record<string, unknown>;
      const index = this.pending.findIndex((entry) => entry.predicate(v));
      if (index >= 0) this.pending.splice(index, 1)[0]!.resolve(v);
      else this.queue.push(v);
    }, (error) => socket.destroy(error)));
  }
  async next(predicate: (v: Record<string, unknown>) => boolean): Promise<Record<string, unknown>> {
    const index = this.queue.findIndex(predicate);
    if (index >= 0) return this.queue.splice(index, 1)[0]!;
    let entry: typeof this.pending[number];
    let timer: NodeJS.Timeout | undefined;
    try {
      return await Promise.race([
        new Promise<Record<string, unknown>>((resolve) => { entry = { predicate, resolve }; this.pending.push(entry); }),
        new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Broker response timed out")), 5_000); }),
      ]);
    } finally {
      clearTimeout(timer);
      this.pending = this.pending.filter((v) => v !== entry!);
    }
  }
  async request(message: Record<string, unknown>, type: string) {
    const requestId = randomUUID();
    const result = this.next((v) => v.type === type && v.requestId === requestId);
    writeMessage(this.socket, { ...message, requestId });
    return await result;
  }
  async sessions(): Promise<SessionInfo[]> {
    return (await this.request({ type: "list" }, "sessions")).sessions as SessionInfo[];
  }
}

async function startBroker(agentDir: string, tcp: boolean): Promise<ChildProcess> {
  const child = spawn(process.execPath, [getTsxCliPath(), path.join(process.cwd(), "broker/broker.ts")], {
    env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, PI_PARLEY_TRANSPORT: tcp ? "tcp" : "socket", PI_PARLEY_TCP: tcp ? "true" : "false" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr!.on("data", (bytes: Buffer) => { stderr = (stderr + bytes.toString()).slice(-4000); });
  try {
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => { clearTimeout(timer); child.stdout!.off("data", onData); child.off("exit", onExit); };
      const onData = (bytes: Buffer) => { if (bytes.toString().includes("Parley broker started")) { cleanup(); resolve(); } };
      const onExit = () => { cleanup(); reject(new Error(`Broker startup failed: ${stderr}`)); };
      const timer = setTimeout(() => { cleanup(); reject(new Error(`Broker startup timeout: ${stderr}`)); }, 10_000);
      child.stdout!.on("data", onData);
      child.once("exit", onExit);
    });
    return child;
  } catch (error) { child.kill("SIGTERM"); throw error; }
}

async function fixture(t: test.TestContext, tcp = false) {
  const root = mkdtempSync(path.join(process.platform === "win32" ? tmpdir() : "/tmp", "pa-"));
  const children: ChildProcess[] = [];
  const sockets: Duplex[] = [];
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await Promise.all(children.map(async (child) => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      child.kill("SIGTERM");
      await once(child, "exit");
    }));
    rmSync(root, { recursive: true, force: true });
  });
  const localDir = path.join(root, "a");
  const remoteDir = path.join(root, "b");
  children.push(await startBroker(localDir, tcp));
  children.push(await startBroker(remoteDir, tcp));
  const target = (dir: string) => tcp ? readBrokerTcpEndpoint(getParleyDirPath(dir)) : getBrokerSocketPath(process.platform, dir);
  const localBroker = target(localDir);
  const remoteBroker = target(remoteDir);
  const origin = async (broker: BrokerConnectTarget) => {
    const socket = connect(broker); sockets.push(socket); await once(socket, "connect");
    const result = await new Inbox(socket).request({ type: "broker_list_scopes", ...auth(broker) }, "broker_list_scopes_result");
    assert.equal(result.ok, true);
    socket.destroy();
    return result.localOrigin as { id: string };
  };
  const localOrigin = await origin(localBroker);
  const remoteOrigin = await origin(remoteBroker);
  const options = (stream: Duplex): AttachPeerStreamOptions => ({
    localBroker, stream, localOrigin, remoteOrigin,
    localScopeBindings: [{ localScopeId: "local-authority", localScopeAlias: "local", remoteScopeAlias: "remote" }],
    remoteScopeBindings: [{ localScopeId: "remote-authority", localScopeAlias: "remote", remoteScopeAlias: "local" }],
    ...(tcp ? { remoteStateId: (remoteBroker as Exclude<BrokerConnectTarget, string>).stateId } : {}),
  });
  const stream = async () => {
    const socket = connect(remoteBroker); sockets.push(socket); await once(socket, "connect"); return socket;
  };
  const ordinary = async (broker: BrokerConnectTarget, id: string, scopeId: string) => {
    const socket = connect(broker); sockets.push(socket); await once(socket, "connect");
    const inbox = new Inbox(socket);
    const result = inbox.next((v) => v.type === "registered");
    writeMessage(socket, { type: "register", sessionId: id, scopeId, ...auth(broker), session: {
      name: id, cwd: process.cwd(), model: "attachment-test", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now(),
    } });
    await result;
    return inbox;
  };
  const local = await ordinary(localBroker, "local-client", "local-authority");
  const remote = await ordinary(remoteBroker, "remote-client", "remote-authority");
  return { options, stream, local, remote, localBroker, remoteBroker, ordinary, sockets };
}

async function roster(inbox: Inbox, present: boolean): Promise<SessionInfo | undefined> {
  for (let attempt = 0; attempt < 150; attempt++) {
    const imported = (await inbox.sessions()).find((session) => session.federation);
    if (!!imported === present) return imported;
    await delay(10);
  }
  throw new Error(`Imported roster did not become ${present ? "present" : "absent"}`);
}
async function send(from: Inbox, to: Inbox, recipient: SessionInfo, text: string) {
  const id = randomUUID();
  const delivery = from.next((v) => (v.type === "delivered" || v.type === "delivery_failed") && v.messageId === id);
  const received = to.next((v) => v.type === "message" && (v.message as { id?: string })?.id === id);
  void received.catch(() => undefined);
  writeMessage(from.socket, { type: "send", to: recipient.id, message: { id, timestamp: Date.now(), content: { text } } });
  const result = await delivery;
  assert.equal(result.type, "delivered", JSON.stringify(result));
  assert.equal(((await received).message as { content: { text: string } }).content.text, text);
}

/** A provider-neutral adapter with controllable write acknowledgement. */
class ObservedDuplex extends Duplex {
  opaque = false;
  sizes: number[] = [];
  activeWrites = 0;
  maximumActiveWrites = 0;
  hold?: ReturnType<typeof deferred<void>>;
  admitted = deferred<void>();
  constructor(readonly socket: net.Socket) {
    super({ allowHalfOpen: true });
    socket.pause();
    socket.on("data", (bytes: Buffer) => { if (!this.push(bytes)) socket.pause(); });
    socket.on("end", () => this.push(null));
    socket.on("error", (error) => this.destroy(error));
    socket.on("close", () => { if (!socket.readableEnded && !this.destroyed) this.destroy(); });
  }
  override _read() { this.socket.resume(); }
  override _write(bytes: Buffer, _encoding: BufferEncoding, callback: (error?: Error | null) => void) {
    if (this.opaque) {
      this.sizes.push(bytes.length);
      this.activeWrites++;
      this.maximumActiveWrites = Math.max(this.maximumActiveWrites, this.activeWrites);
      this.admitted.resolve();
    }
    const opaque = this.opaque;
    const hold = this.hold;
    this.socket.write(bytes, (error) => {
      void (hold?.promise ?? Promise.resolve()).then(() => {
        if (opaque) this.activeWrites--;
        callback(error);
      });
    });
  }
  override _final(callback: (error?: Error | null) => void) { this.socket.end(callback); }
  override _destroy(error: Error | null, callback: (error?: Error | null) => void) {
    if (this.socket.closed) { callback(error); return; }
    this.socket.once("close", () => callback(error));
    this.socket.destroy();
  }
}

test("owned attachment establishes real broker readiness, byte ordering and independent scope authority", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  const provider = new ObservedDuplex(await f.stream()); f.sockets.push(provider);
  const link = await attachPeerStream(f.options(provider));
  assert.match(link.linkId, /^[a-zA-Z0-9_-]+$/);
  assert.deepEqual(link.localOrigin, f.options(provider).localOrigin);
  provider.opaque = true;
  const remote = (await roster(f.local, true))!;
  const local = (await roster(f.remote, true))!;
  assert.equal(remote.federation!.remoteScopeAlias, "remote");
  assert.equal(local.federation!.remoteScopeAlias, "local");
  const privateClient = await f.ordinary(f.remoteBroker, "unmapped", "not-authorized");
  assert.equal((await privateClient.sessions()).some((session) => session.federation), false);
  const text = "\u0000\r\n🛰️ é 漢字 " + "漢".repeat(30_000);
  await send(f.local, f.remote, remote, text);
  await send(f.remote, f.local, local, "reverse 🦉\u0000\n");
  for (const text of ["first", "second", "third"]) await send(f.local, f.remote, remote, text);
  assert.ok(provider.sizes.reduce((total, size) => total + size, 0) > PEER_STREAM_COPY_BYTES);
  assert.ok(provider.sizes.every((size) => size <= PEER_STREAM_COPY_BYTES));
  assert.equal(provider.maximumActiveWrites, 1);
  const close = link.close();
  assert.strictEqual(close, link.close());
  assert.deepEqual(await close, { status: "closed", reason: "close" });
  assert.equal(provider.destroyed, true);
  assert.equal(provider.socket.closed, true);
  await roster(f.local, false);
  await roster(f.remote, false);
});

test("wrong persisted origins or scope mappings fail closed and destroy the supplied stream", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  for (const invalid of [
    { localOrigin: { id: "host:wrong-local" } },
    { remoteOrigin: { id: "host:wrong-remote" } },
    { remoteScopeBindings: [{ localScopeId: "remote-authority", localScopeAlias: "not-remote", remoteScopeAlias: "local" }] },
  ]) {
    const stream = await f.stream();
    await assert.rejects(attachPeerStream({ ...f.options(stream), ...invalid }), (error: unknown) => {
      assert.ok(error instanceof PeerStreamAttachmentError);
      // A rejected handshake can close the peer socket before its control
      // failure arrives; either way ownership and broker state must fail closed.
      assert.ok(["E_ORIGIN_MISMATCH", "E_SCOPE_MISMATCH", "E_CLOSED"].includes(error.code), error.code);
      return true;
    });
    assert.equal(stream.destroyed, true);
    assert.equal(stream.closed, true);
    await roster(f.local, false);
    await roster(f.remote, false);
  }
});

test("ownership includes already-aborted, invalid, failed startup and handshake timeout", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  for (const [code, overrides] of [
    ["E_ABORTED", { signal: AbortSignal.abort(new Error("do not expose my abort reason")) }],
    ["E_INVALID_OPTIONS", { handshakeTimeoutMs: 0 }],
    ["E_INVALID_OPTIONS", { remoteScopeBindings: [] }],
    ["E_INVALID_OPTIONS", { signal: {} as AbortSignal }],
    ["E_STREAM_FAILED", { localBroker: path.join("/tmp", `absent-${randomUUID()}`) }],
  ] satisfies [string, Partial<AttachPeerStreamOptions>][]) {
    const stream = await f.stream();
    await assert.rejects(attachPeerStream({ ...f.options(stream), ...overrides }), (error: unknown) => {
      assert.ok(error instanceof PeerStreamAttachmentError);
      assert.equal(error.code, code);
      return true;
    });
    assert.equal(stream.destroyed, true);
    assert.equal(stream.closed, true);
  }
  const silent = new Duplex({ read() {}, write(_bytes, _encoding, callback) { callback(); } });
  await assert.rejects(attachPeerStream({ ...f.options(silent), handshakeTimeoutMs: 30 }), { code: "E_TIMEOUT" });
  assert.equal(silent.destroyed, true);
  let destroyed = false;
  const noCloseEvent = new Duplex({
    emitClose: false, read() {}, write(_bytes, _encoding, callback) { callback(); },
    destroy(error, callback) { setTimeout(() => { destroyed = true; callback(error); }, 30); },
  });
  await assert.rejects(attachPeerStream({ ...f.options(noCloseEvent), signal: AbortSignal.abort() }), { code: "E_ABORTED" });
  assert.equal(destroyed, true, "startup rejection joins _destroy without relying on close events");
  await roster(f.local, false);
  await roster(f.remote, false);
});

test("close joins an admitted write even when its acknowledgement follows destruction", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  const provider = new ObservedDuplex(await f.stream()); f.sockets.push(provider);
  const link = await attachPeerStream(f.options(provider));
  const remote = (await roster(f.local, true))!;
  await roster(f.remote, true);
  provider.opaque = true;
  provider.hold = deferred<void>();
  writeMessage(f.local.socket, { type: "send", to: remote.id, message: {
    id: randomUUID(), timestamp: Date.now(), content: { text: "漢".repeat(30_000) },
  } });
  await provider.admitted.promise;
  let settled = false;
  const completion = link.close().then((outcome) => { settled = true; return outcome; });
  await delay(30);
  assert.equal(provider.destroyed, true);
  assert.equal(settled, false, "close must join the admitted Node write callback");
  assert.equal(provider.sizes.length, 1, "no further opaque write admitted after close");
  provider.hold.resolve();
  assert.deepEqual(await completion, { status: "closed", reason: "close" });
  assert.equal(provider.activeWrites, 0);
  await roster(f.local, false);
  await roster(f.remote, false);
});

test("active abort and stream error produce observed completion and remove both rosters", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  for (const mode of ["abort", "error", "close"] as const) {
    const controller = new AbortController();
    const stream = await f.stream();
    const link = await attachPeerStream({ ...f.options(stream), signal: controller.signal });
    await roster(f.local, true); await roster(f.remote, true);
    if (mode === "abort") controller.abort(new Error("private abort diagnostic"));
    else if (mode === "error") stream.destroy(new Error("private capability secret"));
    else stream.destroy();
    const outcome = await link.completion;
    if (mode === "error") {
      assert.equal(outcome.status, "failure");
      if (outcome.status === "failure") assert.equal(outcome.error.message.includes("private"), false);
    } else assert.deepEqual(outcome, { status: "closed", reason: mode === "abort" ? "aborted" : "stream" });
    assert.deepEqual(await link.close(), outcome);
    await roster(f.local, false); await roster(f.remote, false);
  }
});

test("a binary Duplex.from stdio-style pair works with destination TCP credentials", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t, true);
  const socket = await f.stream();
  const readable = new PassThrough();
  socket.pipe(readable);
  // Installed @types/node omits the documented Node-stream pair overload.
  const fromNodePair = Duplex.from as unknown as (pair: { readable: NodeJS.ReadableStream; writable: NodeJS.WritableStream }) => Duplex;
  const provider = fromNodePair({ readable, writable: socket }); f.sockets.push(provider);
  const link = await attachPeerStream(f.options(provider));
  const remote = (await roster(f.local, true))!;
  const local = (await roster(f.remote, true))!;
  await send(f.local, f.remote, remote, "stdio adapted");
  await send(f.remote, f.local, local, "reverse stdio adapted");
  await link.close();
  assert.equal(provider.destroyed, true);
  assert.equal(readable.destroyed, true);
  assert.equal(socket.destroyed, true);
  await roster(f.local, false); await roster(f.remote, false);
  const bad = await f.stream();
  await assert.rejects(attachPeerStream({ ...f.options(bad), remoteStateId: randomUUID() }));
  assert.equal(bad.closed, true);
});

/** Force real preparation+ack frames to arrive together: destination receives
 * preparation and the independently known equivalent hello in one write. The
 * dialer's duplicate first hello is stripped; all remaining peer bytes are opaque. */
class CoalescedPreparationDuplex extends ObservedDuplex {
  private prepared = false;
  private droppedHello = false;
  private outgoing = Buffer.alloc(0);
  private incoming = Buffer.alloc(0);
  combined = false;
  fragmented = false;
  constructor(socket: net.Socket) {
    super(socket);
    socket.removeAllListeners("data");
    socket.on("data", (chunk: Buffer) => {
      if (this.combined) {
        if (!this.fragmented) this.incoming = Buffer.concat([this.incoming, chunk]);
        else if (!this.push(chunk)) socket.pause();
        return;
      }
      this.incoming = Buffer.concat([this.incoming, chunk]);
      if (this.incoming.length < 4) return;
      const firstEnd = 4 + this.incoming.readUInt32BE(0);
      if (this.incoming.length < firstEnd + 4) return;
      const secondEnd = firstEnd + 4 + this.incoming.readUInt32BE(firstEnd);
      if (this.incoming.length < secondEnd) return;
      this.combined = true;
      // Fragment the first prefix, then coalesce its tail with the full real ack.
      this.push(this.incoming.subarray(0, 1));
      setTimeout(() => {
        this.fragmented = true;
        if (!this.push(this.incoming.subarray(1))) socket.pause();
        this.incoming = Buffer.alloc(0);
      }, 10);
    });
  }
  override _write(bytes: Buffer, encoding: BufferEncoding, callback: (error?: Error | null) => void) {
    if (!this.prepared) {
      this.prepared = true;
      const prepare = JSON.parse(bytes.subarray(4).toString()) as BrokerAcceptPeerRequest;
      const hello = encode({
        type: "peer_hello", protocol: FEDERATION_PROTOCOL_NAME, version: FEDERATION_PROTOCOL_VERSION,
        linkId: prepare.linkId, origin: prepare.remoteOrigin, expectedPeerOrigin: prepare.localOrigin,
        scopeMappings: prepare.scopeBindings.map((b) => ({ localScopeAlias: b.remoteScopeAlias, remoteScopeAlias: b.localScopeAlias })),
        features: [...FEDERATION_SUPPORTED_FEATURES],
      });
      this.socket.write(Buffer.concat([bytes, hello]), callback);
      return;
    }
    if (!this.droppedHello) {
      this.outgoing = Buffer.concat([this.outgoing, bytes]);
      if (this.outgoing.length < 4 || this.outgoing.length < 4 + this.outgoing.readUInt32BE(0)) { callback(); return; }
      const end = 4 + this.outgoing.readUInt32BE(0);
      assert.equal(JSON.parse(this.outgoing.subarray(4, end).toString()).type, "peer_hello");
      this.droppedHello = true;
      const leftover = this.outgoing.subarray(end);
      this.outgoing = Buffer.alloc(0);
      if (leftover.length) super._write(leftover, encoding, callback);
      else callback();
      return;
    }
    super._write(bytes, encoding, callback);
  }
}

test("fragmented preparation and coalesced leftover real peer frames are not dropped", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  const provider = new CoalescedPreparationDuplex(await f.stream()); f.sockets.push(provider);
  const link = await attachPeerStream(f.options(provider));
  assert.equal(provider.combined, true);
  assert.equal(provider.fragmented, true);
  const remote = (await roster(f.local, true))!;
  const local = (await roster(f.remote, true))!;
  await send(f.local, f.remote, remote, "preserved after coalesced preparation");
  await send(f.remote, f.local, local, "preserved reverse");
  await link.close();
  await roster(f.local, false); await roster(f.remote, false);
});

test("provider registration is lazy and opaque; registration/controller close join live links", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  const controller = new PeerStreamController();
  const binding = Object.freeze({ providerToken: Symbol("an opaque exact counterpart") });
  let acquisitions = 0;
  const provider = controller.registerProvider(async (received: typeof binding, signal) => {
    acquisitions++;
    assert.strictEqual(received, binding);
    assert.equal(signal.aborted, false);
    return await f.stream();
  });
  const { stream: _unused, ...options } = f.options(f.local.socket);
  await delay(10);
  assert.equal(acquisitions, 0, "registration alone never acquires or connects");
  const link = await provider.attach(binding, options);
  assert.equal(acquisitions, 1);
  const remote = (await roster(f.local, true))!;
  await roster(f.remote, true);
  await send(f.local, f.remote, remote, "registered provider's opaque stream");
  const close = provider.close();
  assert.strictEqual(close, provider.close());
  await close;
  assert.equal((await link.completion).status, "closed");
  await roster(f.local, false); await roster(f.remote, false);
  await assert.rejects(provider.attach(binding, options), { code: "E_CLOSED" });
  assert.equal(acquisitions, 1);
  // Closing one registration does not revoke another or the direct-stream API.
  const owned = await controller.attachOwnedStream(f.options(await f.stream()));
  await roster(f.local, true); await roster(f.remote, true);
  const controllerClose = controller.close();
  assert.strictEqual(controllerClose, controller.close());
  await controllerClose;
  assert.equal((await owned.completion).status, "closed");
  await roster(f.local, false); await roster(f.remote, false);
  assert.throws(() => controller.registerProvider(async () => await f.stream()), { code: "E_CLOSED" });
  const revokedStream = await f.stream();
  await assert.rejects(controller.attachOwnedStream(f.options(revokedStream)), { code: "E_CLOSED" });
  assert.equal(revokedStream.closed, true, "revoked admission still owns a supplied stream");
});

test("revocation joins ignored cancellation and asynchronous destruction of late acquired streams", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  const { stream: _unused, ...options } = f.options(f.local.socket);
  for (const mode of ["registration", "controller"] as const) {
    const controller = new PeerStreamController();
    const acquired = deferred<Duplex>();
    let factorySignal: AbortSignal | undefined;
    let acquisitions = 0;
    const registration = controller.registerProvider(async (binding: { foreign: string }, signal) => {
      assert.equal(binding.foreign, "not a Parley origin or alias");
      acquisitions++;
      factorySignal = signal;
      return await acquired.promise; // Deliberately ignore cancellation until later.
    });
    const pending = registration.attach({ foreign: "not a Parley origin or alias" }, options);
    const rejected = assert.rejects(pending, { code: "E_CLOSED" });
    assert.equal(acquisitions, 1);
    const closing = mode === "registration" ? registration.close() : controller.close();
    assert.equal(factorySignal!.aborted, true);
    let settled = false;
    void closing.then(() => { settled = true; });
    await delay(20);
    assert.equal(settled, false, "revocation must join the started acquisition");
    await assert.rejects(registration.attach({ foreign: "revoked" }, options), { code: "E_CLOSED" });
    assert.equal(acquisitions, 1);
    let destructionFinished = false;
    let writes = 0;
    const late = new Duplex({
      emitClose: false,
      read() {},
      write(_bytes, _encoding, callback) { writes++; callback(); },
      destroy(error, callback) { setTimeout(() => { destructionFinished = true; callback(error); }, 40); },
    });
    acquired.resolve(late);
    await delay(10);
    assert.equal(late.destroyed, true);
    assert.equal(settled, false, "revocation must also join the late stream's _destroy");
    await Promise.all([closing, rejected]);
    assert.equal(late.closed, true);
    assert.equal(destructionFinished, true);
    assert.equal(writes, 0, "late acquired stream must never be prepared or connected");
    await controller.close();
    await roster(f.local, false); await roster(f.remote, false);
  }
});

test("abort during startup joins admitted preparation before rejecting", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  const provider = new ObservedDuplex(await f.stream()); f.sockets.push(provider);
  provider.opaque = true;
  provider.hold = deferred<void>();
  const controller = new AbortController();
  const startup = attachPeerStream({ ...f.options(provider), signal: controller.signal });
  let settled = false;
  const rejected = assert.rejects(startup, { code: "E_ABORTED" }).then(() => { settled = true; });
  await provider.admitted.promise;
  controller.abort();
  await delay(20);
  assert.equal(provider.destroyed, true);
  assert.equal(settled, false);
  assert.equal(provider.sizes.length, 1);
  provider.hold.resolve();
  await rejected;
  assert.equal(provider.activeWrites, 0);
  assert.equal(provider.socket.closed, true);
  await roster(f.local, false); await roster(f.remote, false);
});

class ReceiveEOFProvider extends ObservedDuplex {
  private eof = false;
  finalCalls = 0;
  override _read() { if (!this.eof) super._read(); }
  receiveEOF() {
    this.eof = true;
    this.socket.pause();
    this.push(null);
  }
  override _final(callback: (error?: Error | null) => void) {
    this.finalCalls++;
    super._final(callback);
  }
}

test("readable EOF half-closes sending and joins normal end through two brokers", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  const provider = new ReceiveEOFProvider(await f.stream()); f.sockets.push(provider);
  const link = await attachPeerStream(f.options(provider));
  const remote = (await roster(f.local, true))!;
  const local = (await roster(f.remote, true))!;
  await send(f.local, f.remote, remote, "before EOF");
  await send(f.remote, f.local, local, "reverse before EOF");
  assert.equal(provider.finalCalls, 0);
  provider.receiveEOF();
  const outcome = await link.completion;
  assert.deepEqual(outcome, { status: "end" });
  assert.equal(provider.finalCalls, 1);
  assert.equal(provider.activeWrites, 0);
  assert.equal(provider.closed, true);
  assert.deepEqual(await link.close(), { status: "end" });
  await roster(f.local, false); await roster(f.remote, false);
});

class HeldPeerRepliesDuplex extends ObservedDuplex {
  private preparation = Buffer.alloc(0);
  private prepared = false;
  private released = false;
  private held: Buffer[] = [];
  peerReplyHeld = deferred<void>();
  constructor(socket: net.Socket) {
    super(socket);
    socket.removeAllListeners("data");
    const deliverPeer = (bytes: Buffer) => {
      if (!bytes.length) return;
      if (this.released) { if (!this.push(bytes)) socket.pause(); }
      else { this.held.push(bytes); this.peerReplyHeld.resolve(); }
    };
    socket.on("data", (bytes: Buffer) => {
      if (this.prepared) { deliverPeer(bytes); return; }
      this.preparation = Buffer.concat([this.preparation, bytes]);
      if (this.preparation.length < 4 || this.preparation.length < 4 + this.preparation.readUInt32BE(0)) return;
      const end = 4 + this.preparation.readUInt32BE(0);
      this.prepared = true;
      this.push(this.preparation.subarray(0, end));
      deliverPeer(this.preparation.subarray(end));
      this.preparation = Buffer.alloc(0);
    });
  }
  releasePeerReplies() {
    this.released = true;
    if (!this.push(Buffer.concat(this.held))) this.socket.pause();
    this.held = [];
  }
}

test("readiness waits for broker handshake acceptance, not preparation or opaque forwarding setup", { timeout: 20_000 }, async (t) => {
  const f = await fixture(t);
  const provider = new HeldPeerRepliesDuplex(await f.stream()); f.sockets.push(provider);
  let ready = false;
  const startup = attachPeerStream(f.options(provider));
  void startup.then(() => { ready = true; }, () => undefined);
  await provider.peerReplyHeld.promise;
  // Destination preparation and outbound hello have completed, but the dialing
  // broker cannot accept the handshake until its actual peer ack is forwarded.
  await delay(20);
  assert.equal(ready, false);
  provider.releasePeerReplies();
  const link = await startup;
  assert.equal(ready, true);
  const remote = (await roster(f.local, true))!;
  await roster(f.remote, true);
  await send(f.local, f.remote, remote, "after true readiness");
  await link.close();
  await roster(f.local, false); await roster(f.remote, false);
});
