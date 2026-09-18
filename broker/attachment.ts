import net from "node:net";
import { randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { Duplex } from "node:stream";
import type { BrokerConnectTarget } from "./paths.ts";
import { MAX_FRAME_BYTES } from "./framing.ts";
import {
  isBrokerAcceptPeerRequest,
  isBrokerAcceptPeerResult,
  isBrokerDialPeerRequest,
  isBrokerDialPeerResult,
  isFederationBridgeAttach,
} from "./federation-protocol.ts";
import type {
  BrokerAcceptPeerRequest,
  BrokerDialPeerRequest,
  FederationFailureCode,
  FederationOrigin,
  FederationScopeBinding,
} from "./federation-types.ts";

/** Maximum copied bytes in each admitted opaque write; at most one per direction. */
export const PEER_STREAM_COPY_BYTES = 64 * 1024;

export interface AttachPeerStreamOptions {
  /** Explicit local endpoint; TCP targets must use numeric loopback. */
  localBroker: BrokerConnectTarget;
  /** Already authenticated to the exact destination broker. Ownership transfers
   * immediately, including on validation/abort/startup failure. Must be a binary
   * Node Duplex with normal write-callback and destruction semantics. No retry,
   * reopening, discovery or replay is performed. Duplex.from({readable,writable})
   * can adapt a provider's stdio without exposing that provider to this module. */
  stream: Duplex;
  localOrigin: FederationOrigin;
  remoteOrigin: FederationOrigin;
  localScopeBindings: FederationScopeBinding[];
  /** Independently authorized destination bindings, not inferred local authority. */
  remoteScopeBindings: FederationScopeBinding[];
  /** Destination broker TCP authentication, if required. Never inferred. */
  remoteStateId?: string;
  /** Cancels startup or closes the active attachment. */
  signal?: AbortSignal;
  /** Entire broker-handshake deadline, not a forwarding-setup deadline. Default 10s. */
  handshakeTimeoutMs?: number;
}

export type PeerStreamAttachmentErrorCode = FederationFailureCode
  | "E_INVALID_OPTIONS" | "E_ABORTED" | "E_TIMEOUT" | "E_STREAM_FAILED" | "E_CLOSED";

/** Diagnostics deliberately contain no capabilities, endpoint credentials or raw frames. */
export class PeerStreamAttachmentError extends Error {
  constructor(readonly code: PeerStreamAttachmentErrorCode) {
    super(`Peer stream attachment failed (${code})`);
    this.name = "PeerStreamAttachmentError";
  }
}

export type PeerStreamCompletion =
  | { status: "end" }
  | { status: "closed"; reason: "close" | "aborted" | "stream" }
  | { status: "failure"; error: PeerStreamAttachmentError };

export interface PeerStreamAttachment {
  /** Ready means the brokers accepted their peer handshake, not merely setup. */
  readonly linkId: string;
  readonly localOrigin: Readonly<FederationOrigin>;
  readonly remoteOrigin: Readonly<FederationOrigin>;
  /** Never rejects. Resolves after admitted write callbacks and owned cleanup join. */
  readonly completion: Promise<PeerStreamCompletion>;
  /** Stops admissions, destroys owned resources and joins completion. Idempotent. */
  close(): Promise<PeerStreamCompletion>;
}

function failure(code: PeerStreamAttachmentErrorCode): PeerStreamAttachmentError {
  return new PeerStreamAttachmentError(code);
}

/** Public `closed` joins _destroy even on streams configured emitClose:false.
 * Poll only during teardown; a turn boundary also drains deferred error events. */
function joinStreamClose(stream: Duplex): Promise<void> {
  return new Promise((resolve) => {
    let timer: NodeJS.Timeout;
    const done = () => { clearTimeout(timer); stream.off("close", done); resolve(); };
    const poll = () => {
      if (stream.closed) done();
      else timer = setTimeout(poll, 10);
    };
    stream.once("close", done);
    timer = setTimeout(poll, 0);
  });
}

async function discardOwnedStream(stream: Duplex): Promise<void> {
  if (!(stream instanceof Duplex)) throw failure("E_INVALID_OPTIONS");
  const onError = () => undefined;
  stream.on("error", onError);
  stream.pause();
  stream.destroy();
  await joinStreamClose(stream);
  stream.off("error", onError);
}

function frame(value: unknown): Buffer {
  const payload = Buffer.from(JSON.stringify(value), "utf8");
  const result = Buffer.allocUnsafe(4 + payload.length);
  result.writeUInt32BE(payload.length, 0);
  payload.copy(result, 4);
  return result;
}

/** Private lifecycle: all asynchronous work is observed, including shutdown races. */
class OwnedAttachment {
  private readonly controller = new AbortController();
  private readonly streams = new Set<Duplex>();
  private readonly monitored = new Set<Duplex>();
  private readonly writes = new Set<Promise<void>>();
  private readonly pumps: Promise<void>[] = [];
  private readonly listeners: (() => void)[] = [];
  private server?: net.Server;
  private serverClosed: Promise<void> = Promise.resolve();
  private outcome?: PeerStreamCompletion;
  private startupError?: PeerStreamAttachmentError;
  private resolveCompletion!: (outcome: PeerStreamCompletion) => void;
  readonly completion = new Promise<PeerStreamCompletion>((resolve) => { this.resolveCompletion = resolve; });

  constructor(stream: Duplex) {
    this.own(stream);
  }

  bindSignal(signal?: AbortSignal): void {
    if (signal !== undefined && !(signal instanceof AbortSignal)) throw failure("E_INVALID_OPTIONS");
    if (signal) {
      const onAbort = () => this.stop({ status: "closed", reason: "aborted" }, failure("E_ABORTED"));
      signal.addEventListener("abort", onAbort, { once: true });
      this.listeners.push(() => signal.removeEventListener("abort", onAbort));
      if (signal.aborted) onAbort();
    }
  }

  private own(stream: Duplex): void {
    stream.pause();
    // Disable Node's default socket auto-end: a readable EOF must not close the
    // opposite direction before its already-admitted writes and independent EOF.
    stream.allowHalfOpen = true;
    this.streams.add(stream);
    this.monitored.add(stream);
    const onError = () => {
      if (this.monitored.has(stream)) this.stop({ status: "failure", error: failure("E_STREAM_FAILED") });
    };
    const onClose = () => {
      if (this.monitored.has(stream) && !(stream.readableEnded && stream.writableFinished)) {
        this.stop({ status: "closed", reason: "stream" }, failure("E_CLOSED"));
      }
    };
    stream.on("error", onError);
    stream.on("close", onClose);
    this.listeners.push(() => { stream.off("error", onError); stream.off("close", onClose); });
  }

  check(): void {
    if (this.outcome) throw this.startupError ?? failure("E_CLOSED");
  }

  stop(outcome: PeerStreamCompletion, error?: PeerStreamAttachmentError): void {
    if (this.outcome) return;
    this.outcome = outcome;
    this.startupError = error ?? (outcome.status === "failure" ? outcome.error : failure("E_CLOSED"));
    // Synchronous admission barrier, before any asynchronous cleanup.
    this.controller.abort();
    for (const stream of this.streams) { stream.pause(); stream.destroy(); }
    this.server?.close();
    // Writers are joined even when their callbacks arrive after the close event.
    void this.finish();
  }

  private async finish(): Promise<void> {
    await Promise.allSettled([...this.writes, ...this.pumps, ...[...this.streams].map(joinStreamClose), this.serverClosed]);
    for (const remove of this.listeners) remove();
    this.resolveCompletion(this.outcome!);
  }

  private waitReadable(stream: Duplex): Promise<void> {
    this.check();
    return new Promise((resolve, reject) => {
      const cleanup = () => {
        stream.off("readable", onReady);
        stream.off("end", onReady);
        stream.off("close", onClose);
        this.controller.signal.removeEventListener("abort", onAbort);
      };
      const onReady = () => { cleanup(); resolve(); };
      const onClose = () => { cleanup(); reject(failure("E_CLOSED")); };
      const onAbort = () => { cleanup(); reject(this.startupError ?? failure("E_CLOSED")); };
      stream.once("readable", onReady);
      stream.once("end", onReady);
      stream.once("close", onClose);
      this.controller.signal.addEventListener("abort", onAbort, { once: true });
      if (this.controller.signal.aborted) onAbort();
      else if (stream.readableLength > 0 || stream.readableEnded) onReady();
      else if (stream.destroyed) onClose();
    });
  }

  private async read(stream: Duplex, maximum: number): Promise<Buffer | null> {
    while (true) {
      this.check();
      // read(0) asks Node to publish an EOF even when its readable queue is empty.
      // Waiting only for bytes would otherwise miss a push(null)/FIN with no tail.
      if (stream.readableLength === 0) stream.read(0);
      if (stream.readableLength > 0) {
        const chunk: unknown = stream.read(Math.min(maximum, stream.readableLength));
        if (chunk !== null) {
          if (!(chunk instanceof Uint8Array)) throw failure("E_INVALID_OPTIONS");
          return Buffer.from(chunk);
        }
      }
      if (stream.readableEnded) return null;
      if (stream.destroyed || !stream.readable) throw failure("E_CLOSED");
      await this.waitReadable(stream);
    }
  }

  private async readExact(stream: Duplex, length: number): Promise<Buffer> {
    const bytes = Buffer.allocUnsafe(length);
    let offset = 0;
    while (offset < length) {
      const chunk = await this.read(stream, Math.min(length - offset, PEER_STREAM_COPY_BYTES));
      if (!chunk) throw failure("E_HANDSHAKE_FAILED");
      chunk.copy(bytes, offset);
      offset += chunk.length;
    }
    return bytes;
  }

  async readFrame(stream: Duplex): Promise<unknown> {
    const header = await this.readExact(stream, 4);
    const length = header.readUInt32BE(0);
    if (length > MAX_FRAME_BYTES) throw failure("E_HANDSHAKE_FAILED");
    const payload = await this.readExact(stream, length);
    // Exact reads leave coalesced peer bytes in the stream's own readable queue.
    try { return JSON.parse(payload.toString("utf8")); }
    catch { throw failure("E_HANDSHAKE_FAILED"); }
  }

  private admit(start: (callback: (error?: Error | null) => void) => void): Promise<void> {
    this.check();
    const operation = new Promise<void>((resolve, reject) => {
      try { start((error) => error ? reject(failure("E_STREAM_FAILED")) : resolve()); }
      catch { reject(failure("E_STREAM_FAILED")); }
    });
    this.writes.add(operation);
    void operation.then(() => this.writes.delete(operation), () => this.writes.delete(operation));
    return operation;
  }

  write(stream: Duplex, bytes: Buffer): Promise<void> {
    return this.admit((callback) => { stream.write(bytes, callback); });
  }

  async connect(target: BrokerConnectTarget): Promise<net.Socket> {
    this.check();
    const socket = typeof target === "string" ? net.connect(target) : net.connect({ host: target.host, port: target.port });
    this.own(socket);
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        socket.off("connect", onConnect);
        socket.off("error", onError);
        this.controller.signal.removeEventListener("abort", onAbort);
      };
      const onConnect = () => { cleanup(); resolve(); };
      const onError = () => { cleanup(); reject(failure("E_DIAL_FAILED")); };
      const onAbort = () => { cleanup(); reject(this.startupError ?? failure("E_CLOSED")); };
      socket.once("connect", onConnect);
      socket.once("error", onError);
      this.controller.signal.addEventListener("abort", onAbort, { once: true });
      if (this.controller.signal.aborted) onAbort();
    });
    return socket;
  }

  retireControl(socket: net.Socket): void {
    this.monitored.delete(socket);
    socket.destroy();
  }

  async listen(): Promise<{ port: number; accepted: Promise<net.Socket> }> {
    this.check();
    let resolveAccepted!: (socket: net.Socket) => void;
    let rejectAccepted!: (error: Error) => void;
    const accepted = new Promise<net.Socket>((resolve, reject) => { resolveAccepted = resolve; rejectAccepted = reject; });
    // A failure before startup awaits this promise must not be unhandled.
    void accepted.catch(() => undefined);
    let consumed = false;
    const server = net.createServer({ allowHalfOpen: true, pauseOnConnect: true }, (socket) => {
      if (consumed || this.outcome) { socket.destroy(); return; }
      consumed = true;
      this.own(socket);
      server.close();
      resolveAccepted(socket);
    });
    this.server = server;
    this.serverClosed = new Promise<void>((resolve) => { server.once("close", resolve); });
    const onAbort = () => { rejectAccepted(this.startupError ?? failure("E_CLOSED")); };
    this.controller.signal.addEventListener("abort", onAbort, { once: true });
    this.listeners.push(() => this.controller.signal.removeEventListener("abort", onAbort));
    server.on("error", () => {
      rejectAccepted(failure("E_DIAL_FAILED"));
      this.stop({ status: "failure", error: failure("E_DIAL_FAILED") });
    });
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        server.off("listening", onListening);
        this.controller.signal.removeEventListener("abort", onListenAbort);
      };
      const onListening = () => { cleanup(); resolve(); };
      const onListenAbort = () => { cleanup(); reject(this.startupError ?? failure("E_CLOSED")); };
      server.once("listening", onListening);
      this.controller.signal.addEventListener("abort", onListenAbort, { once: true });
      server.listen({ host: "127.0.0.1", port: 0, signal: this.controller.signal });
    });
    this.check();
    const address = server.address();
    if (!address || typeof address === "string") throw failure("E_DIAL_FAILED");
    return { port: address.port, accepted };
  }

  startCopy(local: Duplex, remote: Duplex): void {
    this.check();
    const copy = async (source: Duplex, destination: Duplex) => {
      while (true) {
        const bytes = await this.read(source, PEER_STREAM_COPY_BYTES);
        if (bytes === null) {
          // EOF closes only this sending half, after all admitted writes settle.
          await this.admit((callback) => { destination.end(callback); });
          return;
        }
        await this.write(destination, bytes);
      }
    };
    for (const [source, destination] of [[local, remote], [remote, local]]) {
      const pump = copy(source!, destination!);
      this.pumps.push(pump);
    }
    void Promise.all(this.pumps).then(
      () => this.stop({ status: "end" }),
      (error: unknown) => this.stop({ status: "failure", error: error instanceof PeerStreamAttachmentError ? error : failure("E_STREAM_FAILED") }),
    );
  }
}

/** Attach the local broker to one exact authenticated destination broker stream.
 * Resolves only after both brokers accept the peer handshake. Startup rejection
 * also joins destruction of the supplied stream. Caller supplies all authority;
 * peer traffic after preparation is copied opaquely, byte-for-byte, in order. */
export async function attachPeerStream(options: AttachPeerStreamOptions): Promise<PeerStreamAttachment> {
  if (!(options.stream instanceof Duplex)) throw failure("E_INVALID_OPTIONS");
  const owned = new OwnedAttachment(options.stream);
  let timer: NodeJS.Timeout | undefined;
  try {
    owned.bindSignal(options.signal);
    owned.check();
    const timeoutMs = options.handshakeTimeoutMs ?? 10_000;
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2_147_483_647
      || options.stream.readableObjectMode || options.stream.writableObjectMode
      || options.stream.readableEncoding || options.stream.destroyed
      || !options.stream.readable || !options.stream.writable) throw failure("E_INVALID_OPTIONS");
    const localOrigin = { ...options.localOrigin };
    const remoteOrigin = { ...options.remoteOrigin };
    const capability = randomBytes(32).toString("hex");
    const dial: BrokerDialPeerRequest = {
      type: "broker_dial_peer", requestId: randomUUID(),
      endpoint: { transport: "tcp", host: "127.0.0.1", port: 1 }, capability,
      localOrigin, remoteOrigin,
      scopeBindings: options.localScopeBindings?.map((binding) => ({ ...binding })),
      ...(typeof options.localBroker !== "string" && options.localBroker?.stateId !== undefined ? { stateId: options.localBroker.stateId } : {}),
    };
    const prepare: BrokerAcceptPeerRequest = {
      type: "broker_accept_peer", requestId: randomUUID(), linkId: randomUUID(),
      localOrigin: remoteOrigin, remoteOrigin: localOrigin,
      scopeBindings: options.remoteScopeBindings?.map((binding) => ({ ...binding })),
      ...(options.remoteStateId !== undefined ? { stateId: options.remoteStateId } : {}),
    };
    const target = options.localBroker;
    if (!(typeof target === "string" ? target.length > 0
      : target?.transport === "tcp" && (target.host === "127.0.0.1" || target.host === "::1")
        && Number.isSafeInteger(target.port) && target.port > 0 && target.port <= 65535)
      || !isBrokerDialPeerRequest(dial) || !isBrokerAcceptPeerRequest(prepare)) throw failure("E_INVALID_OPTIONS");
    timer = setTimeout(() => owned.stop({ status: "failure", error: failure("E_TIMEOUT") }), timeoutMs);
    const listener = await owned.listen();
    dial.endpoint.port = listener.port;
    const control = await owned.connect(target);
    await owned.write(control, frame(dial));
    const dialResult = owned.readFrame(control);
    // A control rejection can arrive while destination preparation is waiting.
    // Stop immediately rather than leaving the owned stream until the deadline.
    void dialResult.then((value) => {
      if (!isBrokerDialPeerResult(value) || value.requestId !== dial.requestId) {
        owned.stop({ status: "failure", error: failure("E_HANDSHAKE_FAILED") });
      } else if (!value.ok) owned.stop({ status: "failure", error: failure(value.code) });
    }, (error: unknown) => {
      owned.stop({ status: "failure", error: error instanceof PeerStreamAttachmentError ? error : failure("E_HANDSHAKE_FAILED") });
    });
    // Race the attachment against a broker rejection that never reaches the listener.
    const local = await Promise.race([
      listener.accepted,
      dialResult.then((value) => {
        if (!isBrokerDialPeerResult(value) || value.requestId !== dial.requestId) throw failure("E_HANDSHAKE_FAILED");
        if (!value.ok) throw failure(value.code);
        throw failure("E_HANDSHAKE_FAILED"); // Success cannot precede forwarding/handshake.
      }),
    ]);
    const attach = await owned.readFrame(local);
    if (!isFederationBridgeAttach(attach) || attach.capability.length !== capability.length
      || !timingSafeEqual(Buffer.from(attach.capability), Buffer.from(capability))) throw failure("E_HANDSHAKE_FAILED");
    prepare.linkId = attach.linkId;
    await owned.write(options.stream, frame(prepare));
    const prepared = await owned.readFrame(options.stream);
    if (!isBrokerAcceptPeerResult(prepared) || prepared.requestId !== prepare.requestId) throw failure("E_HANDSHAKE_FAILED");
    if (!prepared.ok) throw failure(prepared.code);
    if (prepared.linkId !== attach.linkId) throw failure("E_HANDSHAKE_FAILED");
    owned.startCopy(local, options.stream);
    const ready = await dialResult;
    if (!isBrokerDialPeerResult(ready) || ready.requestId !== dial.requestId) throw failure("E_HANDSHAKE_FAILED");
    if (!ready.ok) throw failure(ready.code);
    if (ready.linkId !== attach.linkId) throw failure("E_HANDSHAKE_FAILED");
    owned.check();
    clearTimeout(timer);
    owned.retireControl(control);
    return Object.freeze({
      linkId: ready.linkId,
      localOrigin: Object.freeze(localOrigin), remoteOrigin: Object.freeze(remoteOrigin),
      completion: owned.completion,
      close: () => { owned.stop({ status: "closed", reason: "close" }); return owned.completion; },
    });
  } catch (error) {
    const safeError = error instanceof PeerStreamAttachmentError ? error : failure("E_INVALID_OPTIONS");
    owned.stop({ status: "failure", error: safeError });
    await owned.completion;
    throw safeError;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export type PeerStreamAttachOptions = Omit<AttachPeerStreamOptions, "stream">;

/** Acquisition is provider-owned: binding is opaque to Parley. Factories must
 * eventually settle after cancellation; close joins rather than abandons them. */
export type PeerStreamProviderFactory<TBinding> = (binding: TBinding, signal: AbortSignal) => Promise<Duplex>;

export interface PeerStreamProviderRegistration<TBinding> {
  /** Registration itself never acquires. Each call makes one acquisition and
   * one attachment attempt, without reconnect/retry/replay. */
  attach(binding: TBinding, options: PeerStreamAttachOptions): Promise<PeerStreamAttachment>;
  /** Revokes admissions, aborts and joins acquisitions/attachments. Late streams
   * are destroyed and joined, never handed to a broker. Idempotent. */
  close(): Promise<void>;
}

function combineSignals(internal: AbortSignal, external?: AbortSignal): { signal: AbortSignal; dispose: () => void } {
  if (external !== undefined && !(external instanceof AbortSignal)) throw failure("E_INVALID_OPTIONS");
  if (!external) return { signal: internal, dispose: () => undefined };
  const combined = new AbortController();
  const onAbort = () => combined.abort(); // Do not propagate provider/caller diagnostics.
  for (const signal of [internal, external]) signal.addEventListener("abort", onAbort, { once: true });
  if (internal.aborted || external.aborted) onAbort();
  return {
    signal: combined.signal,
    dispose: () => { for (const signal of [internal, external]) signal.removeEventListener("abort", onAbort); },
  };
}

class AttachmentAdmissions {
  private closed = false;
  private readonly admitted = new Set<{ abort: () => void; joined: Promise<void> }>();
  private closing?: Promise<void>;

  attachOwned(options: AttachPeerStreamOptions): Promise<PeerStreamAttachment> {
    return this.start(options, options.stream);
  }

  acquire<TBinding>(factory: PeerStreamProviderFactory<TBinding>, binding: TBinding, options: PeerStreamAttachOptions): Promise<PeerStreamAttachment> {
    return this.start(options, undefined, (signal) => factory(binding, signal));
  }

  private start(options: PeerStreamAttachOptions, ownedStream?: Duplex, acquire?: (signal: AbortSignal) => Promise<Duplex>): Promise<PeerStreamAttachment> {
    if (this.closed) {
      if (ownedStream) return discardOwnedStream(ownedStream).then(() => { throw failure("E_CLOSED"); });
      return Promise.reject(failure("E_CLOSED"));
    }
    const cancellation = new AbortController();
    let combined: ReturnType<typeof combineSignals>;
    try { combined = combineSignals(cancellation.signal, options.signal); }
    catch {
      if (ownedStream) return discardOwnedStream(ownedStream).then(() => { throw failure("E_INVALID_OPTIONS"); });
      return Promise.reject(failure("E_INVALID_OPTIONS"));
    }
    let resolveJoined!: () => void;
    const joined = new Promise<void>((resolve) => { resolveJoined = resolve; });
    const record = { abort: () => cancellation.abort(), joined };
    // Record before invoking any caller factory, including reentrant shutdown.
    this.admitted.add(record);
    const acquireAndAttach = async (): Promise<PeerStreamAttachment> => {
      if (combined.signal.aborted) throw failure(this.closed ? "E_CLOSED" : "E_ABORTED");
      let stream: Duplex;
      try { stream = await acquire!(combined.signal); }
      catch { throw failure(combined.signal.aborted ? (this.closed ? "E_CLOSED" : "E_ABORTED") : "E_STREAM_FAILED"); }
      if (combined.signal.aborted) {
        await discardOwnedStream(stream);
        throw failure(this.closed ? "E_CLOSED" : "E_ABORTED");
      }
      return await attachPeerStream({ ...options, stream, signal: combined.signal });
    };
    // Owned-stream transfer is synchronous at invocation, not deferred behind
    // provider acquisition or another attachment's readiness.
    const ready = ownedStream
      ? attachPeerStream({ ...options, stream: ownedStream, signal: combined.signal })
      : acquireAndAttach();
    void ready.then((attachment) => attachment.completion, () => undefined).then(() => {
      combined.dispose();
      this.admitted.delete(record);
      resolveJoined();
    });
    return ready;
  }

  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed = true;
    const records = [...this.admitted];
    // Publish the joined promise before abort callbacks can reenter close().
    this.closing = Promise.all(records.map((record) => record.joined)).then(() => undefined);
    for (const record of records) record.abort();
    return this.closing;
  }
}

/** Caller-created, provider-neutral lifetime for explicit attachment attempts.
 * No globals, discovery, default endpoints, host interpretation or auto-enable.
 * Registration alone never invokes a factory. Close permanently revokes all
 * admissions and joins started acquisitions, late-stream disposal and links. */
export class PeerStreamController {
  private closed = false;
  private readonly direct = new AttachmentAdmissions();
  private readonly providers = new Set<AttachmentAdmissions>();
  private closing?: Promise<void>;

  registerProvider<TBinding>(factory: PeerStreamProviderFactory<TBinding>): PeerStreamProviderRegistration<TBinding> {
    if (this.closed) throw failure("E_CLOSED");
    if (typeof factory !== "function") throw failure("E_INVALID_OPTIONS");
    const admissions = new AttachmentAdmissions();
    this.providers.add(admissions);
    return Object.freeze({
      attach: (binding: TBinding, options: PeerStreamAttachOptions) => this.closed
        ? Promise.reject(failure("E_CLOSED")) : admissions.acquire(factory, binding, options),
      close: () => {
        const closed = admissions.close();
        void closed.then(() => this.providers.delete(admissions));
        return closed;
      },
    });
  }

  attachOwnedStream(options: AttachPeerStreamOptions): Promise<PeerStreamAttachment> {
    return this.direct.attachOwned(options);
  }

  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed = true;
    let resolveClosed!: () => void;
    this.closing = new Promise<void>((resolve) => { resolveClosed = resolve; });
    const joined = [this.direct.close(), ...[...this.providers].map((provider) => provider.close())];
    void Promise.all(joined).then(() => { this.providers.clear(); resolveClosed(); });
    return this.closing;
  }
}
