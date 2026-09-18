import { createHash, randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, statSync, openSync, closeSync, fsyncSync, writeSync } from "node:fs";
import { join } from "node:path";
import type { Message } from "../types.ts";
import { isCanonicalFederationOriginId, isCanonicalFederationScopeAlias, isFederationCorrelationId } from "./federation-protocol.ts";

/** Retained identity is independent of the per-attempt transport sendId. */
export interface ConversationEndpoint {
  originId: string;
  originEpoch: string;
  scopeAlias: string;
  stableSessionId: string;
  endpointEpoch: string;
}
export interface ConversationMessageIdentity extends ConversationEndpoint { nonce: string }
const PREFIX = "oqm1.";

function validEndpoint(value: ConversationEndpoint): boolean {
  return isCanonicalFederationOriginId(value.originId)
    && isFederationCorrelationId(value.originEpoch)
    && isCanonicalFederationScopeAlias(value.scopeAlias)
    && typeof value.stableSessionId === "string" && value.stableSessionId.length > 0
    && value.stableSessionId.length <= 512 && value.stableSessionId.trim().length > 0
    && !/[\p{Cc}\p{Cf}]/u.test(value.stableSessionId)
    && isFederationCorrelationId(value.endpointEpoch);
}
export function encodeConversationMessageId(identity: ConversationMessageIdentity): string {
  if (!validEndpoint(identity) || !isFederationCorrelationId(identity.nonce)) throw new Error("Invalid conversation message identity");
  return PREFIX + Buffer.from(JSON.stringify([identity.originId, identity.originEpoch, identity.scopeAlias,
    identity.stableSessionId, identity.endpointEpoch, identity.nonce])).toString("base64url");
}
export function decodeConversationMessageId(value: unknown): ConversationMessageIdentity | undefined {
  if (typeof value !== "string" || !value.startsWith(PREFIX) || value.length > 8192) return;
  const encoded = value.slice(PREFIX.length);
  if (!/^[A-Za-z0-9_-]+$/.test(encoded)) return;
  try {
    const tuple = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
    if (!Array.isArray(tuple) || tuple.length !== 6) return;
    const identity = { originId: tuple[0], originEpoch: tuple[1], scopeAlias: tuple[2],
      stableSessionId: tuple[3], endpointEpoch: tuple[4], nonce: tuple[5] };
    if (encodeConversationMessageId(identity) === value) return identity;
  } catch { /* Noncanonical and malformed handles are not identities. */ }
}
export function sameConversationEndpoint(left: ConversationEndpoint, right: ConversationEndpoint): boolean {
  return left.originId === right.originId && left.originEpoch === right.originEpoch
    && left.scopeAlias === right.scopeAlias && left.stableSessionId === right.stableSessionId
    && left.endpointEpoch === right.endpointEpoch;
}
export class ConversationStoreError extends Error {
  constructor(readonly code: string, message: string) { super(message); }
}
interface Edge { author: ConversationEndpoint; recipient: ConversationEndpoint; createdAt: number; scalarAlias?: string }

/** Single-hop recorded text edges. No scalar ID fallback, mailbox, or replacement rebinding. */
export class FederationConversations {
  private prepared = new Map<string, Edge>();
  private retained = new Map<string, Edge>();
  private readonly dispositions = new Map<string, "bound" | "unknown" | "not_delivered">();
  private readonly scalarAssociations = new Map<string, string>();
  private journalBytes: number | undefined;
  private journalFailure: Error | undefined;
  private admissionFailure: Error | undefined;
  private journalAbsent = false;
  constructor(private readonly barrierDir: string, private readonly barrierCapacity = 8192,
    private readonly journalByteCapacity = 4 * 1024 * 1024) {}

  prepare(author: ConversationEndpoint, recipient: ConversationEndpoint, requestedId?: string): string {
    this.prune();
    const supplied = decodeConversationMessageId(requestedId);
    if (requestedId?.startsWith(PREFIX) && !supplied) throw new Error("E_CONVERSATION_ID");
    if (supplied && !sameConversationEndpoint(supplied, author)) throw new Error("E_CONVERSATION_AUTHOR");
    const id = supplied ? requestedId! : encodeConversationMessageId({ ...author, nonce: requestedId ?? randomUUID() });
    const previous = this.prepared.get(id) ?? this.retained.get(id);
    if (previous && (!sameConversationEndpoint(previous.author, author) || !sameConversationEndpoint(previous.recipient, recipient))) {
      throw new Error("E_CONVERSATION_TARGET");
    }
    this.prepared.set(id, { author, recipient, createdAt: Date.now(),
      ...((!supplied && requestedId !== undefined) || previous?.scalarAlias !== undefined
        ? { scalarAlias: !supplied && requestedId !== undefined ? requestedId : previous!.scalarAlias } : {}) });
    return id;
  }
  /** Original caller identity, never a peer routing ID. Its scoped durable
   * barrier is admitted atomically with the authored handle before dispatch. */
  preparedScalarAlias(id: string): string | undefined {
    this.prune();
    return this.prepared.get(id)?.scalarAlias;
  }
  isPrepared(id: string, author: ConversationEndpoint, recipient: ConversationEndpoint): boolean {
    this.prune();
    const edge = this.prepared.get(id);
    return Boolean(edge && sameConversationEndpoint(edge.author, author) && sameConversationEndpoint(edge.recipient, recipient));
  }
  permitsReply(replyTo: string, author: ConversationEndpoint, recipient: ConversationEndpoint): boolean {
    this.prune();
    const edge = this.retained.get(replyTo);
    return Boolean(edge && sameConversationEndpoint(edge.recipient, author) && sameConversationEndpoint(edge.author, recipient));
  }
  retain(id: string, author: ConversationEndpoint, recipient: ConversationEndpoint): void {
    this.prune();
    this.retained.set(id, { author, recipient, createdAt: Date.now() });
  }
  /** File-fsynced, bounded attempt journal: broker-process crash/restart safety,
   * not a hardware power-loss or damaged-filesystem recovery guarantee. Unknown
   * attempts are never evicted. Only correlated preacceptance failure rearms one. */
  /** Passive prior-attempt lookup. Never creates or appends a journal. A torn
   * final append preserves the verified prefix but disables new admission;
   * unreadable or corrupt history is not treated as absence. */
  hasDispatched(id: string): boolean {
    this.loadJournal(false);
    return this.dispatchKeys(id).some(key => this.dispositions.get(key) === "unknown");
  }
  /** Persist an immutable identity association before returning a converted
   * handle. Binding is not admission and never asserts a dispatched attempt. */
  bindDispatchAlias(id: string, aliasId: string): void {
    this.loadJournal(true);
    const keys = this.dispatchKeys(id, aliasId);
    if (keys.length < 2 || this.scalarAssociations.get(keys[0]!) === keys[1]) return;
    const additional = keys.filter(key => !this.dispositions.has(key)).length;
    if (this.dispositions.size + additional > this.barrierCapacity) {
      throw new ConversationStoreError("E_CONVERSATION_CAPACITY", "Conversation identity retention is full; no frame was written");
    }
    this.appendDisposition(keys, "bound");
  }
  hasDispatchAlias(id: string, aliasId: string): boolean {
    this.loadJournal(false);
    return this.scalarAssociations.get(this.dispatchKey(id)) === this.dispatchKey(aliasId);
  }
  /** Identity equivalence survives edge eviction and disposition changes.
   * A newer admission supersedes earlier partner receipts in either direction. */
  sharesDispatchIdentity(leftId: string, rightId: string): boolean {
    this.loadJournal(false);
    const group = (id: string) => {
      const key = this.dispatchKey(id);
      return this.scalarAssociations.get(key) ?? key;
    };
    return group(leftId) === group(rightId);
  }
  beginDispatch(id: string, aliasId?: string): "new" | "existing" {
    this.loadJournal(true);
    const keys = this.dispatchKeys(id, aliasId);
    if (keys.some(key => this.dispositions.get(key) === "unknown")) return "existing";
    const additional = keys.filter(key => !this.dispositions.has(key)).length;
    if (this.dispositions.size + additional > this.barrierCapacity) {
      throw new ConversationStoreError("E_CONVERSATION_CAPACITY", "Conversation dispatch retention is full; no frame was written");
    }
    this.appendDisposition(keys, "unknown");
    return "new";
  }
  /** Broker calls ONLY for the exact pending transport correlation and a
   * destination verdict proving no receiver write happened. Never timeout,
   * link loss, duplicate rejection, cancellation, or unconfirmed replacement. */
  settleNotDelivered(id: string, aliasId?: string): void {
    this.loadJournal(true);
    const keys = this.dispatchKeys(id, aliasId);
    if (keys.some(key => this.dispositions.get(key) !== "unknown")) throw new Error("No pending dispatch evidence exists for this nondelivery verdict");
    this.appendDisposition(keys, "not_delivered");
  }
  private dispatchKeys(id: string, aliasId?: string): string[] {
    const primary = this.dispatchKey(id);
    const retained = this.scalarAssociations.get(primary);
    const alias = aliasId !== undefined ? this.dispatchKey(aliasId) : retained;
    if (retained !== undefined && alias !== retained) throw new Error("Dispatch scalar association cannot be changed");
    return [primary, ...(alias !== undefined && alias !== primary ? [alias] : [])];
  }
  private dispatchKey(id: string): string { return createHash("sha256").update(id).digest("hex"); }
  private journalPath(): string { return join(this.barrierDir, "dispatch.log"); }
  private loadJournal(create: boolean): void {
    if (this.journalFailure) throw this.journalFailure;
    if (create && this.admissionFailure) throw this.admissionFailure;
    if (this.journalBytes !== undefined || (!create && this.journalAbsent)) return;
    try {
      const path = this.journalPath();
      let present = true;
      try { statSync(path); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") present = false; else throw error; }
      if (!present) {
        if (!create) { this.journalAbsent = true; return; }
        mkdirSync(this.barrierDir, { recursive: true, mode: 0o700 });
        const fd = openSync(path, "wx+", 0o600);
        try { this.writeAll(fd, Buffer.from("parley-dispatch-v1\n"), 0); fsyncSync(fd); } finally { closeSync(fd); }
        // Windows exposes file FlushFileBuffers through fsync, but cannot open
        // directories this way. File creation + append fsync survives process
        // restart on either platform; no stronger power-loss promise is made.
        if (process.platform !== "win32") {
          const directory = openSync(this.barrierDir, "r");
          try { fsyncSync(directory); } finally { closeSync(directory); }
        }
      }
      if (statSync(path).size > this.journalByteCapacity) throw new Error("Dispatch journal exceeds its byte bound");
      const bytes = readFileSync(path);
      const text = bytes.toString("utf8");
      const header = "parley-dispatch-v1\n";
      if (!text.startsWith(header)) {
        // Header creation itself may be interrupted before any admission.
        if (!header.startsWith(text)) throw new Error("Invalid dispatch journal header");
        this.admissionFailure = new ConversationStoreError("E_CONVERSATION_STATE_FAILURE", "Dispatch journal header is incomplete; new federated dispatch is disabled");
        this.journalBytes = bytes.length;
        if (create) throw this.admissionFailure;
        return;
      }
      // A frame is never dispatched until its complete admission record is
      // fsynced. An incomplete final append therefore cannot erase earlier
      // unknowns or establish a new disposition. Never repair/truncate it.
      const records = text.slice(header.length).split("\n");
      const tail = records.pop()!;
      for (const record of records) {
        const parts = record.split("\t");
        if (parts.length !== 2 || this.dispatchKey(parts[0]!) !== parts[1]) throw new Error("Corrupt dispatch journal checksum");
        const value = JSON.parse(parts[0]!);
        if (!Array.isArray(value) || value.length !== 2
          || (value[1] !== "bound" && value[1] !== "unknown" && value[1] !== "not_delivered")) throw new Error("Invalid dispatch journal record");
        // Original single-ID records remain readable. A converted caller ID and
        // its authored handle share one checksummed, fsynced disposition record.
        const keys = typeof value[0] === "string" ? [value[0]] : value[0];
        if (!Array.isArray(keys) || keys.length < 1 || keys.length > 2 || (value[1] === "bound" && keys.length !== 2) || new Set(keys).size !== keys.length
          || keys.some(key => typeof key !== "string" || !/^[0-9a-f]{64}$/.test(key))) throw new Error("Invalid dispatch identities");
        for (const key of keys) {
          const prior = this.dispositions.get(key);
          if ((value[1] === "not_delivered" && prior !== "unknown") || (value[1] === "unknown" && prior === "unknown")) {
            throw new Error("Invalid dispatch journal transition");
          }
        }
        if (keys.length === 2) {
          const retained = this.scalarAssociations.get(keys[0]);
          if (retained !== undefined && retained !== keys[1]) throw new Error("Invalid dispatch scalar association");
          this.scalarAssociations.set(keys[0], keys[1]);
        }
        for (const key of keys) if (value[1] !== "bound" || !this.dispositions.has(key)) this.dispositions.set(key, value[1]);
        if (this.dispositions.size > this.barrierCapacity) throw new Error("Dispatch journal exceeds its identity bound");
      }
      this.journalBytes = bytes.length;
      this.journalAbsent = false;
      if (tail.length > 0) {
        this.admissionFailure = new ConversationStoreError("E_CONVERSATION_STATE_FAILURE", "Dispatch journal has an incomplete tail; new federated dispatch is disabled");
        if (create) throw this.admissionFailure;
      }
    } catch (error) {
      if (error === this.admissionFailure) throw error;
      this.journalFailure = new ConversationStoreError("E_CONVERSATION_STATE_FAILURE", `Dispatch journal is unavailable; federated dispatch is disabled: ${(error as Error).message}`);
      throw this.journalFailure;
    }
  }
  private appendDisposition(keys: string[], disposition: "bound" | "unknown" | "not_delivered"): void {
    const payload = JSON.stringify([keys.length === 1 ? keys[0] : keys, disposition]);
    const bytes = Buffer.from(payload + "\t" + this.dispatchKey(payload) + "\n");
    if (this.journalBytes! + bytes.length > this.journalByteCapacity) {
      throw new ConversationStoreError("E_CONVERSATION_CAPACITY", "Conversation dispatch journal byte limit reached; no frame was written");
    }
    try {
      // Never truncate/replace/rename: every accepted record is in the same
      // writable, file-fsynced journal, including Windows FlushFileBuffers.
      const fd = openSync(this.journalPath(), "r+");
      try {
        if (statSync(this.journalPath()).size !== this.journalBytes) throw new Error("Dispatch journal changed outside this broker");
        this.writeAll(fd, bytes, this.journalBytes!);
        fsyncSync(fd);
      } finally { closeSync(fd); }
      this.journalBytes! += bytes.length;
      if (keys.length === 2) this.scalarAssociations.set(keys[0]!, keys[1]!);
      for (const key of keys) if (disposition !== "bound" || !this.dispositions.has(key)) this.dispositions.set(key, disposition);
    } catch (error) {
      // A partial write/fsync failure cannot be retried blindly even in this
      // process. Recovery either reads a complete record or fails closed.
      this.journalFailure = new ConversationStoreError("E_CONVERSATION_STATE_FAILURE", `Dispatch journal write failed: ${(error as Error).message}`);
      throw this.journalFailure;
    }
  }
  private writeAll(fd: number, bytes: Buffer, position: number): void {
    let written = 0;
    while (written < bytes.length) {
      const count = writeSync(fd, bytes, written, bytes.length - written, position + written);
      if (count <= 0) throw new Error("Dispatch journal write made no progress");
      written += count;
    }
  }
  private prune(): void {
    const cutoff = Date.now() - 24 * 60 * 60 * 1000;
    for (const entries of [this.prepared, this.retained]) {
      for (const [id, edge] of entries) if (edge.createdAt < cutoff) entries.delete(id);
      while (entries.size > 4096) entries.delete(entries.keys().next().value!);
    }
  }
}
export function conversationCompletesAsk(message: Pick<Message, "replyTo" | "expectsReply" | "completesAsk">): boolean {
  return Boolean(message.replyTo && !message.expectsReply && (message.completesAsk ?? true));
}
