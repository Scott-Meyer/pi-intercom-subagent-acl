import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  closeSync,
  copyFileSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";

const STATE_FORMAT_VERSION = 1;
const HASH_PATTERN = /^[a-f0-9]{64}$/;
const SCOPE_FILE_PATTERN = /^([a-f0-9]{64})\.json(?:\.bak)?$/;
const DEFAULT_MAX_IDENTITIES = 2_048;
const DEFAULT_MAX_CONTACT_EDGES = 2_048;
const DEFAULT_MAX_SERIALIZED_BYTES = 256 * 1024;
const DEFAULT_CONTACT_FLUSH_DELAY_MS = 250;
const MAX_RECENT_COMPACTION_EVENTS = 64;
const MAX_LOADED_SCOPE_STATES = 512;
const MAX_HISTORICAL_SCOPE_STATES = 512;

interface CompactionEventRecord {
  eventHash: string;
  generation: number;
  compactedAt: number;
}

interface CompactionRecord {
  generation: number;
  compactedAt: number;
  touchedAt: number;
  recentEvents: CompactionEventRecord[];
}

interface ContactRecord {
  observerKey: string;
  peerKey: string;
  generation: number;
  touchedAt: number;
}

interface StagedBaselineRecord extends ContactRecord {
  tokenHash: string;
}

interface AcceptedBaselineToken {
  observerKey: string;
  touchedAt: number;
}

interface StoredState {
  identities: Map<string, CompactionRecord>;
  contacts: Map<string, ContactRecord>;
  stagedBaselines: Map<string, StagedBaselineRecord>;
  acceptedBaselineTokens: Map<string, AcceptedBaselineToken>;
}

interface StatePayload {
  identities: Record<string, CompactionRecord>;
  contacts: Record<string, ContactRecord>;
  stagedBaselines?: Record<string, StagedBaselineRecord>;
  acceptedBaselineTokens?: Record<string, AcceptedBaselineToken>;
}

interface StateEnvelope {
  formatVersion: 1;
  revision: number;
  updatedAt: number;
  payloadSha256: string;
  payload: StatePayload;
}

interface LoadedState {
  revision: number;
  state: StoredState;
}

interface ScopeState extends LoadedState {
  scopeKey: string;
  statePath: string;
  backupPath: string;
  lastTouchedAt: number;
  contactFlushTimer: ReturnType<typeof setTimeout> | undefined;
  contactsPending: boolean;
  hasCommittedPrimary: boolean;
}

/** The durable successful-compaction state of one stable session identity. */
export interface CollaborationCompactionState {
  generation: number;
  /** Informational only; generation comparison determines whether compaction occurred. */
  compactedAt?: number;
}

/** A non-mutating view used to decide whether direct contact needs a notice. */
export interface CollaborationContactSnapshot {
  peerGeneration: number;
  /** Informational timestamp for the peer's latest successful compaction. */
  peerCompactedAt?: number;
  /** Absent on first-ever contact, which establishes a baseline and produces no notice. */
  lastContactGeneration?: number;
  /** True only when a prior baseline exists and the peer generation has advanced. */
  compactedSinceLastContact: boolean;
}

export interface CollaborationStateStoreOptions {
  /** Delay for coalescing accepted-contact writes. Successful compactions never wait for it. */
  contactFlushDelayMs?: number;
  maxIdentities?: number;
  maxContactEdges?: number;
  maxSerializedBytes?: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function hash(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function payloadHash(payloadJson: string): string {
  return hash(payloadJson);
}

function cloneState(state: StoredState): StoredState {
  return {
    identities: new Map(Array.from(state.identities, ([key, value]) => [key, {
      ...value,
      recentEvents: value.recentEvents.map((event) => ({ ...event })),
    }])),
    contacts: new Map(Array.from(state.contacts, ([key, value]) => [key, { ...value }])),
    stagedBaselines: new Map(Array.from(state.stagedBaselines, ([key, value]) => [key, { ...value }])),
    acceptedBaselineTokens: new Map(Array.from(state.acceptedBaselineTokens, ([key, value]) => [key, { ...value }])),
  };
}

function payloadFromState(state: StoredState): StatePayload {
  return {
    identities: Object.fromEntries(state.identities),
    contacts: Object.fromEntries(state.contacts),
    ...(state.stagedBaselines.size > 0 ? { stagedBaselines: Object.fromEntries(state.stagedBaselines) } : {}),
    ...(state.acceptedBaselineTokens.size > 0 ? { acceptedBaselineTokens: Object.fromEntries(state.acceptedBaselineTokens) } : {}),
  };
}

function serializeEnvelope(state: StoredState, revision: number, updatedAt: number): string {
  const payload = payloadFromState(state);
  const payloadJson = JSON.stringify(payload);
  const envelope: StateEnvelope = {
    formatVersion: STATE_FORMAT_VERSION,
    revision,
    updatedAt,
    payloadSha256: payloadHash(payloadJson),
    payload,
  };
  return JSON.stringify(envelope);
}

function oldestKeys<T extends { touchedAt: number }>(records: Map<string, T>): string[] {
  return Array.from(records)
    .sort(([leftKey, left], [rightKey, right]) => left.touchedAt - right.touchedAt || leftKey.localeCompare(rightKey))
    .map(([key]) => key);
}

function removeIdentity(state: StoredState, identityKey: string): void {
  state.identities.delete(identityKey);
  for (const [edgeKey, edge] of state.contacts) {
    if (edge.observerKey === identityKey || edge.peerKey === identityKey) {
      state.contacts.delete(edgeKey);
    }
  }
  for (const [tokenHash, staged] of state.stagedBaselines) {
    if (staged.observerKey === identityKey || staged.peerKey === identityKey) {
      state.stagedBaselines.delete(tokenHash);
    }
  }
  for (const [tokenHash, accepted] of state.acceptedBaselineTokens) {
    if (accepted.observerKey === identityKey) state.acceptedBaselineTokens.delete(tokenHash);
  }
}

/**
 * Durable broker-owned state for compaction generations and directional contact
 * watermarks. Each scope has an independently bounded atomic state file. Keys are
 * derived only from `(scopeId, stable session ID)`; display names, working
 * directories, and endpoint epochs are intentionally not accepted.
 */
export class CollaborationStateStore {
  private readonly stateDir: string;
  private readonly contactFlushDelayMs: number;
  private readonly maxIdentities: number;
  private readonly maxContactEdges: number;
  private readonly maxSerializedBytes: number;
  private readonly scopes = new Map<string, ScopeState>();
  private closed = false;

  constructor(runtimeDir: string, options: CollaborationStateStoreOptions = {}) {
    this.contactFlushDelayMs = this.validateLimit(options.contactFlushDelayMs ?? DEFAULT_CONTACT_FLUSH_DELAY_MS, "contactFlushDelayMs", true);
    this.maxIdentities = this.validateLimit(options.maxIdentities ?? DEFAULT_MAX_IDENTITIES, "maxIdentities");
    this.maxContactEdges = this.validateLimit(options.maxContactEdges ?? DEFAULT_MAX_CONTACT_EDGES, "maxContactEdges");
    this.maxSerializedBytes = this.validateLimit(options.maxSerializedBytes ?? DEFAULT_MAX_SERIALIZED_BYTES, "maxSerializedBytes");
    if (this.maxSerializedBytes < 512) {
      throw new RangeError("maxSerializedBytes must be at least 512");
    }

    this.stateDir = join(runtimeDir, "collaboration-state");
    mkdirSync(this.stateDir, { recursive: true, mode: 0o700 });
    if (process.platform !== "win32") chmodSync(this.stateDir, 0o700);
    this.pruneHistoricalScopeFiles(new Set());
  }

  /**
   * Synchronously and durably commits a successful compaction before returning.
   * Retrying the same opaque event ID for the same stable identity returns the
   * original result. Throws without advancing the in-memory generation if persistence fails.
   */
  recordSuccessfulCompaction(
    scopeId: string | undefined,
    sessionId: string,
    eventId: string,
    compactedAt: number = Date.now(),
  ): CollaborationCompactionState {
    this.assertOpen();
    this.validateIdentity(scopeId, sessionId);
    if (typeof eventId !== "string" || eventId.length === 0) {
      throw new TypeError("eventId must be a non-empty opaque event ID");
    }
    if (!isNonNegativeSafeInteger(compactedAt)) throw new RangeError("compactedAt must be a non-negative safe integer");

    const scope = this.getScope(scopeId);
    const identityKey = this.identityKey(scopeId, sessionId);
    const eventHash = this.compactionEventHash(scopeId, sessionId, eventId);
    const current = scope.state.identities.get(identityKey);
    const recordedEvent = current?.recentEvents.find((event) => event.eventHash === eventHash);
    if (recordedEvent) {
      return { generation: recordedEvent.generation, compactedAt: recordedEvent.compactedAt };
    }

    const generation = (current?.generation ?? 0) + 1;
    if (!Number.isSafeInteger(generation)) throw new RangeError("Compaction generation exhausted");

    const candidate = cloneState(scope.state);
    const recentEvents = [
      ...(current?.recentEvents ?? []),
      { eventHash, generation, compactedAt },
    ].slice(-MAX_RECENT_COMPACTION_EVENTS);
    candidate.identities.set(identityKey, {
      generation,
      compactedAt,
      touchedAt: this.nextTouchedAt(scope),
      recentEvents,
    });
    this.persist(scope, candidate);
    return { generation, compactedAt };
  }

  /**
   * Reads a peer generation and the observer→peer watermark without changing
   * identity/edge recency or scheduling persistence. A missing watermark is
   * first-ever contact.
   */
  readContact(
    scopeId: string | undefined,
    observerSessionId: string,
    peerSessionId: string,
  ): CollaborationContactSnapshot {
    this.validateIdentity(scopeId, observerSessionId);
    this.validateIdentity(scopeId, peerSessionId);
    const scope = this.getScope(scopeId);
    const observerKey = this.identityKey(scopeId, observerSessionId);
    const peerKey = this.identityKey(scopeId, peerSessionId);
    const peer = scope.state.identities.get(peerKey);
    const contact = scope.state.contacts.get(this.edgeKey(observerKey, peerKey));
    const peerGeneration = peer?.generation ?? 0;
    const lastContactGeneration = contact?.generation;
    return {
      peerGeneration,
      ...(peer ? { peerCompactedAt: peer.compactedAt } : {}),
      ...(lastContactGeneration === undefined ? {} : { lastContactGeneration }),
      compactedSinceLastContact: lastContactGeneration !== undefined && peerGeneration > lastContactGeneration,
    };
  }

  /**
   * Records a directional observer→peer watermark after contact was accepted.
   * Pass the generation from the pre-delivery `readContact` snapshot. Failed
   * deliveries and broadcasts should not call this method. First-contact
   * baselines use `durable=true` so a crash cannot suppress the first later notice.
   */
  recordAcceptedContact(
    scopeId: string | undefined,
    observerSessionId: string,
    peerSessionId: string,
    observedPeerGeneration: number,
    durable = false,
  ): void {
    this.assertOpen();
    this.validateIdentity(scopeId, observerSessionId);
    this.validateIdentity(scopeId, peerSessionId);
    if (!isNonNegativeSafeInteger(observedPeerGeneration)) {
      throw new RangeError("observedPeerGeneration must be a non-negative safe integer");
    }

    const scope = this.getScope(scopeId);
    const observerKey = this.identityKey(scopeId, observerSessionId);
    const peerKey = this.identityKey(scopeId, peerSessionId);
    const edgeKey = this.edgeKey(observerKey, peerKey);
    const previous = scope.state.contacts.get(edgeKey);
    const candidate = cloneState(scope.state);
    candidate.contacts.set(edgeKey, {
      observerKey,
      peerKey,
      generation: Math.max(previous?.generation ?? 0, observedPeerGeneration),
      touchedAt: this.nextTouchedAt(scope),
    });
    const nextState = this.prune(candidate, scope.revision + 1, Date.now());
    if (durable) {
      // Do not install the candidate in memory unless both durable files commit.
      // A failed pre-delivery baseline must remain entirely provisional.
      this.persist(scope, nextState);
    } else {
      scope.state = nextState;
      scope.contactsPending = true;
      this.scheduleContactFlush(scope);
    }
  }

  /**
   * Durably stages a receiver's first-contact baseline before delivery. The
   * stage is ignored by readContact until the capable receiver acknowledges its
   * opaque token, and survives a broker restart in the meantime.
   */
  stageFirstContactBaseline(
    scopeId: string | undefined,
    observerSessionId: string,
    peerSessionId: string,
    observedPeerGeneration: number,
    token: string,
  ): void {
    this.assertOpen();
    this.validateIdentity(scopeId, observerSessionId);
    this.validateIdentity(scopeId, peerSessionId);
    if (!isNonNegativeSafeInteger(observedPeerGeneration)) {
      throw new RangeError("observedPeerGeneration must be a non-negative safe integer");
    }
    if (!token) throw new TypeError("baseline token must be non-empty");

    const scope = this.getScope(scopeId);
    const observerKey = this.identityKey(scopeId, observerSessionId);
    const peerKey = this.identityKey(scopeId, peerSessionId);
    const tokenHash = this.baselineTokenHash(scopeId, token);
    const candidate = cloneState(scope.state);
    candidate.stagedBaselines.set(tokenHash, {
      tokenHash,
      observerKey,
      peerKey,
      generation: observedPeerGeneration,
      touchedAt: this.nextTouchedAt(scope),
    });
    this.persist(scope, candidate);
    if (!scope.state.stagedBaselines.has(tokenHash)) {
      throw new Error("Collaboration state bounds could not retain staged baseline");
    }
  }

  /** Promotes a previously staged first-contact token to a durable watermark. */
  acceptStagedFirstContactBaseline(
    scopeId: string | undefined,
    observerSessionId: string,
    token: string,
  ): boolean {
    this.assertOpen();
    this.validateIdentity(scopeId, observerSessionId);
    if (!token) return false;
    const scope = this.getScope(scopeId);
    const tokenHash = this.baselineTokenHash(scopeId, token);
    const staged = scope.state.stagedBaselines.get(tokenHash);
    const observerKey = this.identityKey(scopeId, observerSessionId);
    const alreadyAccepted = scope.state.acceptedBaselineTokens.get(tokenHash);
    if (alreadyAccepted) return alreadyAccepted.observerKey === observerKey;
    if (!staged || staged.observerKey !== observerKey) return false;

    const edgeKey = this.edgeKey(staged.observerKey, staged.peerKey);
    const previous = scope.state.contacts.get(edgeKey);
    const candidate = cloneState(scope.state);
    candidate.stagedBaselines.delete(tokenHash);
    candidate.acceptedBaselineTokens.set(tokenHash, {
      observerKey: staged.observerKey,
      touchedAt: this.nextTouchedAt(scope),
    });
    candidate.contacts.set(edgeKey, {
      observerKey: staged.observerKey,
      peerKey: staged.peerKey,
      generation: Math.max(previous?.generation ?? 0, staged.generation),
      touchedAt: this.nextTouchedAt(scope),
    });
    this.persist(scope, candidate);
    return true;
  }

  /** Synchronously persists any debounced accepted-contact updates in all loaded scopes. */
  flush(): void {
    this.assertOpen();
    for (const scope of this.scopes.values()) {
      if (scope.contactsPending) this.flushScope(scope);
    }
  }

  /** Flushes pending contacts in all loaded scopes and prevents further mutations. Idempotent. */
  close(): void {
    if (this.closed) return;
    for (const scope of this.scopes.values()) this.clearContactFlushTimer(scope);
    for (const scope of this.scopes.values()) {
      if (scope.contactsPending) this.persist(scope, scope.state);
    }
    this.closed = true;
    this.pruneHistoricalScopeFiles(new Set());
  }

  private validateLimit(value: number, name: string, allowZero = false): number {
    if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1)) {
      throw new RangeError(`${name} must be ${allowZero ? "a non-negative" : "a positive"} safe integer`);
    }
    return value;
  }

  private validateIdentity(scopeId: string | undefined, sessionId: string): void {
    if (scopeId !== undefined && (typeof scopeId !== "string" || scopeId.length === 0)) {
      throw new TypeError("scopeId must be undefined or a non-empty string");
    }
    if (typeof sessionId !== "string" || sessionId.length === 0) {
      throw new TypeError("sessionId must be a non-empty stable session ID");
    }
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("CollaborationStateStore is closed");
  }

  private scopeKey(scopeId: string | undefined): string {
    return hash(JSON.stringify(["scope", scopeId ?? null]));
  }

  private identityKey(scopeId: string | undefined, sessionId: string): string {
    return hash(JSON.stringify(["scope", scopeId ?? null, "session", sessionId]));
  }

  private compactionEventHash(scopeId: string | undefined, sessionId: string, eventId: string): string {
    return hash(JSON.stringify(["scope", scopeId ?? null, "session", sessionId, "compaction-event", eventId]));
  }

  private edgeKey(observerKey: string, peerKey: string): string {
    return hash(JSON.stringify(["observer", observerKey, "peer", peerKey]));
  }

  private baselineTokenHash(scopeId: string | undefined, token: string): string {
    return hash(JSON.stringify(["scope", scopeId ?? null, "baseline-token", token]));
  }

  private getScope(scopeId: string | undefined): ScopeState {
    const scopeKey = this.scopeKey(scopeId);
    const existing = this.scopes.get(scopeKey);
    if (existing) {
      // Refresh insertion order so eviction below is least-recently used.
      this.scopes.delete(scopeKey);
      this.scopes.set(scopeKey, existing);
      return existing;
    }

    while (this.scopes.size >= MAX_LOADED_SCOPE_STATES) {
      const oldestKey = this.scopes.keys().next().value;
      if (oldestKey === undefined) break;
      const oldest = this.scopes.get(oldestKey)!;
      this.clearContactFlushTimer(oldest);
      if (oldest.contactsPending) this.flushScope(oldest);
      this.scopes.delete(oldestKey);
    }

    const statePath = join(this.stateDir, `${scopeKey}.json`);
    const backupPath = `${statePath}.bak`;
    const primary = this.readStateFile(statePath);
    const backup = this.readStateFile(backupPath);
    // Primary rename is the commit point. A newer backup with an older valid
    // primary is an uncommitted stage left by a failed write and must roll back.
    // If primary is missing/corrupt, the valid backup is the recovery source.
    const loaded = primary ?? backup;
    if (primary && backup?.revision !== primary.revision) {
      this.mirrorStateFile(statePath, backupPath);
    } else if (!primary && backup) {
      this.mirrorStateFile(backupPath, statePath);
    }
    const revision = loaded?.revision ?? 0;
    const state = this.prune(loaded?.state ?? { identities: new Map(), contacts: new Map(), stagedBaselines: new Map(), acceptedBaselineTokens: new Map() }, revision, Date.now());
    let lastTouchedAt = 0;
    for (const record of state.identities.values()) lastTouchedAt = Math.max(lastTouchedAt, record.touchedAt);
    for (const record of state.contacts.values()) lastTouchedAt = Math.max(lastTouchedAt, record.touchedAt);
    for (const record of state.stagedBaselines.values()) lastTouchedAt = Math.max(lastTouchedAt, record.touchedAt);
    for (const record of state.acceptedBaselineTokens.values()) lastTouchedAt = Math.max(lastTouchedAt, record.touchedAt);

    const scope: ScopeState = {
      scopeKey,
      statePath,
      backupPath,
      revision,
      state,
      lastTouchedAt,
      contactFlushTimer: undefined,
      contactsPending: false,
      hasCommittedPrimary: loaded !== null,
    };
    this.scopes.set(scopeKey, scope);
    this.touchScopeFiles(scope);
    return scope;
  }

  private nextTouchedAt(scope: ScopeState): number {
    const now = Date.now();
    scope.lastTouchedAt = Math.max(now, scope.lastTouchedAt + 1);
    if (!Number.isSafeInteger(scope.lastTouchedAt)) scope.lastTouchedAt = now;
    return scope.lastTouchedAt;
  }

  private scheduleContactFlush(scope: ScopeState): void {
    if (scope.contactFlushTimer || this.contactFlushDelayMs === 0) {
      if (this.contactFlushDelayMs === 0) this.flushScope(scope);
      return;
    }
    scope.contactFlushTimer = setTimeout(() => {
      scope.contactFlushTimer = undefined;
      try {
        this.flushScope(scope);
      } catch {
        // Keep the update pending. A later contact, compaction, flush, or close retries it.
      }
    }, this.contactFlushDelayMs);
    scope.contactFlushTimer.unref?.();
  }

  private clearContactFlushTimer(scope: ScopeState): void {
    if (!scope.contactFlushTimer) return;
    clearTimeout(scope.contactFlushTimer);
    scope.contactFlushTimer = undefined;
  }

  private flushScope(scope: ScopeState): void {
    if (!scope.contactsPending) return;
    this.clearContactFlushTimer(scope);
    this.persist(scope, scope.state);
  }

  private prune(source: StoredState, revision: number, updatedAt: number): StoredState {
    const state = cloneState(source);

    const oldestAcceptedTokens = oldestKeys(state.acceptedBaselineTokens);
    while (state.acceptedBaselineTokens.size > this.maxContactEdges) {
      state.acceptedBaselineTokens.delete(oldestAcceptedTokens.shift()!);
    }

    const oldestStaged = oldestKeys(state.stagedBaselines);
    while (state.stagedBaselines.size > this.maxContactEdges) {
      state.stagedBaselines.delete(oldestStaged.shift()!);
    }

    const oldestContacts = oldestKeys(state.contacts);
    while (state.contacts.size > this.maxContactEdges) {
      state.contacts.delete(oldestContacts.shift()!);
    }

    const oldestIdentities = oldestKeys(state.identities);
    while (state.identities.size > this.maxIdentities) {
      removeIdentity(state, oldestIdentities.shift()!);
    }

    let serialized = serializeEnvelope(state, revision, updatedAt);
    // Old ACK replay tokens are sacrificed before provisional stages, accepted
    // watermarks, and generation counters when byte bounds are exhausted.
    for (const key of oldestKeys(state.acceptedBaselineTokens)) {
      if (Buffer.byteLength(serialized, "utf8") <= this.maxSerializedBytes) break;
      state.acceptedBaselineTokens.delete(key);
      serialized = serializeEnvelope(state, revision, updatedAt);
    }
    // Unaccepted delivery stages are sacrificed before accepted watermarks and
    // generation counters when the configured byte bound is exhausted.
    for (const key of oldestKeys(state.stagedBaselines)) {
      if (Buffer.byteLength(serialized, "utf8") <= this.maxSerializedBytes) break;
      state.stagedBaselines.delete(key);
      serialized = serializeEnvelope(state, revision, updatedAt);
    }
    // Contact watermarks are intentionally sacrificed before generation counters.
    for (const key of oldestKeys(state.contacts)) {
      if (Buffer.byteLength(serialized, "utf8") <= this.maxSerializedBytes) break;
      state.contacts.delete(key);
      serialized = serializeEnvelope(state, revision, updatedAt);
    }
    // Keep the newest event hash for each surviving identity when possible, but
    // sacrifice deeper retry history before sacrificing durable generations.
    for (const key of oldestKeys(state.identities)) {
      const identity = state.identities.get(key)!;
      while (
        identity.recentEvents.length > 1
        && Buffer.byteLength(serialized, "utf8") > this.maxSerializedBytes
      ) {
        identity.recentEvents.shift();
        serialized = serializeEnvelope(state, revision, updatedAt);
      }
    }
    for (const key of oldestKeys(state.identities)) {
      if (Buffer.byteLength(serialized, "utf8") <= this.maxSerializedBytes) break;
      removeIdentity(state, key);
      serialized = serializeEnvelope(state, revision, updatedAt);
    }
    if (Buffer.byteLength(serialized, "utf8") > this.maxSerializedBytes) {
      throw new Error("Collaboration state cannot fit within maxSerializedBytes");
    }
    return state;
  }

  private persist(scope: ScopeState, source: StoredState): void {
    const createsScopeFile = !scope.hasCommittedPrimary;
    if (!scope.hasCommittedPrimary) {
      // Establish a committed revision-0 primary before staging the first real
      // mutation in backup. Otherwise a failed first primary write would leave
      // no authoritative file to distinguish the stage from a commit.
      const initialUpdatedAt = Date.now();
      const initial = serializeEnvelope(scope.state, scope.revision, initialUpdatedAt);
      this.writeStateFileAtomically(scope.backupPath, initial);
      this.writeStateFileAtomically(scope.statePath, initial);
      scope.hasCommittedPrimary = true;
    }
    const updatedAt = Date.now();
    const nextRevision = scope.revision + 1;
    if (!Number.isSafeInteger(nextRevision)) throw new RangeError("Collaboration state revision exhausted");
    const candidate = this.prune(source, nextRevision, updatedAt);
    const serialized = serializeEnvelope(candidate, nextRevision, updatedAt);

    // Stage the candidate in backup first, then atomically rename primary as the
    // commit point. If primary fails, its older valid revision remains
    // authoritative on restart and repairs the uncommitted backup stage. Once
    // primary succeeds, both independently durable copies contain the revision.
    this.writeStateFileAtomically(scope.backupPath, serialized);
    this.writeStateFileAtomically(scope.statePath, serialized);

    scope.state = candidate;
    scope.revision = nextRevision;
    scope.contactsPending = false;
    this.clearContactFlushTimer(scope);
    if (createsScopeFile) this.pruneHistoricalScopeFiles(new Set(this.scopes.keys()));
  }

  private writeStateFileAtomically(destinationPath: string, serialized: string): void {
    const tempPath = `${destinationPath}.tmp.${process.pid}.${randomUUID()}`;
    try {
      writeFileSync(tempPath, serialized, { mode: 0o600 });
      if (process.platform !== "win32") chmodSync(tempPath, 0o600);
      this.fsyncFile(tempPath);
      renameSync(tempPath, destinationPath);
      this.fsyncDirectory(dirname(destinationPath));
    } catch (error) {
      throw new Error("Failed to persist collaboration state", { cause: error });
    } finally {
      rmSync(tempPath, { force: true });
    }
  }

  private mirrorStateFile(sourcePath: string, destinationPath: string): void {
    const tempPath = `${destinationPath}.tmp.${process.pid}.${randomUUID()}`;
    try {
      copyFileSync(sourcePath, tempPath);
      if (process.platform !== "win32") chmodSync(tempPath, 0o600);
      this.fsyncFile(tempPath);
      renameSync(tempPath, destinationPath);
      this.fsyncDirectory(dirname(destinationPath));
    } catch (error) {
      throw new Error("Failed to mirror collaboration state", { cause: error });
    } finally {
      rmSync(tempPath, { force: true });
    }
  }

  private touchScopeFiles(scope: ScopeState): void {
    const now = new Date();
    for (const filePath of [scope.statePath, scope.backupPath]) {
      try {
        if (existsSync(filePath)) utimesSync(filePath, now, now);
      } catch {
        // Recency is advisory; inability to update it must not make state unreadable.
      }
    }
  }

  private pruneHistoricalScopeFiles(protectedScopeKeys: Set<string>): void {
    try {
      const lastUsedByScope = new Map<string, number>();
      for (const entry of readdirSync(this.stateDir, { withFileTypes: true })) {
        if (!entry.isFile()) continue;
        const match = SCOPE_FILE_PATTERN.exec(entry.name);
        if (!match) continue;
        const scopeKey = match[1]!;
        let modifiedAt: number;
        try {
          modifiedAt = statSync(join(this.stateDir, entry.name)).mtimeMs;
        } catch {
          continue;
        }
        lastUsedByScope.set(scopeKey, Math.max(lastUsedByScope.get(scopeKey) ?? 0, modifiedAt));
      }

      const removable = Array.from(lastUsedByScope)
        .filter(([scopeKey]) => !protectedScopeKeys.has(scopeKey))
        .sort(([leftKey, leftTime], [rightKey, rightTime]) => leftTime - rightTime || leftKey.localeCompare(rightKey));
      let excess = lastUsedByScope.size - MAX_HISTORICAL_SCOPE_STATES;
      for (const [scopeKey] of removable) {
        if (excess <= 0) break;
        rmSync(join(this.stateDir, `${scopeKey}.json`), { force: true });
        rmSync(join(this.stateDir, `${scopeKey}.json.bak`), { force: true });
        excess -= 1;
      }
    } catch {
      // Retention cleanup is best-effort and must never interfere with active state.
    }
  }

  private fsyncFile(filePath: string): void {
    // Windows FlushFileBuffers requires write access, even after a completed write.
    const file = openSync(filePath, "r+");
    try {
      fsyncSync(file);
    } finally {
      closeSync(file);
    }
  }

  private fsyncDirectory(directoryPath: string): void {
    try {
      const directory = openSync(directoryPath, "r");
      try {
        fsyncSync(directory);
      } finally {
        closeSync(directory);
      }
    } catch {
      // Directory fsync is unavailable on some platforms.
    }
  }

  private readStateFile(filePath: string): LoadedState | null {
    if (!existsSync(filePath)) return null;
    try {
      // Reject unexpectedly large/corrupt input before allocating and parsing it.
      if (statSync(filePath).size > this.maxSerializedBytes) return null;
      const value: unknown = JSON.parse(readFileSync(filePath, "utf8"));
      if (!isRecord(value) || value.formatVersion !== STATE_FORMAT_VERSION) return null;
      if (!isNonNegativeSafeInteger(value.revision) || !isNonNegativeSafeInteger(value.updatedAt)) return null;
      if (typeof value.payloadSha256 !== "string" || !isRecord(value.payload)) return null;
      const payloadJson = JSON.stringify(value.payload);
      if (payloadHash(payloadJson) !== value.payloadSha256) return null;

      const rawIdentities = value.payload.identities;
      const rawContacts = value.payload.contacts;
      const rawStagedBaselines = value.payload.stagedBaselines ?? {};
      const rawAcceptedBaselineTokens = value.payload.acceptedBaselineTokens ?? {};
      if (
        !isRecord(rawIdentities)
        || !isRecord(rawContacts)
        || !isRecord(rawStagedBaselines)
        || !isRecord(rawAcceptedBaselineTokens)
      ) return null;
      const identities = new Map<string, CompactionRecord>();
      const contacts = new Map<string, ContactRecord>();
      const stagedBaselines = new Map<string, StagedBaselineRecord>();
      const acceptedBaselineTokens = new Map<string, AcceptedBaselineToken>();

      for (const [key, raw] of Object.entries(rawIdentities)) {
        if (!HASH_PATTERN.test(key) || !isRecord(raw)) continue;
        if (
          !isNonNegativeSafeInteger(raw.generation) || raw.generation < 1
          || !isNonNegativeSafeInteger(raw.compactedAt)
          || !isNonNegativeSafeInteger(raw.touchedAt)
          || (raw.recentEvents !== undefined && !Array.isArray(raw.recentEvents))
        ) continue;

        const recentEvents: CompactionEventRecord[] = [];
        for (const rawEvent of raw.recentEvents ?? []) {
          if (
            !isRecord(rawEvent)
            || typeof rawEvent.eventHash !== "string" || !HASH_PATTERN.test(rawEvent.eventHash)
            || !isNonNegativeSafeInteger(rawEvent.generation) || rawEvent.generation < 1
            || rawEvent.generation > raw.generation
            || !isNonNegativeSafeInteger(rawEvent.compactedAt)
          ) continue;
          const duplicateIndex = recentEvents.findIndex((event) => event.eventHash === rawEvent.eventHash);
          if (duplicateIndex >= 0) recentEvents.splice(duplicateIndex, 1);
          recentEvents.push({
            eventHash: rawEvent.eventHash,
            generation: rawEvent.generation,
            compactedAt: rawEvent.compactedAt,
          });
        }
        identities.set(key, {
          generation: raw.generation,
          compactedAt: raw.compactedAt,
          touchedAt: raw.touchedAt,
          recentEvents: recentEvents.slice(-MAX_RECENT_COMPACTION_EVENTS),
        });
      }

      for (const [key, raw] of Object.entries(rawContacts)) {
        if (!HASH_PATTERN.test(key) || !isRecord(raw)) continue;
        if (
          typeof raw.observerKey !== "string" || !HASH_PATTERN.test(raw.observerKey)
          || typeof raw.peerKey !== "string" || !HASH_PATTERN.test(raw.peerKey)
          || !isNonNegativeSafeInteger(raw.generation)
          || !isNonNegativeSafeInteger(raw.touchedAt)
          || this.edgeKey(raw.observerKey, raw.peerKey) !== key
        ) continue;
        contacts.set(key, {
          observerKey: raw.observerKey,
          peerKey: raw.peerKey,
          generation: raw.generation,
          touchedAt: raw.touchedAt,
        });
      }
      for (const [key, raw] of Object.entries(rawStagedBaselines)) {
        if (!HASH_PATTERN.test(key) || !isRecord(raw)) continue;
        if (
          typeof raw.tokenHash !== "string" || raw.tokenHash !== key
          || typeof raw.observerKey !== "string" || !HASH_PATTERN.test(raw.observerKey)
          || typeof raw.peerKey !== "string" || !HASH_PATTERN.test(raw.peerKey)
          || !isNonNegativeSafeInteger(raw.generation)
          || !isNonNegativeSafeInteger(raw.touchedAt)
        ) continue;
        stagedBaselines.set(key, {
          tokenHash: key,
          observerKey: raw.observerKey,
          peerKey: raw.peerKey,
          generation: raw.generation,
          touchedAt: raw.touchedAt,
        });
      }
      for (const [key, raw] of Object.entries(rawAcceptedBaselineTokens)) {
        if (!HASH_PATTERN.test(key) || !isRecord(raw)) continue;
        if (
          typeof raw.observerKey !== "string" || !HASH_PATTERN.test(raw.observerKey)
          || !isNonNegativeSafeInteger(raw.touchedAt)
        ) continue;
        acceptedBaselineTokens.set(key, {
          observerKey: raw.observerKey,
          touchedAt: raw.touchedAt,
        });
      }
      return {
        revision: value.revision,
        state: { identities, contacts, stagedBaselines, acceptedBaselineTokens },
      };
    } catch {
      return null;
    }
  }
}
