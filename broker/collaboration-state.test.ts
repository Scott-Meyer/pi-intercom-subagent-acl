import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { CollaborationStateStore, type CollaborationContactSnapshot } from "./collaboration-state.ts";

function temporaryRuntime(): string {
  return mkdtempSync(path.join(tmpdir(), "pi-parley-collaboration-"));
}

function statePath(runtimeDir: string, scopeId: string | undefined): string {
  const scopeKey = createHash("sha256")
    .update(JSON.stringify(["scope", scopeId ?? null]))
    .digest("hex");
  return path.join(runtimeDir, "collaboration-state", `${scopeKey}.json`);
}

function backupPath(runtimeDir: string, scopeId: string | undefined): string {
  return `${statePath(runtimeDir, scopeId)}.bak`;
}

function accept(
  store: CollaborationStateStore,
  scopeId: string | undefined,
  observer: string,
  peer: string,
  snapshot: CollaborationContactSnapshot,
): void {
  store.recordAcceptedContact(scopeId, observer, peer, snapshot.peerGeneration);
}

test("first direct contact establishes a baseline and each later generation is noticed once", () => {
  const runtimeDir = temporaryRuntime();
  const store = new CollaborationStateStore(runtimeDir, { contactFlushDelayMs: 60_000 });
  try {
    store.recordSuccessfulCompaction("project-a", "peer", "project-a-peer-event-1", 1_000);

    const first = store.readContact("project-a", "observer", "peer");
    assert.deepEqual(first, {
      peerGeneration: 1,
      peerCompactedAt: 1_000,
      compactedSinceLastContact: false,
    });
    accept(store, "project-a", "observer", "peer", first);

    store.recordSuccessfulCompaction("project-a", "peer", "project-a-peer-event-2", 2_000);
    const notice = store.readContact("project-a", "observer", "peer");
    assert.equal(notice.compactedSinceLastContact, true);
    assert.equal(notice.lastContactGeneration, 1);
    assert.equal(notice.peerGeneration, 2);
    assert.equal(notice.peerCompactedAt, 2_000);
    accept(store, "project-a", "observer", "peer", notice);

    const alreadyNoticed = store.readContact("project-a", "observer", "peer");
    assert.equal(alreadyNoticed.compactedSinceLastContact, false);
    assert.equal(alreadyNoticed.lastContactGeneration, 2);

    store.recordSuccessfulCompaction("project-a", "peer", "project-a-peer-event-3", 3_000);
    store.recordSuccessfulCompaction("project-a", "peer", "project-a-peer-event-4", 4_000);
    const coalescedNotice = store.readContact("project-a", "observer", "peer");
    assert.equal(coalescedNotice.compactedSinceLastContact, true);
    assert.equal(coalescedNotice.lastContactGeneration, 2);
    assert.equal(coalescedNotice.peerGeneration, 4);
  } finally {
    store.close();
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("contact watermarks are independent by direction, peer, and scope", () => {
  const runtimeDir = temporaryRuntime();
  const store = new CollaborationStateStore(runtimeDir, { contactFlushDelayMs: 60_000 });
  try {
    const baseline = store.readContact("scope-a", "alice", "bob");
    accept(store, "scope-a", "alice", "bob", baseline);
    store.recordSuccessfulCompaction("scope-a", "bob", "scope-a-bob-event-1", 100);

    assert.equal(store.readContact("scope-a", "alice", "bob").compactedSinceLastContact, true);
    assert.equal(store.readContact("scope-a", "bob", "alice").compactedSinceLastContact, false, "reverse direction is independent");
    assert.equal(store.readContact("scope-a", "carol", "bob").compactedSinceLastContact, false, "another observer gets a first-contact baseline");
    assert.equal(store.readContact("scope-a", "alice", "carol").peerGeneration, 0, "another peer is independent");
    assert.equal(store.readContact("scope-b", "alice", "bob").peerGeneration, 0, "another scope is independent");
    assert.equal(store.readContact(undefined, "alice", "bob").peerGeneration, 0, "the unscoped namespace is independent");
  } finally {
    store.close();
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("accepted-contact watermarks survive restart after flush", () => {
  const runtimeDir = temporaryRuntime();
  try {
    const firstStore = new CollaborationStateStore(runtimeDir, { contactFlushDelayMs: 60_000 });
    const baseline = firstStore.readContact("scope", "observer", "peer");
    accept(firstStore, "scope", "observer", "peer", baseline);
    firstStore.flush();
    firstStore.recordSuccessfulCompaction("scope", "peer", "restart-peer-event-1", 1234);
    firstStore.close();

    const secondStore = new CollaborationStateStore(runtimeDir, { contactFlushDelayMs: 60_000 });
    const notice = secondStore.readContact("scope", "observer", "peer");
    assert.equal(notice.compactedSinceLastContact, true);
    assert.equal(notice.lastContactGeneration, 0);
    assert.equal(notice.peerGeneration, 1);
    accept(secondStore, "scope", "observer", "peer", notice);
    secondStore.flush();
    secondStore.close();

    const thirdStore = new CollaborationStateStore(runtimeDir);
    assert.equal(thirdStore.readContact("scope", "observer", "peer").compactedSinceLastContact, false);
    thirdStore.close();
  } finally {
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("a durable first-contact baseline survives the crash window before a later compaction", () => {
  const runtimeDir = temporaryRuntime();
  const writer = new CollaborationStateStore(runtimeDir, { contactFlushDelayMs: 60_000 });
  let reader: CollaborationStateStore | undefined;
  try {
    const baseline = writer.readContact("scope", "observer", "peer");
    writer.recordAcceptedContact("scope", "observer", "peer", baseline.peerGeneration, true);

    // Open a new store without flushing or closing the writer, modeling a broker
    // process loss immediately after the first-contact acknowledgment.
    reader = new CollaborationStateStore(runtimeDir, { contactFlushDelayMs: 60_000 });
    reader.recordSuccessfulCompaction("scope", "peer", "post-baseline-event", 1_234);
    const notice = reader.readContact("scope", "observer", "peer");
    assert.equal(notice.lastContactGeneration, 0);
    assert.equal(notice.peerGeneration, 1);
    assert.equal(notice.compactedSinceLastContact, true);
  } finally {
    reader?.close();
    writer.close();
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("staged receiver baseline survives restart but is invisible until token acknowledgement", () => {
  const runtimeDir = temporaryRuntime();
  try {
    const writer = new CollaborationStateStore(runtimeDir);
    writer.stageFirstContactBaseline("scope", "receiver", "sender", 0, "opaque-baseline-token");
    assert.equal(writer.readContact("scope", "receiver", "sender").lastContactGeneration, undefined);
    writer.close();

    const reader = new CollaborationStateStore(runtimeDir);
    assert.equal(reader.acceptStagedFirstContactBaseline("scope", "wrong-receiver", "opaque-baseline-token"), false);
    assert.equal(reader.acceptStagedFirstContactBaseline("scope", "receiver", "opaque-baseline-token"), true);
    assert.equal(reader.readContact("scope", "receiver", "sender").lastContactGeneration, 0);
    assert.equal(reader.acceptStagedFirstContactBaseline("scope", "receiver", "opaque-baseline-token"), true);
    reader.close();

    const replay = new CollaborationStateStore(runtimeDir);
    assert.equal(replay.acceptStagedFirstContactBaseline("scope", "receiver", "opaque-baseline-token"), true);
    const serialized = readFileSync(statePath(runtimeDir, "scope"), "utf8");
    assert.equal(serialized.includes("opaque-baseline-token"), false);
    assert.equal(serialized.includes("receiver"), false);
    assert.equal(serialized.includes("sender"), false);
    replay.close();
  } finally {
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("a newer staged backup rolls back to the valid primary commit point", () => {
  const runtimeDir = temporaryRuntime();
  try {
    const writer = new CollaborationStateStore(runtimeDir);
    writer.stageFirstContactBaseline("scope", "receiver", "sender", 0, "uncommitted-token");
    writer.close();

    const candidateBackup = readFileSync(backupPath(runtimeDir, "scope"), "utf8");
    const oldEnvelope = JSON.parse(readFileSync(statePath(runtimeDir, "scope"), "utf8")) as {
      revision: number;
      updatedAt: number;
      payloadSha256: string;
      payload: Record<string, unknown>;
    };
    oldEnvelope.revision = 0;
    oldEnvelope.updatedAt = 0;
    oldEnvelope.payload = { identities: {}, contacts: {} };
    oldEnvelope.payloadSha256 = createHash("sha256").update(JSON.stringify(oldEnvelope.payload)).digest("hex");
    writeFileSync(statePath(runtimeDir, "scope"), JSON.stringify(oldEnvelope), { mode: 0o600 });
    writeFileSync(backupPath(runtimeDir, "scope"), candidateBackup, { mode: 0o600 });

    const recovered = new CollaborationStateStore(runtimeDir);
    assert.equal(
      recovered.acceptStagedFirstContactBaseline("scope", "receiver", "uncommitted-token"),
      false,
      "newer backup alone is provisional and cannot create a phantom contact",
    );
    assert.equal(recovered.readContact("scope", "receiver", "sender").lastContactGeneration, undefined);
    recovered.close();
    assert.equal(
      readFileSync(backupPath(runtimeDir, "scope"), "utf8"),
      readFileSync(statePath(runtimeDir, "scope"), "utf8"),
      "startup repairs the provisional backup from the authoritative primary",
    );
  } finally {
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("successful compaction is durable without an explicit flush", () => {
  const runtimeDir = temporaryRuntime();
  try {
    const writer = new CollaborationStateStore(runtimeDir, { contactFlushDelayMs: 60_000 });
    assert.deepEqual(writer.recordSuccessfulCompaction(
      "secret-scope",
      "stable-secret-session",
      "secret-compaction-event",
      9_999,
    ), {
      generation: 1,
      compactedAt: 9_999,
    });

    const reader = new CollaborationStateStore(runtimeDir);
    const restored = reader.readContact("secret-scope", "any-observer", "stable-secret-session");
    assert.equal(restored.peerGeneration, 1);
    assert.equal(restored.peerCompactedAt, 9_999);

    const serialized = readFileSync(statePath(runtimeDir, "secret-scope"), "utf8");
    assert.equal(serialized.includes("secret-scope"), false);
    assert.equal(serialized.includes("stable-secret-session"), false);
    assert.equal(readdirSync(path.dirname(statePath(runtimeDir, "secret-scope"))).join("\n").includes("secret-scope"), false);
    if (process.platform !== "win32") {
      assert.equal(statSync(statePath(runtimeDir, "secret-scope")).mode & 0o777, 0o600);
      assert.equal(statSync(path.dirname(statePath(runtimeDir, "secret-scope"))).mode & 0o777, 0o700);
    }
    writer.close();
    reader.close();
  } finally {
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("compaction event retries stay idempotent across restart and event IDs are not persisted", () => {
  const runtimeDir = temporaryRuntime();
  const retriedEventId = "opaque-retried-event-raw-value";
  const nextEventId = "opaque-next-event-raw-value";
  try {
    const firstStore = new CollaborationStateStore(runtimeDir);
    assert.deepEqual(firstStore.recordSuccessfulCompaction("scope", "peer", retriedEventId, 111), {
      generation: 1,
      compactedAt: 111,
    });
    firstStore.close();

    const restartedStore = new CollaborationStateStore(runtimeDir);
    assert.deepEqual(restartedStore.recordSuccessfulCompaction("scope", "peer", nextEventId, 222), {
      generation: 2,
      compactedAt: 222,
    });
    assert.deepEqual(restartedStore.recordSuccessfulCompaction("scope", "peer", retriedEventId, 999), {
      generation: 1,
      compactedAt: 111,
    });
    restartedStore.close();

    const stateDirectory = path.dirname(statePath(runtimeDir, "scope"));
    for (const fileName of readdirSync(stateDirectory)) {
      const serialized = readFileSync(path.join(stateDirectory, fileName), "utf8");
      assert.equal(serialized.includes(retriedEventId), false, `${fileName} must not contain the retried raw event ID`);
      assert.equal(serialized.includes(nextEventId), false, `${fileName} must not contain the next raw event ID`);
    }
  } finally {
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("a corrupt primary recovers the latest committed generation from backup and corrupt files start empty", () => {
  const runtimeDir = temporaryRuntime();
  try {
    const store = new CollaborationStateStore(runtimeDir);
    store.recordSuccessfulCompaction("scope", "peer", "recovery-event-1", 100);
    store.recordSuccessfulCompaction("scope", "peer", "recovery-event-2", 200);
    store.close();

    writeFileSync(statePath(runtimeDir, "scope"), "{ definitely not json", { mode: 0o600 });
    const recovered = new CollaborationStateStore(runtimeDir);
    const state = recovered.readContact("scope", "observer", "peer");
    assert.equal(state.peerGeneration, 2);
    assert.equal(state.peerCompactedAt, 200);
    recovered.close();

    writeFileSync(statePath(runtimeDir, "scope"), "primary corrupt again", { mode: 0o600 });
    writeFileSync(backupPath(runtimeDir, "scope"), "also corrupt", { mode: 0o600 });
    const empty = new CollaborationStateStore(runtimeDir);
    assert.equal(empty.readContact("scope", "observer", "peer").peerGeneration, 0);
    assert.equal(empty.recordSuccessfulCompaction("scope", "peer", "after-corruption", 300).generation, 1);
    empty.close();
    assert.equal(
      readFileSync(backupPath(runtimeDir, "scope"), "utf8"),
      readFileSync(statePath(runtimeDir, "scope"), "utf8"),
      "the first post-corruption mutation re-establishes a committed primary before staging",
    );
  } finally {
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("startup repairs a missing backup before acknowledging an idempotent retry", () => {
  const runtimeDir = temporaryRuntime();
  try {
    const writer = new CollaborationStateStore(runtimeDir);
    const original = writer.recordSuccessfulCompaction("scope", "peer", "repair-event", 321);
    writer.close();
    rmSync(backupPath(runtimeDir, "scope"));

    const retrying = new CollaborationStateStore(runtimeDir);
    assert.deepEqual(
      retrying.recordSuccessfulCompaction("scope", "peer", "repair-event", 999),
      original,
      "retry returns the original durable event result",
    );
    retrying.close();
    assert.equal(existsSync(backupPath(runtimeDir, "scope")), true, "loading repaired the missing backup");

    writeFileSync(statePath(runtimeDir, "scope"), "corrupt primary", { mode: 0o600 });
    const recovered = new CollaborationStateStore(runtimeDir);
    assert.equal(recovered.readContact("scope", "observer", "peer").peerGeneration, 1);
    recovered.close();
  } finally {
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("state remains bounded while retaining generations ahead of contact edges", () => {
  const runtimeDir = temporaryRuntime();
  const options = {
    contactFlushDelayMs: 60_000,
    maxIdentities: 2,
    maxContactEdges: 20,
    maxSerializedBytes: 1_600,
  };
  try {
    const store = new CollaborationStateStore(runtimeDir, options);
    store.recordSuccessfulCompaction("scope", "old-generation", "bounded-event-old", 1);
    store.recordSuccessfulCompaction("scope", "kept-generation-a", "bounded-event-a", 2);
    store.recordSuccessfulCompaction("scope", "kept-generation-b", "bounded-event-b", 3);

    for (let index = 0; index < 30; index += 1) {
      const snapshot = store.readContact("scope", "observer", `peer-${index}`);
      accept(store, "scope", "observer", `peer-${index}`, snapshot);
    }
    store.flush();
    store.close();

    assert.ok(statSync(statePath(runtimeDir, "scope")).size <= options.maxSerializedBytes);
    const restored = new CollaborationStateStore(runtimeDir, options);
    assert.equal(restored.readContact("scope", "observer", "old-generation").peerGeneration, 0, "least-recent generation was pruned at the identity bound");
    assert.equal(restored.readContact("scope", "observer", "kept-generation-a").peerGeneration, 1);
    assert.equal(restored.readContact("scope", "observer", "kept-generation-b").peerGeneration, 1);
    assert.equal(restored.readContact("scope", "observer", "peer-0").lastContactGeneration, undefined, "old contact edges were pruned first");
    assert.equal(restored.readContact("scope", "observer", "peer-29").lastContactGeneration, 0, "recent contact survives pruning");
    restored.close();
  } finally {
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});

test("flooding one scope cannot evict another scope's generation or contact watermark", () => {
  const runtimeDir = temporaryRuntime();
  const options = {
    contactFlushDelayMs: 60_000,
    maxIdentities: 3,
    maxContactEdges: 3,
    maxSerializedBytes: 1_600,
  };
  try {
    const store = new CollaborationStateStore(runtimeDir, options);
    store.recordSuccessfulCompaction("scope-a", "peer", "isolated-scope-event-1", 100);
    const baseline = store.readContact("scope-a", "observer", "peer");
    accept(store, "scope-a", "observer", "peer", baseline);
    store.recordSuccessfulCompaction("scope-a", "peer", "isolated-scope-event-2", 200);

    for (let index = 0; index < 30; index += 1) {
      store.recordSuccessfulCompaction("scope-b", `churn-${index}`, `churn-event-${index}`, 1_000 + index);
      const snapshot = store.readContact("scope-b", "observer", `contact-${index}`);
      accept(store, "scope-b", "observer", `contact-${index}`, snapshot);
    }
    store.flush();
    store.close();

    assert.ok(statSync(statePath(runtimeDir, "scope-a")).size <= options.maxSerializedBytes);
    assert.ok(statSync(statePath(runtimeDir, "scope-b")).size <= options.maxSerializedBytes);

    const restored = new CollaborationStateStore(runtimeDir, options);
    const survivingNotice = restored.readContact("scope-a", "observer", "peer");
    assert.equal(survivingNotice.peerGeneration, 2);
    assert.equal(survivingNotice.peerCompactedAt, 200);
    assert.equal(survivingNotice.lastContactGeneration, 1);
    assert.equal(survivingNotice.compactedSinceLastContact, true);
    restored.close();
  } finally {
    rmSync(runtimeDir, { recursive: true, force: true });
  }
});
