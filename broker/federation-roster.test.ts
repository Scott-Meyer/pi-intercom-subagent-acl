import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import type { SessionInfo } from "../types.ts";
import {
  FEDERATION_PROTOCOL_NAME,
  FEDERATION_PROTOCOL_VERSION,
  type FederationScopeBinding,
} from "./federation-types.ts";
import { decodeOriginQualifiedSessionIdentity } from "./federation-protocol.ts";
import { isSessionInfo } from "./protocol.ts";
import type { FederationPeerLink } from "./peer-link.ts";
import {
  FederationRosterError,
  FederationRosterState,
  isPeerRosterDelta,
  isPeerRosterSnapshot,
  type FederationRosterEntry,
  type FederationRosterFrame,
  type ImportedRosterChange,
  type LocallyOwnedFederationSession,
  type PeerRosterDelta,
  type PeerRosterSnapshot,
} from "./federation-roster.ts";

const localOrigin = { id: "host:local", label: "Local" };
const remoteOrigin = { id: "host:remote", label: "Remote" };
const binding: FederationScopeBinding = {
  localScopeId: "local-private-scope",
  localScopeAlias: "local-work",
  remoteScopeAlias: "remote-work",
};

function session(id: string, overrides: Partial<SessionInfo> = {}): SessionInfo {
  return {
    id,
    endpointEpoch: `endpoint_${id.replace(/[^A-Za-z0-9_-]/g, "_")}_12345678`,
    name: id,
    cwd: `/work/${id}`,
    model: "test-model",
    pid: 42,
    startedAt: 100,
    lastActivity: 200,
    ...overrides,
  };
}

function peerLink(
  linkId = "link_12345678",
  scopeBindings: FederationScopeBinding[] = [binding],
  origins = { local: localOrigin, remote: remoteOrigin },
): FederationPeerLink {
  return {
    linkId,
    direction: "outbound",
    socket: new net.Socket(),
    localOrigin: origins.local,
    remoteOrigin: origins.remote,
    scopeBindings,
    features: ["peer-identity-v1", "peer-single-hop-v1", "peer-roster-v1"],
    connectedAt: 1,
  };
}

function remoteEntry(stableSessionId: string, overrides: Partial<FederationRosterEntry> = {}): FederationRosterEntry {
  return {
    scopeAlias: "remote-work",
    stableSessionId,
    session: {
      endpointEpoch: `remote_endpoint_${stableSessionId.replace(/[^A-Za-z0-9_-]/g, "_")}`,
      name: `Remote ${stableSessionId}`,
      cwd: `/remote/${stableSessionId}`,
      model: "remote-model",
      pid: 77,
      startedAt: 300,
      lastActivity: 400,
    },
    ...overrides,
  };
}

function snapshot(
  sessions: FederationRosterEntry[],
  sequence = 1,
  originEpoch = "remote_epoch_12345678",
  originId = remoteOrigin.id,
): PeerRosterSnapshot {
  return {
    type: "peer_roster_snapshot",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    originId,
    originEpoch,
    sequence,
    sessions,
  };
}

function delta(
  sequence: number,
  upserts: FederationRosterEntry[] = [],
  removals: Array<{ scopeAlias: string; stableSessionId: string }> = [],
  originEpoch = "remote_epoch_12345678",
): PeerRosterDelta {
  return {
    type: "peer_roster_delta",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    originId: remoteOrigin.id,
    originEpoch,
    sequence,
    upserts,
    removals,
  };
}

function harness(localSessions: LocallyOwnedFederationSession[] = []) {
  const sent: Array<{ linkId: string; frame: FederationRosterFrame }> = [];
  const changes: ImportedRosterChange[] = [];
  const roster = new FederationRosterState({
    originEpoch: "local_epoch_12345678",
    listLocalSessions: () => localSessions,
    send: (link, frame) => sent.push({ linkId: link.linkId, frame }),
    onImportedChange: (change) => changes.push(change),
  });
  return { roster, sent, changes, localSessions };
}

test("links without the negotiated roster feature exchange no roster frames", () => {
  const { roster, sent } = harness();
  const link = peerLink();
  link.features = ["peer-identity-v1", "peer-single-hop-v1"];
  roster.linkUp(link);
  assert.equal(sent.length, 0);
  assert.deepEqual(roster.listImported(), []);
});

test("link-up sends an authoritative bounded snapshot of eligible locally owned sessions only", () => {
  const eligible = session("main-local");
  const hidden = session("hidden-child", { isSubagent: true });
  const importedLookalike = session("imported-lookalike");
  const unsafeLocal = session("unsafe-local", { cwd: "/unsafe\npath" });
  const longNameLocal = session("long-name-local", { name: "x".repeat(2000) });
  const localSessions: LocallyOwnedFederationSession[] = [
    { ownership: "local", exportEligible: true, localScopeId: binding.localScopeId, info: eligible },
    { ownership: "local", exportEligible: false, localScopeId: binding.localScopeId, info: hidden },
    { ownership: "local", exportEligible: true, localScopeId: binding.localScopeId, info: unsafeLocal },
    { ownership: "local", exportEligible: true, localScopeId: binding.localScopeId, info: longNameLocal },
    // Runtime defense backs up the compile-time single-hop ownership contract.
    { ownership: "imported", exportEligible: true, localScopeId: binding.localScopeId, info: importedLookalike } as unknown as LocallyOwnedFederationSession,
  ];
  const { roster, sent } = harness(localSessions);
  roster.linkUp(peerLink());

  assert.equal(sent.length, 1);
  const frame = sent[0]!.frame;
  assert.equal(isPeerRosterSnapshot(frame), true);
  assert.equal(frame.type, "peer_roster_snapshot");
  if (frame.type !== "peer_roster_snapshot") return;
  assert.equal(frame.sequence, 1);
  assert.deepEqual(frame.sessions.map((entry) => entry.stableSessionId), [eligible.id, unsafeLocal.id, longNameLocal.id]);
  assert.equal(frame.sessions[0]?.scopeAlias, binding.localScopeAlias);
  assert.equal(frame.sessions[1]?.session.cwd, "");
  assert.equal(frame.sessions[2]?.session.name, undefined);
  assert.equal(JSON.stringify(frame).includes(binding.localScopeId!), false);
  assert.equal("trustedLocal" in frame.sessions[0]!.session, false);
});

test("initial snapshot binds remote aliases to local scope and creates origin-qualified untrusted projections", () => {
  const { roster, sent, changes } = harness();
  const link = peerLink();
  roster.linkUp(link);
  sent.length = 0;
  const entry = remoteEntry("remote / α");

  assert.equal(roster.handlePeerFrame(link.linkId, snapshot([entry])), true);
  const imported = roster.listImported();
  assert.equal(imported.length, 1);
  assert.equal(imported[0]?.localScopeId, binding.localScopeId);
  assert.equal(imported[0]?.info.trustedLocal, false);
  assert.equal(isSessionInfo(imported[0]?.info), true);
  assert.equal(imported[0]?.originEpoch, "remote_epoch_12345678");
  assert.deepEqual(imported[0]?.info.federation, {
    originId: remoteOrigin.id,
    originEpoch: "remote_epoch_12345678",
    conversation: false,
    originLabel: remoteOrigin.label,
    remoteScopeAlias: binding.remoteScopeAlias,
    remoteStableSessionId: entry.stableSessionId,
  });
  assert.deepEqual(decodeOriginQualifiedSessionIdentity(imported[0]!.info.id), {
    originId: remoteOrigin.id,
    remoteScopeAlias: binding.remoteScopeAlias,
    remoteStableSessionId: entry.stableSessionId,
  });
  assert.equal(changes.length, 1);
  assert.equal(changes[0]?.joined.length, 1);
  assert.equal(sent.length, 0);
});

test("exact next-sequence delta applies update and removal atomically", () => {
  const { roster, changes } = harness();
  const link = peerLink();
  roster.linkUp(link);
  const first = remoteEntry("one");
  const second = remoteEntry("two");
  roster.handlePeerFrame(link.linkId, snapshot([first, second]));
  changes.length = 0;

  const changedSecond = remoteEntry("two", { session: { ...second.session, status: "thinking", lastActivity: 500 } });
  assert.equal(roster.handlePeerFrame(link.linkId, delta(2, [changedSecond], [
    { scopeAlias: first.scopeAlias, stableSessionId: first.stableSessionId },
  ])), true);

  const imported = roster.listImported();
  assert.equal(imported.length, 1);
  assert.equal(imported[0]?.info.federation.remoteStableSessionId, "two");
  assert.equal(imported[0]?.info.status, "thinking");
  assert.equal(changes.length, 1);
  assert.equal(changes[0]?.updated.length, 1);
  assert.equal(changes[0]?.left.length, 1);
});

test("sequence gap requests one resync, suppresses deltas, and accepts a newer authoritative snapshot", () => {
  const { roster, sent, changes } = harness();
  const link = peerLink();
  roster.linkUp(link);
  sent.length = 0;
  roster.handlePeerFrame(link.linkId, snapshot([remoteEntry("one")]));
  changes.length = 0;

  roster.handlePeerFrame(link.linkId, delta(3, [remoteEntry("gap")], []));
  assert.equal(roster.listImported()[0]?.info.federation.remoteStableSessionId, "one");
  assert.equal(sent.length, 1);
  assert.deepEqual(sent[0]?.frame, {
    type: "peer_roster_resync_request",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    originId: localOrigin.id,
    expectedOriginEpoch: "remote_epoch_12345678",
    expectedSequence: 2,
  });

  roster.handlePeerFrame(link.linkId, delta(2, [remoteEntry("suppressed")], []));
  assert.equal(sent.length, 1, "deltas stay suppressed without resync flooding");
  assert.equal(changes.length, 0);

  roster.handlePeerFrame(link.linkId, snapshot([remoteEntry("replacement")], 4));
  assert.equal(roster.listImported()[0]?.info.federation.remoteStableSessionId, "replacement");
  assert.equal(changes.length, 1);
});

test("stale frames never mutate and a previously unseen origin epoch snapshot atomically replaces state", () => {
  const { roster, changes, sent } = harness();
  const link = peerLink();
  roster.linkUp(link);
  roster.handlePeerFrame(link.linkId, snapshot([remoteEntry("old")], 5));
  changes.length = 0;

  roster.handlePeerFrame(link.linkId, delta(4, [remoteEntry("stale")], []));
  roster.handlePeerFrame(link.linkId, snapshot([remoteEntry("also-stale")], 5));
  assert.equal(roster.listImported()[0]?.info.federation.remoteStableSessionId, "old");
  assert.equal(changes.length, 0);

  roster.handlePeerFrame(link.linkId, snapshot([remoteEntry("new-epoch")], 1, "remote_epoch_new_123"));
  assert.equal(roster.listImported()[0]?.info.federation.remoteStableSessionId, "new-epoch");
  assert.equal(changes.length, 1);
  changes.length = 0;
  sent.length = 0;

  roster.handlePeerFrame(link.linkId, snapshot([remoteEntry("rolled-back")], 99, "remote_epoch_12345678"));
  roster.handlePeerFrame(link.linkId, delta(100, [remoteEntry("old-delta")], [], "remote_epoch_12345678"));
  assert.equal(roster.listImported()[0]?.info.federation.remoteStableSessionId, "new-epoch");
  assert.equal(changes.length, 0);
  assert.equal(sent.length, 0, "previously seen epochs are ignored without forcing resync");
});

test("invalid, duplicate, unauthorized, and non-atomic delta inputs cannot mutate imported state", () => {
  const { roster } = harness();
  const link = peerLink();
  roster.linkUp(link);
  roster.handlePeerFrame(link.linkId, snapshot([remoteEntry("one")]));
  const before = roster.listImported().map((item) => item.info.id);

  const duplicate = snapshot([remoteEntry("dup"), remoteEntry("dup")], 2);
  assert.equal(isPeerRosterSnapshot(duplicate), false);
  assert.equal(roster.handlePeerFrame(link.linkId, duplicate), false);

  const unsafe = snapshot([remoteEntry("bad\nidentity")], 2);
  assert.equal(isPeerRosterSnapshot(unsafe), false);
  assert.equal(roster.handlePeerFrame(link.linkId, unsafe), false);

  const unauthorized = snapshot([remoteEntry("other", { scopeAlias: "not-authorized" })], 2);
  assert.throws(() => roster.handlePeerFrame(link.linkId, unauthorized), FederationRosterError);
  assert.deepEqual(roster.listImported().map((item) => item.info.id), before);

  const nonAtomic = delta(2, [remoteEntry("valid")], [
    { scopeAlias: "not-authorized", stableSessionId: "one" },
  ]);
  assert.equal(isPeerRosterDelta(nonAtomic), true);
  assert.throws(() => roster.handlePeerFrame(link.linkId, nonAtomic), FederationRosterError);
  assert.deepEqual(roster.listImported().map((item) => item.info.id), before);
});

test("aggregate imported roster bytes are bounded across peer links before commit", () => {
  const { roster } = harness();
  const first = peerLink("link_budget_first");
  const second = peerLink("link_budget_second", [binding], {
    local: localOrigin,
    remote: { id: "host:other", label: "Other" },
  });
  roster.linkUp(first);
  roster.linkUp(second);
  const entries = Array.from({ length: 64 }, (_, index) => {
    const prefix = `remote-${index}-`;
    return remoteEntry(`${prefix}${"\ud800".repeat(512 - prefix.length)}`, {
      session: {
        endpointEpoch: `remote_endpoint_${index}_12345678`,
        cwd: `/${"x".repeat(1000)}`,
        model: "remote-model",
        pid: 77,
        startedAt: 300,
        lastActivity: 400,
      },
    });
  });
  const firstFrame = snapshot(entries, 1, "first_budget_epoch", remoteOrigin.id);
  const secondFrame = snapshot(entries, 1, "second_budget_epoch", "host:other");
  assert.equal(isPeerRosterSnapshot(firstFrame), true);
  assert.equal(isPeerRosterSnapshot(secondFrame), true);
  roster.handlePeerFrame(first.linkId, firstFrame);
  assert.equal(roster.listImported().length, 64);
  assert.throws(() => roster.handlePeerFrame(second.linkId, secondFrame), /client roster byte budget/);
  assert.equal(roster.listImported().length, 64);
});

test("local reconciliation emits sequenced bounded deltas and peer resync gets a newer snapshot", () => {
  const localSessions: LocallyOwnedFederationSession[] = [{
    ownership: "local",
    exportEligible: true,
    localScopeId: binding.localScopeId,
    info: session("local-one"),
  }];
  const { roster, sent } = harness(localSessions);
  const link = peerLink();
  roster.linkUp(link);
  localSessions[0] = { ...localSessions[0]!, info: session("local-one", { status: "busy", lastActivity: 250 }) };
  localSessions.push({
    ownership: "local",
    exportEligible: true,
    localScopeId: binding.localScopeId,
    info: session("local-two"),
  });
  roster.reconcileLocalRoster();
  assert.equal(sent[1]?.frame.type, "peer_roster_delta");
  assert.equal((sent[1]?.frame as PeerRosterDelta).sequence, 2);
  assert.equal((sent[1]?.frame as PeerRosterDelta).upserts.length, 2);

  roster.handlePeerFrame(link.linkId, {
    type: "peer_roster_resync_request",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    originId: remoteOrigin.id,
    expectedOriginEpoch: "local_epoch_12345678",
    expectedSequence: 3,
  });
  assert.equal(sent[2]?.frame.type, "peer_roster_snapshot");
  assert.equal((sent[2]?.frame as PeerRosterSnapshot).sequence, 3);
  assert.equal((sent[2]?.frame as PeerRosterSnapshot).sessions.length, 2);
  roster.handlePeerFrame(link.linkId, {
    type: "peer_roster_resync_request",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    originId: remoteOrigin.id,
    expectedOriginEpoch: "local_epoch_12345678",
    expectedSequence: 3,
  });
  assert.equal(sent.length, 3, "duplicate resync requests do not flood full snapshots");
  for (let index = 0; index < 50; index += 1) {
    roster.handlePeerFrame(link.linkId, {
      type: "peer_roster_resync_request",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      originId: remoteOrigin.id,
      expectedOriginEpoch: "local_epoch_12345678",
      expectedSequence: 1000 + index,
    });
  }
  assert.equal(sent.length, 3, "varying resync keys cannot amplify snapshot responses until local state advances");
  localSessions.push({
    ownership: "local",
    exportEligible: true,
    localScopeId: binding.localScopeId,
    info: session("amplification-new-local"),
  });
  roster.reconcileLocalRoster();
  roster.handlePeerFrame(link.linkId, {
    type: "peer_roster_resync_request",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    originId: remoteOrigin.id,
    expectedOriginEpoch: "local_epoch_12345678",
    expectedSequence: 2000,
  });
  assert.equal(
    sent.filter((item) => item.frame.type === "peer_roster_snapshot").length,
    3,
    "one new authoritative response becomes available after local state advances",
  );
});

test("delta batching is byte-aware for worst-case escaped identities and projections", () => {
  const longId = (prefix: string, index: number) => {
    const start = `${prefix}-${index}-`;
    return `${start}${"\ud800".repeat(512 - start.length)}`;
  };
  const makeLocal = (prefix: string, index: number): LocallyOwnedFederationSession => ({
    ownership: "local",
    exportEligible: true,
    localScopeId: binding.localScopeId,
    info: session(longId(prefix, index), {
      endpointEpoch: `endpoint_${prefix}_${index}_12345678`,
      name: undefined,
      cwd: `/${"x".repeat(1000)}`,
    }),
  });
  const localSessions = Array.from({ length: 128 }, (_, index) => makeLocal("old", index));
  const { roster, sent } = harness(localSessions);
  roster.linkUp(peerLink());
  localSessions.splice(0, localSessions.length, ...Array.from({ length: 128 }, (_, index) => makeLocal("new", index)));
  roster.reconcileLocalRoster();
  const deltas = sent.slice(1).map((item) => item.frame);
  assert.ok(deltas.length >= 2);
  assert.equal(deltas.every(isPeerRosterDelta), true);
});

test("local roster overflow fails the peer link without throwing through an ordinary client update", () => {
  const localSessions: LocallyOwnedFederationSession[] = Array.from({ length: 128 }, (_, index) => ({
    ownership: "local" as const,
    exportEligible: true,
    localScopeId: binding.localScopeId,
    info: session(`local-${index}`),
  }));
  const errors: FederationRosterError[] = [];
  const roster = new FederationRosterState({
    originEpoch: "local_epoch_12345678",
    listLocalSessions: () => localSessions,
    send: () => undefined,
    onLinkError: (_link, error) => errors.push(error),
  });
  roster.linkUp(peerLink());
  localSessions.push({
    ownership: "local",
    exportEligible: true,
    localScopeId: binding.localScopeId,
    info: session("overflow"),
  });
  assert.doesNotThrow(() => roster.reconcileLocalRoster());
  assert.equal(errors.length, 1);
  assert.match(errors[0]!.message, /exceeds/);
});

test("disconnect atomically prunes only that peer link's imported projection", () => {
  const { roster, changes } = harness();
  const first = peerLink("link_first_123", [binding]);
  const otherBinding: FederationScopeBinding = {
    localScopeId: "another-local-scope",
    localScopeAlias: "another-local",
    remoteScopeAlias: "another-remote",
  };
  const second = peerLink(
    "link_second_123",
    [otherBinding],
    { local: localOrigin, remote: { id: "host:other", label: "Other" } },
  );
  roster.linkUp(first);
  roster.linkUp(second);
  roster.handlePeerFrame(first.linkId, snapshot([remoteEntry("first")], 1, "first_epoch_123", remoteOrigin.id));
  roster.handlePeerFrame(second.linkId, snapshot([
    remoteEntry("second", { scopeAlias: otherBinding.remoteScopeAlias }),
  ], 1, "second_epoch_123", "host:other"));
  assert.equal(roster.listImported().length, 2);
  changes.length = 0;

  roster.linkDown(first.linkId);
  const remaining = roster.listImported();
  assert.equal(remaining.length, 1);
  assert.equal(remaining[0]?.info.federation.originId, "host:other");
  assert.equal(changes.length, 1);
  assert.equal(changes[0]?.left.length, 1);
  assert.equal(changes[0]?.left[0]?.info.federation.originId, remoteOrigin.id);
});

test("bounded compact exports retain endpoint identity for exact delivery", () => {
  const info = session("compact-local", { cwd: "/" + "x".repeat(3000) });
  const { roster, sent } = harness([{ ownership: "local", exportEligible: true, localScopeId: binding.localScopeId, info }]);
  roster.linkUp(peerLink());
  const frame = sent[0]?.frame;
  assert.ok(frame && frame.type === "peer_roster_snapshot");
  assert.equal(isPeerRosterSnapshot(frame), true);
  assert.ok(frame.sessions[0]!.session.cwd.length < info.cwd.length);
  assert.equal(frame.sessions[0]!.session.endpointEpoch, info.endpointEpoch);
});

test("negotiated endpoint conversation support survives compact exports and capability updates, never legacy rosters", () => {
  const info = session("compact-conversation", { cwd: "/" + "x".repeat(3000) });
  const local = { ownership: "local" as const, exportEligible: true, conversationCapable: true, localScopeId: binding.localScopeId, info };
  const { roster, sent } = harness([local]);
  const current = peerLink();
  current.features.push("peer-send-v1", "peer-send-exact-v1", "peer-conversation-text-v1");
  roster.linkUp(current);
  const frame = sent[0]?.frame;
  assert.ok(frame?.type === "peer_roster_snapshot");
  assert.equal(frame.sessions[0]?.session.conversation, true);
  assert.ok(frame.sessions[0]!.session.cwd.length < info.cwd.length);
  const remote = remoteEntry("capable");
  remote.session.conversation = true;
  roster.handlePeerFrame(current.linkId, snapshot([remote]));
  assert.equal(roster.listImported()[0]?.info.federation.conversation, true);
  assert.equal("conversation" in roster.listImported()[0]!.info, false, "peer marker is not a session-authored public field");
  remote.session.conversation = false;
  roster.handlePeerFrame(current.linkId, delta(2, [remote], []));
  assert.equal(roster.listImported()[0]?.info.federation.conversation, false);
  const legacy = harness([local]);
  legacy.roster.linkUp(peerLink());
  const oldFrame = legacy.sent[0]?.frame;
  assert.ok(oldFrame?.type === "peer_roster_snapshot");
  assert.equal("conversation" in oldFrame.sessions[0]!.session, false);
});
