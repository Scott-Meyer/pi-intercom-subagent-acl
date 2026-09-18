import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import path from "node:path";
import { mkdtempSync, rmSync, writeFileSync, appendFileSync, existsSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { once } from "node:events";
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { ParleyClient, ConversationPrepareError } from "./client.ts";
import { createMessageReader, writeMessage } from "./framing.ts";
import { getBrokerSocketPath, getParleyDirPath, getBrokerPortFilePath } from "./paths.ts";
import { getTsxCliPath } from "./spawn.ts";
import { FEDERATION_CONVERSATION_FEATURE, FEDERATION_EXACT_SEND_FEATURE } from "./federation-types.ts";
import type { Message, SessionInfo } from "../types.ts";

async function until<T>(read: () => T | undefined | Promise<T | undefined>): Promise<T> {
  for (let attempt = 0; attempt < 1000; attempt++) {
    const result = await read();
    if (result !== undefined) return result;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error("Timed out waiting for public broker behavior");
}
async function startBroker(dir: string): Promise<ChildProcess> {
  const child = spawn(process.execPath, [getTsxCliPath(), path.join(process.cwd(), "broker/broker.ts")], {
    env: { ...process.env, PI_CODING_AGENT_DIR: dir }, stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout!.on("data", chunk => { output += String(chunk); });
  child.stderr!.on("data", chunk => { output += String(chunk); });
  await until(() => {
    if (child.exitCode !== null) throw new Error(output);
    return output.includes("Parley broker started") ? true : undefined;
  });
  return child;
}
async function stopBroker(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGTERM");
  await once(child, "exit");
}
/** Reads exactly one frame and preserves following bytes for the opaque stream. */
async function firstFrame(socket: net.Socket): Promise<{ value: Record<string, unknown>; leftover: Buffer }> {
  return new Promise((resolve, reject) => {
    let buffered = Buffer.alloc(0);
    const read = (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      if (buffered.length < 4 || buffered.length < 4 + buffered.readUInt32BE()) return;
      const length = buffered.readUInt32BE();
      socket.pause();
      socket.off("data", read);
      socket.off("error", reject);
      resolve({ value: JSON.parse(buffered.subarray(4, 4 + length).toString()), leftover: buffered.subarray(4 + length) });
    };
    socket.on("data", read);
    socket.once("error", reject);
  });
}
interface Incoming { from: SessionInfo; message: Message }
function inbox(client: ParleyClient): Incoming[] {
  const messages: Incoming[] = [];
  client.on("message", (from, message) => messages.push({ from, message }));
  return messages;
}

/** Consumer-boundary fixture: clients speak to real child-process brokers.
 * The transport proxy only frames for deterministic loss/delay/old-peer cases. */
async function fixture(legacy: boolean | "send-only-exact" = false, clientFaults = false) {
  const root = mkdtempSync(path.join(process.platform === "win32" ? tmpdir() : "/tmp", "pq-"));
  const dirs = [path.join(root, "a"), path.join(root, "b")];
  const socketPaths = dirs.map(dir => getBrokerSocketPath(process.platform, dir));
  const brokers = [await startBroker(dirs[0]!), await startBroker(dirs[1]!)];
  const clients: ParleyClient[] = [];
  const sockets: net.Socket[] = [];
  const failures: Error[] = [];
  const frames: Record<string, unknown>[] = [];
  let dropResults = false;
  let holdNext = false;
  let mutateNext: ((frame: Record<string, unknown>) => void) | undefined;
  let held: { socket: net.Socket; frame: Record<string, unknown> } | undefined;
  let preparationFault: "drop" | "disconnect" | undefined;
  let rosterFault: "drop" | "disconnect" | undefined;
  const clientDir = path.join(root, "client");
  const clientProxy = clientFaults ? net.createServer(source => {
    const destination = net.connect(socketPaths[0]!);
    sockets.push(source, destination);
    source.on("error", () => undefined); destination.on("error", () => undefined);
    source.pipe(destination);
    destination.on("data", createMessageReader(value => {
      const frame = value as Record<string, unknown>;
      const fault = frame.type === "sessions" ? rosterFault
        : frame.type === "conversation_prepared" || frame.type === "conversation_prepare_failed" ? preparationFault : undefined;
      if (fault) {
        if (fault === "disconnect") source.destroy();
        return;
      }
      writeMessage(source, frame);
    }, error => failures.push(error)));
    source.on("close", () => destination.destroy()); destination.on("close", () => source.destroy());
  }) : undefined;
  if (clientProxy) {
    clientProxy.listen(0, "127.0.0.1"); await once(clientProxy, "listening");
    mkdirSync(getParleyDirPath(clientDir), { recursive: true });
    writeFileSync(getBrokerPortFilePath(getParleyDirPath(clientDir)), JSON.stringify({ transport: "tcp", host: "127.0.0.1",
      port: (clientProxy.address() as net.AddressInfo).port, stateId: "fixture-authenticated-local-proxy" }));
  }
  const proxy = net.createServer(source => {
    sockets.push(source);
    source.on("error", () => undefined);
    void (async () => {
      const attach = await firstFrame(source);
      assert.equal(attach.value.type, "bridge_attach");
      const destination = net.connect(socketPaths[1]!);
      sockets.push(destination);
      destination.on("error", () => undefined);
      await once(destination, "connect");
      writeMessage(destination, { type: "broker_accept_peer", requestId: randomUUID(), linkId: attach.value.linkId,
        localOrigin: { id: "host:conversation-b" }, remoteOrigin: { id: "host:conversation-a" },
        scopeBindings: [
          { localScopeId: null, localScopeAlias: "b", remoteScopeAlias: "a" },
          { localScopeId: "private-b", localScopeAlias: "pb", remoteScopeAlias: "pa" },
        ],
      });
      const accepted = await firstFrame(destination);
      assert.equal(accepted.value.ok, true);
      const forward = createMessageReader(value => {
        const frame = value as Record<string, unknown>;
        if (legacy && frame.type === "peer_hello") frame.features = (frame.features as string[])
          .filter(feature => feature !== FEDERATION_CONVERSATION_FEATURE && (legacy !== true || feature !== FEDERATION_EXACT_SEND_FEATURE));
        if (frame.type === "peer_send") {
          if (mutateNext) { const mutate = mutateNext; mutateNext = undefined; mutate(frame); }
          frames.push(frame);
          if (holdNext) { holdNext = false; held = { socket: destination, frame }; return; }
        }
        writeMessage(destination, frame);
      }, error => failures.push(error));
      source.on("data", forward);
      destination.on("data", createMessageReader(value => {
        const frame = value as Record<string, unknown>;
        if (dropResults && frame.type === "peer_send_result") return;
        if (frame.type === "peer_send") frames.push(frame);
        writeMessage(source, frame);
      }, error => failures.push(error)));
      source.on("close", () => destination.destroy());
      destination.on("close", () => source.destroy());
      if (attach.leftover.length) forward(attach.leftover);
      destination.resume();
      source.resume();
    })().catch(error => { failures.push(error); source.destroy(); });
  });
  proxy.listen(0, "127.0.0.1");
  await once(proxy, "listening");
  const address = proxy.address() as net.AddressInfo;
  async function link() {
    const control = net.connect(socketPaths[0]!);
    control.on("error", () => undefined);
    await once(control, "connect");
    const response = firstFrame(control);
    writeMessage(control, { type: "broker_dial_peer", requestId: randomUUID(),
      endpoint: { transport: "tcp", host: "127.0.0.1", port: address.port }, capability: "A".repeat(32),
      localOrigin: { id: "host:conversation-a" }, remoteOrigin: { id: "host:conversation-b" },
      scopeBindings: [
        { localScopeId: null, localScopeAlias: "a", remoteScopeAlias: "b" },
        { localScopeId: "private-a", localScopeAlias: "pa", remoteScopeAlias: "pb" },
      ],
    });
    assert.equal((await response).value.ok, true);
    control.destroy();
  }
  async function connect(side: number, id: string, scope?: string) {
    const original = process.env.PI_CODING_AGENT_DIR;
    const originalScope = process.env.PI_PARLEY_SCOPE_ID;
    const originalTransport = process.env.PI_PARLEY_TRANSPORT;
    process.env.PI_CODING_AGENT_DIR = side === 0 && clientProxy ? clientDir : dirs[side];
    if (side === 0 && clientProxy) process.env.PI_PARLEY_TRANSPORT = "tcp";
    if (scope === undefined) delete process.env.PI_PARLEY_SCOPE_ID; else process.env.PI_PARLEY_SCOPE_ID = scope;
    const client = new ParleyClient();
    clients.push(client);
    try {
      await client.connect({ name: id, cwd: process.cwd(), model: "public-conversation-test", pid: process.pid,
        startedAt: Date.now(), lastActivity: Date.now() }, id);
    } finally {
      if (original === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = original;
      if (originalScope === undefined) delete process.env.PI_PARLEY_SCOPE_ID; else process.env.PI_PARLEY_SCOPE_ID = originalScope;
      if (originalTransport === undefined) delete process.env.PI_PARLEY_TRANSPORT; else process.env.PI_PARLEY_TRANSPORT = originalTransport;
    }
    return client;
  }
  async function remote(client: ParleyClient, id: string) {
    return until(async () => (await client.listSessions()).find(row => row.federation?.remoteStableSessionId === id));
  }
  await link();
  return { connect, remote, frames,
    mutateNext(mutate: (frame: Record<string, unknown>) => void) { mutateNext = mutate; },
    journalExists(side: number) { return existsSync(path.join(getParleyDirPath(dirs[side]!), "conversation-dispatches", "dispatch.log")); },
    corruptJournal(side: number) { appendFileSync(path.join(getParleyDirPath(dirs[side]!), "conversation-dispatches", "dispatch.log"), "torn record"); },
    blockPersistence(side: number) {
      const dir = path.join(getParleyDirPath(dirs[side]!), "conversation-dispatches");
      rmSync(dir, { recursive: true, force: true });
      writeFileSync(dir, "not a directory");
    },
    async oldEndpoint(side: number, id: string) {
      const socket = net.connect(socketPaths[side]!);
      sockets.push(socket);
      socket.on("error", () => undefined);
      await once(socket, "connect");
      const registration = firstFrame(socket);
      writeMessage(socket, { type: "register", sessionId: id, session: {
        name: id, cwd: process.cwd(), model: "old-endpoint", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now(),
      } });
      assert.equal((await registration).value.type, "registered");
      const messages: Record<string, unknown>[] = [];
      socket.on("data", createMessageReader(frame => messages.push(frame as Record<string, unknown>), error => failures.push(error)));
      socket.resume();
      return { messages, socket };
    },
    dropResults(value: boolean) { dropResults = value; },
    peerMode(value: boolean | "send-only-exact") { legacy = value; },
    preparationFault(value?: "drop" | "disconnect") { preparationFault = value; },
    rosterFault(value?: "drop" | "disconnect") { rosterFault = value; },
    hold() { holdNext = true; },
    async release() { const frame = await until(() => held); held = undefined; writeMessage(frame.socket, frame.frame); },
    async held() { await until(() => held); },
    async unlink() { for (const socket of sockets) socket.destroy(); await new Promise(resolve => setTimeout(resolve, 80)); },
    link,
    async restart(side: number) { await stopBroker(brokers[side]!); brokers[side] = await startBroker(dirs[side]!); },
    async close() {
      for (const client of clients) await client.disconnect();
      for (const socket of sockets) socket.destroy();
      await new Promise<void>(resolve => proxy.close(() => resolve()));
      if (clientProxy) await new Promise<void>(resolve => clientProxy.close(() => resolve()));
      for (const broker of brokers) await stopBroker(broker);
      rmSync(root, { recursive: true, force: true });
      assert.deepEqual(failures, []);
    },
  };
}

test("public two-broker conversations preserve retained identity, reverse only recorded edges, and distinguish progress", { concurrency: false }, async () => {
  const f = await fixture();
  try {
    const a = await f.connect(0, "a");
    const b = await f.connect(1, "b");
    const c = await f.connect(1, "c");
    const otherAuthor = await f.connect(0, "other-author");
    const ai = inbox(a), bi = inbox(b), oi = inbox(otherAuthor);
    const rb = await f.remote(a, "b"), ra = await f.remote(b, "a");
    assert.equal(rb.federation?.conversation, true);
    const ask = await a.prepareConversation(rb, { messageId: "shared_nonce_1234" });
    assert.equal((await a.prepareConversation(rb, { messageId: ask.messageId })).messageId, ask.messageId);
    assert.equal(ask.author.endpointEpoch, a.getSelfSession()?.endpointEpoch);
    assert.equal(ask.recipient.endpointEpoch, rb.endpointEpoch);
    // Fast response is sent before the original sender awaits its delivery ACK.
    let fastReply: Promise<unknown> | undefined;
    b.once("message", (_from: SessionInfo, message: Message) => {
      fastReply = (async () => {
        const reply = await b.prepareConversation(ra);
        return b.sendToSession(reply.recipient, { messageId: reply.messageId, text: "fast answer", replyTo: message.id });
      })();
    });
    const sent = await a.sendToSession(ask.recipient, { messageId: ask.messageId, text: "A asks B", expectsReply: true });
    assert.equal(sent.id, ask.messageId);
    assert.equal(sent.delivered, true);
    const received = await until(() => ai.find(row => row.message.replyTo === ask.messageId));
    await until(() => fastReply);
    await fastReply;
    assert.equal(bi[0]?.message.id, ask.messageId);
    assert.equal(received.message.completesAsk, true);
    assert.notEqual(received.message.id, ask.messageId);
    assert.notEqual(f.frames[0]?.sendId, ask.messageId, "transport correlation is not retained identity");

    const privateA = await f.connect(0, "a", "private-a"), privateB = await f.connect(1, "b", "private-b");
    const privateBi = inbox(privateB);
    const scoped = await privateA.prepareConversation(await f.remote(privateA, "b"), { messageId: "shared_nonce_1234" });
    assert.notEqual(scoped.messageId, ask.messageId, "same author stable ID + nonce in another scope is independent");
    assert.equal((await privateA.sendToSession(scoped.recipient, { messageId: scoped.messageId, text: "private scoped ask", expectsReply: true })).delivered, true);
    await until(() => privateBi.find(row => row.message.id === scoped.messageId));
    const crossScope = await privateB.prepareConversation(await f.remote(privateB, "a"));
    assert.equal((await privateB.sendToSession(crossScope.recipient, { messageId: crossScope.messageId, text: "wrong scope thread", replyTo: ask.messageId })).code, "E_REPLY_TARGET");

    const reverse = await b.prepareConversation(ra, { messageId: "shared_nonce_1234" });
    const collision = await otherAuthor.prepareConversation(await f.remote(otherAuthor, "b"), { messageId: "shared_nonce_1234" });
    assert.equal(new Set([ask.messageId, reverse.messageId, collision.messageId]).size, 3, "same scalar nonce across origins/authors remains disjoint");
    assert.equal((await b.sendToSession(reverse.recipient, { messageId: reverse.messageId, text: "B asks A", expectsReply: true })).delivered, true);
    assert.equal((await otherAuthor.sendToSession(collision.recipient, { messageId: collision.messageId, text: "another author asks B", expectsReply: true })).delivered, true);
    const progress = await a.prepareConversation(rb);
    assert.equal((await a.sendToSession(progress.recipient, { messageId: progress.messageId, text: "progress", replyTo: reverse.messageId, completesAsk: false })).delivered, true);
    const question = await a.prepareConversation(rb);
    assert.equal((await a.sendToSession(question.recipient, { messageId: question.messageId, text: "threaded question", replyTo: reverse.messageId, expectsReply: true, completesAsk: true })).delivered, true);
    const answer = await a.prepareConversation(rb);
    assert.equal((await a.sendToSession(answer.recipient, { messageId: answer.messageId, text: "answer", replyTo: reverse.messageId })).delivered, true);
    await until(() => bi.find(row => row.message.id === answer.messageId));
    assert.equal(bi.find(row => row.message.id === progress.messageId)?.message.completesAsk, false);
    assert.equal(bi.find(row => row.message.id === question.messageId)?.message.completesAsk, false);
    assert.equal(bi.find(row => row.message.id === answer.messageId)?.message.completesAsk, true);

    const wrong = await c.prepareConversation(await f.remote(c, "a"));
    const denied = await c.sendToSession(wrong.recipient, { messageId: wrong.messageId, text: "not your counterpart", replyTo: ask.messageId });
    assert.equal(denied.code, "E_REPLY_TARGET");
    const forged = await otherAuthor.sendToSession(rb, { messageId: ask.messageId, text: "stolen author", expectsReply: true });
    assert.equal(forged.code, "E_CONVERSATION_AUTHOR");
    const mistakenThread = await b.prepareConversation(ra);
    assert.equal((await b.sendToSession(mistakenThread.recipient, { messageId: mistakenThread.messageId, text: "wrong original author", replyTo: collision.messageId })).code, "E_REPLY_TARGET");
    const correctThread = await b.prepareConversation(await f.remote(b, "other-author"));
    assert.equal((await b.sendToSession(correctThread.recipient, { messageId: correctThread.messageId, text: "correct author", replyTo: collision.messageId })).delivered, true);
    await until(() => oi.find(row => row.message.replyTo === collision.messageId));
    assert.equal(ai.some(row => row.message.replyTo === collision.messageId), false);

    const ordinary = await a.send(rb.id, { text: "ordinary upgraded notification" });
    assert.equal(ordinary.delivered, true);
    assert.match(ordinary.id, /^oqm1\./);
    await until(() => bi.find(row => row.message.id === ordinary.id));
    const ordinaryReply = await b.send(ra.id, { text: "reply to ordinary notification", replyTo: ordinary.id });
    assert.equal(ordinaryReply.delivered, true);
    assert.match(ordinaryReply.id, /^oqm1\./);
    await until(() => ai.find(row => row.message.id === ordinaryReply.id && row.message.replyTo === ordinary.id));
  } finally { await f.close(); }
});

test("conversation delivery rejects replacement of either pinned endpoint, including after peer dispatch", { concurrency: false }, async () => {
  const f = await fixture();
  try {
    let a = await f.connect(0, "a"), b = await f.connect(1, "b");
    let rb = await f.remote(a, "b");
    const first = await a.prepareConversation(rb);
    f.hold();
    const sending = a.sendToSession(first.recipient, { messageId: first.messageId, text: "old recipient", expectsReply: true });
    await f.held();
    await b.disconnect();
    b = await f.connect(1, "b");
    const bi = inbox(b);
    await f.release();
    const replacedTarget = await sending;
    assert.equal(replacedTarget.code, "E_TARGET_REBOUND");
    assert.equal(replacedTarget.outcomeKnown, true);
    assert.equal(bi.length, 0);
    rb = await until(async () => { const row = await f.remote(a, "b"); return row.endpointEpoch !== first.recipient.endpointEpoch ? row : undefined; });
    const second = await a.prepareConversation(rb);
    f.hold();
    const sourceSending = a.sendToSession(second.recipient, { messageId: second.messageId, text: "old author", expectsReply: true });
    await f.held();
    const oldA = a;
    a = await f.connect(0, "a");
    await until(async () => { const row = await f.remote(b, "a"); return row.endpointEpoch !== second.author.endpointEpoch ? row : undefined; });
    await f.release();
    assert.equal((await sourceSending).delivered, false);
    assert.equal(bi.length, 0, "old source incarnation never arrives at recipient");
    await oldA.disconnect();
    const third = await a.prepareConversation(await f.remote(a, "b"));
    assert.equal((await a.sendToSession(third.recipient, { messageId: third.messageId, text: "live ask", expectsReply: true })).delivered, true);
    await until(() => bi.find(row => row.message.id === third.messageId));
    const ra = await f.remote(b, "a");
    await b.disconnect();
    b = await f.connect(1, "b");
    const replacementReply = await b.prepareConversation(ra);
    assert.equal((await b.sendToSession(replacementReply.recipient, { messageId: replacementReply.messageId, text: "replacement cannot answer", replyTo: third.messageId })).code, "E_REPLY_TARGET");
    const fourth = await a.prepareConversation(await f.remote(a, "b"));
    assert.equal((await a.sendToSession(fourth.recipient, { messageId: fourth.messageId, text: "another live ask", expectsReply: true })).delivered, true);
    await a.disconnect();
    a = await f.connect(0, "a");
    const changedAuthor = await until(async () => { const row = await f.remote(b, "a"); return row.endpointEpoch !== fourth.author.endpointEpoch ? row : undefined; });
    const replyToChangedAuthor = await b.prepareConversation(changedAuthor);
    assert.equal((await b.sendToSession(replyToChangedAuthor.recipient, { messageId: replyToChangedAuthor.messageId, text: "cannot answer replacement requester", replyTo: fourth.messageId })).code, "E_REPLY_TARGET");
  } finally { await f.close(); }
});

test("ACK loss is permanently nonreplayable on peer reconnect, client timeout, explicit retry, and broker restart", { concurrency: false }, async () => {
  const f = await fixture();
  try {
    let a = await f.connect(0, "a");
    const b = await f.connect(1, "b"), bi = inbox(b);
    const ask = await a.prepareConversation(await f.remote(a, "b"));
    f.dropResults(true);
    const send = a.sendToSession(ask.recipient, { messageId: ask.messageId, text: "dispatch once", expectsReply: true, timeoutMs: 100 });
    await until(() => bi.find(row => row.message.id === ask.messageId));
    const timeout = await send;
    assert.equal(timeout.delivery, "unknown");
    assert.equal(timeout.outcomeKnown, false);
    assert.equal(timeout.retryable, false);
    await f.unlink();
    f.dropResults(false);
    await f.link();
    await f.remote(a, "b");
    const attempts = f.frames.length;
    const retry = await a.sendToSession(ask.recipient, { messageId: ask.messageId, text: "dispatch once", expectsReply: true });
    assert.equal(retry.outcomeKnown, false);
    assert.equal(retry.retryable, false);
    assert.equal(f.frames.length, attempts);
    await f.unlink();
    await f.restart(0);
    a = await f.connect(0, "a");
    const beforeRelink = await a.sendToSession(ask.recipient, { messageId: ask.messageId, text: "dispatch once", expectsReply: true });
    assert.equal(beforeRelink.delivery, "unknown", "durable ownership cannot depend on an active peer link");
    assert.equal(beforeRelink.outcomeKnown, false);
    assert.equal(beforeRelink.retryable, false);
    assert.equal(f.frames.length, attempts);
    const wrongScope = await f.connect(0, "a", "private-a");
    const wrongScopeResult = await wrongScope.sendToSession(ask.recipient, { messageId: ask.messageId, text: "dispatch once", expectsReply: true });
    assert.equal(wrongScopeResult.code, "E_TARGET_NOT_FOUND", "same stable session in another local scope does not own the dispatch outcome");
    assert.equal(wrongScopeResult.outcomeKnown, true);
    await f.link();
    const rb = await f.remote(a, "b");
    const restarted = await a.sendToSession(rb, { messageId: ask.messageId, text: "dispatch once", expectsReply: true });
    assert.equal(restarted.delivery, "unknown");
    assert.equal(restarted.retryable, false);
    assert.equal(f.frames.length, attempts);
    assert.equal(bi.filter(row => row.message.id === ask.messageId).length, 1);
    await assert.rejects(a.prepareConversation(rb, { messageId: ask.messageId }), error =>
      error instanceof ConversationPrepareError && error.code === "E_DELIVERY_UNKNOWN" && error.outcomeKnown === false,
      "preparation preserves prior uncertain dispatch evidence rather than relabelling it as known author-validation nondelivery");
  } finally { await f.close(); }
});

test("unchanged legacy retries cannot cross into local routing after recovery, including a torn journal suffix", { concurrency: false }, async () => {
  for (const torn of [false, true]) {
    const f = await fixture(true);
    try {
      let a = await f.connect(0, "a");
      const b = await f.connect(1, "b"), bi = inbox(b);
      const local = await f.connect(0, "local");
      await f.remote(a, "b");
      assert.equal((await a.send(local.sessionId!, { text: "fresh local work before federation" })).delivered, true);
      assert.equal(f.journalExists(0), false, "ordinary local delivery never creates a federation journal");
      const options = { messageId: "name_recovery_1234", text: "execute this work once", timeoutMs: 100 };
      f.dropResults(true);
      const initial = await a.send("b", options);
      assert.equal(initial.delivery, "unknown");
      assert.equal(initial.retryable, false);
      await until(() => bi.find(row => row.message.id === options.messageId));
      await a.disconnect();
      await f.unlink();
      if (torn) f.corruptJournal(0);
      await f.restart(0);
      a = await f.connect(0, "a");
      const replacement = await f.connect(0, "b"), ri = inbox(replacement);
      const retry = await a.send("b", options);
      assert.equal(retry.delivery, "unknown", "an unchanged name may now resolve locally, but the instruction still has a prior unknown attempt");
      assert.equal(retry.outcomeKnown, false);
      assert.equal(retry.retryable, false);
      assert.equal(retry.code, "E_DELIVERY_UNKNOWN");
      assert.equal(ri.length, 0, "the newly local namesake never receives the retained instruction");
      assert.equal(bi.length, 1);
      assert.equal((await a.send("b", { text: "fresh unrelated local work" })).delivered, true);
      await until(() => ri.find(row => row.message.content.text === "fresh unrelated local work"));
      assert.equal(ri.length, 1, "passive valid-prefix recovery protects old work without disabling fresh local delivery");
    } finally { await f.close(); }
  }
});

test("caller-supplied scalar IDs remain nonreplayable after canonical conversion and author reincarnation", { concurrency: false }, async () => {
  for (const restart of [false, true]) {
    const f = await fixture();
    try {
      let a = await f.connect(0, "a");
      const b = await f.connect(1, "b"), bi = inbox(b);
      await f.remote(a, "b");
      const options = { messageId: "converted_recovery_1234", text: "execute this operation once", timeoutMs: 100 };
      f.dropResults(true);
      const original = await a.send("b", options);
      assert.equal(original.delivery, "unknown");
      assert.match(original.id, /^oqm1\./);
      await until(() => bi.find(row => row.message.id === original.id));
      await f.unlink();
      await a.disconnect();
      if (restart) await f.restart(0);
      a = await f.connect(0, "a");
      f.dropResults(false);
      await f.link();
      await f.remote(a, "b");
      const before = f.frames.length;
      const retry = await a.send("b", options);
      assert.equal(f.frames.length, before, "author incarnation change cannot rename the same caller-owned operation and dispatch it twice");
      assert.equal(retry.id, options.messageId);
      assert.equal(retry.delivery, "unknown");
      assert.equal(retry.outcomeKnown, false);
      assert.equal(retry.retryable, false);
      assert.equal(bi.length, 1);
      await f.unlink();
      const local = await f.connect(0, "b"), li = inbox(local);
      const localRetry = await a.send("b", options);
      assert.equal(localRetry.delivery, "unknown", "the original scalar alias is protected at common admission even when local routing skips preflight");
      assert.equal(localRetry.outcomeKnown, false);
      assert.equal(localRetry.retryable, false);
      assert.equal(li.length, 0);
      assert.equal(f.frames.length, before);
    } finally { await f.close(); }
  }
});

test("prepared canonical handles retain uncertainty when their scalar alias dispatches on a legacy route", { concurrency: false }, async () => {
  const f = await fixture();
  try {
    const a = await f.connect(0, "a"), b = await f.connect(1, "b"), bi = inbox(b);
    const rb = await f.remote(a, "b");
    const scalar = "reverse_alias_recovery_1234";
    const prepared = await a.prepareConversation(rb, { messageId: scalar });
    await a.prepareConversation(prepared.recipient, { messageId: prepared.messageId });
    await f.unlink(); f.peerMode(true); await f.link();
    const legacy = await until(async () => {
      const row = await f.remote(a, "b"); return row.federation?.conversation === false ? row : undefined;
    });
    const options = { messageId: scalar, text: "execute this once", timeoutMs: 100 };
    const unsupported = await a.sendToSession(legacy, { ...options, messageId: prepared.messageId });
    assert.equal(unsupported.code, "E_SEND_UNSUPPORTED");
    assert.equal(unsupported.outcomeKnown, true, "the canonical attempt was genuinely unsent before its alias dispatches");
    f.dropResults(true);
    assert.equal((await a.send("b", options)).delivery, "unknown");
    await until(() => bi.find(row => row.message.id === scalar));
    const before = f.frames.length;
    const canonicalOptions = { ...options, messageId: prepared.messageId };
    for (const retry of [() => a.send("b", canonicalOptions), () => a.sendToSession(legacy, canonicalOptions)]) {
      const result = await retry();
      assert.equal(result.delivery, "unknown", "passive retained lookup includes the dispatched scalar partner before capability validation");
      assert.equal(result.outcomeKnown, false);
      assert.equal(result.retryable, false);
    }
    await assert.rejects(a.prepareConversation(legacy, { messageId: prepared.messageId }), error =>
      error instanceof ConversationPrepareError && error.outcomeKnown === false);
    assert.equal(f.frames.length, before);
    assert.equal(bi.length, 1);
  } finally { await f.close(); }
});

test("canonical alias admission supersedes an older local rebound verdict", { concurrency: false }, async () => {
  const f = await fixture();
  try {
    const a = await f.connect(0, "a"), b = await f.connect(1, "b"), bi = inbox(b);
    const rb = await f.remote(a, "b");
    const local = await f.connect(0, "local");
    const snapshot = (await a.listSessions()).find(row => row.id === local.sessionId)!;
    await local.disconnect();
    const replacement = await f.connect(0, "local"), li = inbox(replacement);
    const options = { messageId: "stale_alias_recovery_1234", text: "execute this once", timeoutMs: 100 };
    const rebound = await a.sendToSession(snapshot, options);
    assert.equal(rebound.code, "E_TARGET_REBOUND");
    assert.equal(rebound.outcomeKnown, true);
    f.dropResults(true);
    const dispatched = await a.send("b", options);
    assert.equal(dispatched.delivery, "unknown");
    await until(() => bi.find(row => row.message.id === dispatched.id));
    const before = f.frames.length;
    for (const retry of [() => a.sendToSession(snapshot, options), () => a.send("local", options)]) {
      const result = await retry();
      assert.equal(result.delivery, "unknown", "a stale local nondelivery record cannot override the newly admitted canonical attempt");
      assert.equal(result.outcomeKnown, false);
      assert.equal(result.retryable, false);
    }
    await assert.rejects(a.prepareConversation(rb, { messageId: options.messageId }), error =>
      error instanceof ConversationPrepareError && error.outcomeKnown === false);
    assert.equal(li.length, 0);
    assert.equal(bi.length, 1);
    assert.equal(f.frames.length, before);
    const acceptedOptions = { ...options, messageId: "accepted_scalar_recovery_1234" };
    assert.equal((await a.send("local", acceptedOptions)).delivery, "socket_delivered");
    await until(() => li.find(row => row.message.id === acceptedOptions.messageId));
    const converted = await a.send("b", acceptedOptions);
    assert.equal(converted.code, "E_MESSAGE_ID_REUSE", "conversion cannot rename a scalar operation already accepted locally");
    assert.equal(converted.outcomeKnown, true);
    assert.equal(f.frames.length, before);
    assert.equal(bi.length, 1);
    const preparedOptions = { ...options, messageId: "prepared_accepted_scalar_1234" };
    const prepared = await a.prepareConversation(rb, { messageId: preparedOptions.messageId });
    assert.equal((await a.send("local", preparedOptions)).delivery, "socket_delivered");
    await until(() => li.find(row => row.message.id === preparedOptions.messageId));
    const preparedRetry = await a.sendToSession(prepared.recipient, { ...preparedOptions, messageId: prepared.messageId });
    assert.equal(preparedRetry.code, "E_MESSAGE_ID_REUSE", "earlier preparation cannot bypass later local acceptance of the scalar partner");
    assert.equal(preparedRetry.outcomeKnown, true);
    assert.equal(f.frames.length, before);
    assert.equal(bi.length, 1);
  } finally { await f.close(); }
});

test("legacy unknown identities cannot be reminted when the same target upgrades to conversations", { concurrency: false }, async () => {
  for (const restart of [false, true]) {
    const f = await fixture(true);
    try {
      let a = await f.connect(0, "a");
      const b = await f.connect(1, "b"), bi = inbox(b);
      assert.equal((await f.remote(a, "b")).federation?.conversation, false);
      const options = { messageId: "upgrade_recovery_1234", text: "execute this work once", timeoutMs: 100 };
      f.dropResults(true);
      assert.equal((await a.send("b", options)).delivery, "unknown");
      await until(() => bi.find(row => row.message.id === options.messageId));
      await f.unlink();
      if (restart) {
        await a.disconnect();
        await f.restart(0);
        a = await f.connect(0, "a");
      }
      await b.disconnect();
      const upgraded = await f.connect(1, "b"), ui = inbox(upgraded);
      f.peerMode(false);
      f.dropResults(false);
      await f.link();
      const target = await until(async () => {
        const row = await f.remote(a, "b");
        return row.federation?.conversation ? row : undefined;
      });
      const before = f.frames.length;
      for (const retry of [() => a.send("b", options), () => a.sendToSession(target, options)]) {
        const result = await retry();
        assert.equal(f.frames.length, before, "an unchanged uncertain instruction never dispatches a second peer frame after identity conversion");
        assert.equal(result.id, options.messageId, "preflight does not replace an instruction whose original identity is uncertain");
        assert.equal(result.delivery, "unknown");
        assert.equal(result.outcomeKnown, false);
        assert.equal(result.retryable, false);
        assert.equal(result.code, "E_DELIVERY_UNKNOWN");
      }
      await assert.rejects(a.prepareConversation(target, { messageId: options.messageId }), error =>
        error instanceof ConversationPrepareError && error.code === "E_DELIVERY_UNKNOWN" && error.outcomeKnown === false);
      assert.equal(f.frames.length, before);
      assert.equal(ui.length, 0, "capability upgrade and endpoint rebind cannot cause a second execution");
      assert.equal(bi.length, 1);
      assert.equal((await a.send("b", { text: "fresh conversation work" })).delivered, true);
      await until(() => ui.find(row => row.message.content.text === "fresh conversation work"));
    } finally { await f.close(); }
  }
});

test("unavailable preflight verdicts keep retained uncertainty while fresh preparation remains known unsent", { concurrency: false }, async () => {
  for (const fault of ["drop", "disconnect"] as const) {
    const f = await fixture(true, true);
    try {
      let a = await f.connect(0, "a");
      const b = await f.connect(1, "b"), bi = inbox(b);
      await f.remote(a, "b");
      const options = { messageId: "preflight_loss_1234", text: "execute only once", timeoutMs: 100 };
      f.dropResults(true);
      assert.equal((await a.send("b", options)).delivery, "unknown");
      await until(() => bi.find(row => row.message.id === options.messageId));
      await f.unlink();
      a = await f.connect(0, "a");
      f.peerMode(false); f.dropResults(false);
      await f.link();
      const target = await until(async () => {
        const row = await f.remote(a, "b"); return row.federation?.conversation ? row : undefined;
      });
      const before = f.frames.length;
      f.preparationFault(fault);
      for (const snapshot of [false, true]) {
        const result = snapshot ? await a.sendToSession(target, options) : await a.send("b", options);
        assert.equal(result.id, options.messageId);
        assert.equal(result.delivery, "unknown");
        assert.equal(result.outcomeKnown, false, "losing the recheck does not prove the original instruction was undelivered");
        assert.equal(result.retryable, false);
        if (fault === "disconnect") a = await f.connect(0, "a");
      }
      await assert.rejects(a.prepareConversation(target, { messageId: options.messageId, timeoutMs: 30 }), error =>
        error instanceof ConversationPrepareError && error.outcomeKnown === false);
      if (fault === "disconnect") a = await f.connect(0, "a");
      await assert.rejects(a.prepareConversation(target, { timeoutMs: 30 }), error =>
        error instanceof ConversationPrepareError && error.outcomeKnown === true,
        "fresh preparation dispatches no payload and has no earlier operation to recheck");
      assert.equal(f.frames.length, before);
      assert.equal(bi.length, 1);
      f.preparationFault();
      if (fault === "disconnect") a = await f.connect(0, "a");
      f.rosterFault(fault);
      const discovery = await a.send("b", options);
      assert.equal(discovery.delivery, "unknown", "unavailable discovery cannot establish the retained operation's outcome");
      assert.equal(discovery.outcomeKnown, false);
      assert.equal(discovery.retryable, false);
      f.rosterFault();
      await a.disconnect();
      const disconnected = await a.send("b", options);
      assert.equal(disconnected.delivery, "unknown");
      assert.equal(disconnected.retryable, false);
      assert.equal((await a.send("b", { text: "fresh disconnected operation" })).outcomeKnown, true);
      a = await f.connect(0, "a");
      const aborted = new AbortController(); aborted.abort();
      const cancelled = await a.send("b", { ...options, signal: aborted.signal });
      assert.equal(cancelled.delivery, "unknown");
      assert.equal(cancelled.retryable, false);
      const freshCancelled = await a.sendToSession(target, { text: "fresh aborted snapshot", signal: aborted.signal });
      assert.equal(freshCancelled.outcomeKnown, true, "an allocated handle does not turn a fresh operation into historical uncertainty");
      assert.equal(freshCancelled.delivery, "failed");
      assert.equal(f.frames.length, before);
    } finally { await f.close(); }
  }
});

test("capability downgrade cannot replace a retained unknown or acknowledged conversation verdict", { concurrency: false }, async () => {
  for (const lostAck of [true, false]) {
    const f = await fixture();
    try {
      const a = await f.connect(0, "a"), b = await f.connect(1, "b"), bi = inbox(b);
      await f.remote(a, "b");
      f.dropResults(lostAck);
      const initial = await a.send("b", { text: "retain this exact operation", timeoutMs: 100 });
      assert.equal(initial.delivery, lostAck ? "unknown" : "socket_delivered");
      await until(() => bi.find(row => row.message.id === initial.id));
      await f.unlink();
      f.peerMode("send-only-exact");
      f.dropResults(false);
      await f.link();
      await until(async () => (await f.remote(a, "b")).federation?.conversation === false ? true : undefined);
      const before = f.frames.length;
      const retry = await a.send("b", { messageId: initial.id, text: "retain this exact operation" });
      assert.equal(retry.delivery, initial.delivery, "today's capabilities do not change yesterday's operation receipt");
      assert.equal(retry.outcomeKnown, !lostAck);
      assert.equal(retry.retryable, false);
      assert.equal(f.frames.length, before);
      assert.equal(bi.length, 1);
    } finally { await f.close(); }
  }
});

test("send-only legacy peers remain ordinary-text usable but never claim or accept conversations", { concurrency: false }, async () => {
  const f = await fixture(true);
  try {
    const a = await f.connect(0, "a"), b = await f.connect(1, "b");
    const rb = await f.remote(a, "b"), bi = inbox(b);
    assert.equal(rb.federation?.conversation, false);
    assert.equal((await f.remote(b, "a")).federation?.conversation, false);
    await assert.rejects(a.prepareConversation(rb), error => error instanceof ConversationPrepareError && error.code === "E_SEND_UNSUPPORTED");
    const ordinary = await a.send(rb.id, { text: "ordinary legacy text", messageId: "legacy_nonce_1234" });
    assert.equal(ordinary.delivered, true);
    await until(() => bi.find(row => row.message.id === ordinary.id));
    assert.equal((await a.send(rb.id, { text: "unsupported ask", expectsReply: true })).code, "E_SEND_UNSUPPORTED");
    assert.equal((await a.send(rb.id, { text: "unsupported reply", replyTo: ordinary.id })).code, "E_SEND_UNSUPPORTED");
    assert.equal(bi.length, 1);
  } finally { await f.close(); }
});

test("current brokers keep an old local endpoint text-only and project legacy provenance without disconnecting it", { concurrency: false }, async () => {
  const f = await fixture();
  try {
    const a = await f.connect(0, "a");
    const old = await f.oldEndpoint(1, "old");
    const row = await f.remote(a, "old");
    assert.equal(row.federation?.conversation, false, "link support does not imply endpoint reply support");
    await assert.rejects(a.prepareConversation(row), error => error instanceof ConversationPrepareError && error.code === "E_SEND_UNSUPPORTED");
    const sent = await a.send(row.id, { text: "ordinary text to old local endpoint" });
    assert.equal(sent.delivered, true);
    const delivered = await until(() => old.messages.find(frame => frame.type === "message"));
    const legacySender = delivered.from as SessionInfo;
    assert.deepEqual(Object.keys(legacySender.federation!).sort(), ["originId", "remoteScopeAlias", "remoteStableSessionId"].sort());
    writeMessage(old.socket, { type: "list", requestId: "old_roster_request" });
    const listed = await until(() => old.messages.find(frame => frame.type === "sessions"));
    const imported = (listed.sessions as SessionInfo[]).find(session => session.federation);
    assert.equal(imported?.federation?.conversation, undefined);
    assert.equal(imported?.federation?.originEpoch, undefined);
    assert.equal((await a.send(row.id, { text: "old endpoint cannot accept ask", expectsReply: true })).code, "E_SEND_UNSUPPORTED");
    assert.equal(old.messages.filter(frame => frame.type === "message").length, 1);
  } finally { await f.close(); }
});

test("destination authenticates peer author/origin pins and persistence failure is known nondelivery", { concurrency: false }, async () => {
  const f = await fixture();
  try {
    const a = await f.connect(0, "a"), b = await f.connect(1, "b"), bi = inbox(b);
    const rb = await f.remote(a, "b");
    f.mutateNext(frame => { frame.senderEndpointEpoch = randomUUID(); });
    const forgedAuthor = await a.send(rb.id, { text: "forged endpoint pin" });
    assert.equal(forgedAuthor.code, "E_SEND_UNAUTHORIZED");
    assert.equal(forgedAuthor.outcomeKnown, true);
    f.mutateNext(frame => { frame.targetOriginEpoch = randomUUID(); });
    const forgedOrigin = await a.send(rb.id, { text: "wrong broker incarnation" });
    assert.equal(forgedOrigin.code, "E_TARGET_REBOUND");
    assert.equal(forgedOrigin.outcomeKnown, true);
    assert.equal(bi.length, 0);
    f.blockPersistence(0);
    const framesBefore = f.frames.length;
    const notDispatched = await a.send(rb.id, { text: "fail closed on persistence" });
    assert.equal(notDispatched.code, "E_CONVERSATION_STATE_FAILURE");
    assert.equal(notDispatched.delivery, "failed");
    assert.equal(notDispatched.outcomeKnown, true);
    assert.equal(f.frames.length, framesBefore);
    assert.equal(bi.length, 0);
  } finally { await f.close(); }
});

test("exact-capable send-only peers preserve known-negative ordinary refresh without replaying uncertain work", { concurrency: false }, async () => {
  const f = await fixture("send-only-exact");
  try {
    const a = await f.connect(0, "a");
    let b = await f.connect(1, "b");
    const rb = await f.remote(a, "b");
    assert.equal(rb.federation?.conversation, false);
    f.hold();
    const sending = a.send(rb.id, { text: "known-negative refresh", messageId: "known_negative_1234" });
    await f.held();
    await b.disconnect();
    b = await f.connect(1, "b");
    const bi = inbox(b);
    const replacement = await until(async () => {
      const row = await f.remote(a, "b");
      return row.endpointEpoch !== rb.endpointEpoch ? row : undefined;
    });
    await f.release();
    const refreshed = await sending;
    assert.equal(refreshed.delivered, true, "correlated preacceptance rebound allows ordinary discovery to refresh");
    assert.equal(refreshed.outcomeKnown, true);
    assert.equal(refreshed.recipient?.endpointEpoch, replacement.endpointEpoch);
    await until(() => bi.find(row => row.message.id === refreshed.id));
    assert.equal(bi.length, 1);
    assert.equal(f.frames.length, 2, "only the destination-rejected attempt was repeated");
    assert.equal(f.frames[0]?.targetEndpointEpoch, rb.endpointEpoch);
    assert.equal(f.frames[1]?.targetEndpointEpoch, replacement.endpointEpoch);
    assert.equal((await a.send(rb.id, { text: "known-negative refresh", messageId: refreshed.id })).delivered, true);
    assert.equal(f.frames.length, 2, "accepted replay returns prior result without dispatch");
  } finally { await f.close(); }
});


test("torn journal recovery disables federated dispatch without disabling unrelated local conversation delivery", { concurrency: false }, async () => {
  const f = await fixture();
  try {
    let a = await f.connect(0, "a");
    const b = await f.connect(1, "b"), bi = inbox(b);
    const first = await a.send((await f.remote(a, "b")).id, { text: "original remote delivery" });
    assert.equal(first.delivered, true);
    await until(() => bi.find(row => row.message.id === first.id));
    await f.unlink();
    f.corruptJournal(0);
    await f.restart(0);
    a = await f.connect(0, "a");
    const local = await f.connect(0, "local"), li = inbox(local);
    await f.link();
    const rb = await f.remote(a, "b");
    const framesBefore = f.frames.length;
    const retry = await a.send(rb.id, { messageId: first.id, text: "original remote delivery" });
    assert.equal(retry.code, "E_DELIVERY_UNKNOWN", "a verified prior attempt survives an uncommitted torn suffix");
    assert.equal(retry.outcomeKnown, false);
    assert.equal(retry.retryable, false);
    const fresh = await a.send(rb.id, { text: "no fresh federated dispatch while journal is corrupt" });
    assert.equal(fresh.delivered, false);
    assert.equal(fresh.retryable, false);
    assert.equal(f.frames.length, framesBefore);
    const ordinaryLocal = await a.send("local", { text: "unrelated local delivery still works" });
    assert.equal(ordinaryLocal.delivered, true);
    await until(() => li.find(row => row.message.id === ordinaryLocal.id));
    assert.equal(bi.length, 1);
  } finally { await f.close(); }
});
