import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { Duplex, Readable, Transform, Writable } from "node:stream";
import { createMessageReader } from "./broker/framing.ts";
import { once } from "node:events";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { PeerStreamController } from "./broker/attachment.ts";
import { getBrokerSocketPath } from "./broker/paths.ts";
import { getTsxCliPath } from "./broker/spawn.ts";
import { createExtensionHarness, type CapturedToolResult } from "./test/extension-harness.ts";
import type { Message } from "./types.ts";
import { restoreConversationHistory } from "./conversation-history.ts";

// This test process owns its fixture brokers, never the invoking agent's runtime.
for (const key of Object.keys(process.env)) {
  if (key.startsWith("PI_PARLEY_") || key.startsWith("PI_SUBAGENT_") || key === "PI_CODING_AGENT_DIR") delete process.env[key];
}
const repo = process.cwd();
const text = (result: CapturedToolResult) => result.content.map((part) => part.text).join("\n");

async function startBroker(agentDir: string): Promise<ChildProcess> {
  const child = spawn(process.execPath, [getTsxCliPath(), path.join(repo, "broker/broker.ts")], {
    cwd: repo, env: { ...process.env, PI_CODING_AGENT_DIR: agentDir }, stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr!.on("data", (data: Buffer) => { stderr = (stderr + data.toString()).slice(-4000); });
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => finish(new Error(`Broker startup timed out: ${stderr}`)), 10_000);
    const onData = (data: Buffer) => { if (data.toString().includes("Parley broker started")) finish(); };
    const onExit = () => finish(new Error(`Broker exited during startup: ${stderr}`));
    const finish = (error?: Error) => {
      clearTimeout(timer); child.stdout!.off("data", onData); child.off("exit", onExit);
      if (error) { child.kill("SIGTERM"); reject(error); } else resolve();
    };
    child.stdout!.on("data", onData); child.once("exit", onExit);
  });
  return child;
}
async function stopBroker(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGTERM");
  await exited;
}
async function inAgentDir<T>(dir: string, work: () => Promise<T>): Promise<T> {
  const previous = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = dir;
  try { return await work(); }
  finally {
    if (previous === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previous;
  }
}
function caller(harness: ReturnType<typeof createExtensionHarness>) {
  return (params: Record<string, unknown>) => harness.tools.find((tool) => tool.name === "parley")!
    .execute("federated-call", params, new AbortController().signal, undefined, harness.ctx);
}
async function waitUntil(predicate: () => boolean | Promise<boolean>, explanation: string): Promise<void> {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail(explanation);
}

// A deliberately lossy opaque provider, not a mock broker. It admits the ask
// and its real reply, but withholds just that ask's delivery acknowledgement.
function lossyProvider(socket: net.Socket) {
  const lost = new Set<string>();
  const dispatched: string[] = [];
  const observe = createMessageReader((raw) => {
    const frame = raw as { type?: string; sendId?: string; message?: { text?: string; expectsReply?: boolean } };
    if (frame.type === "peer_send" && frame.message?.expectsReply && frame.message.text?.includes("fast:lost-ack")) {
      lost.add(frame.sendId!); dispatched.push(frame.message.text);
    }
  }, (error) => socket.destroy(error));
  const decode = createMessageReader((raw) => {
    const value = raw as { type?: string; sendId?: string };
    if (value.type === "peer_send_result" && lost.has(value.sendId!)) return;
    const payload = Buffer.from(JSON.stringify(raw));
    const frame = Buffer.alloc(4 + payload.length);
    frame.writeUInt32BE(payload.length); payload.copy(frame, 4);
    incoming.push(frame);
  }, (error) => incoming.destroy(error));
  const incoming = new Transform({ transform(chunk: Buffer, _encoding, done) { decode(chunk); done(); } });
  const outgoing = new Writable({
    write(chunk: Buffer, _encoding, done) { observe(chunk); socket.write(chunk, done); },
    final(done) { socket.end(done); },
    destroy(error, done) {
      if (socket.closed) { done(error); return; }
      socket.once("close", () => done(error)); socket.destroy();
    },
  });
  socket.on("error", (error) => incoming.destroy(error));
  socket.pipe(incoming);
  return { stream: Duplex.fromWeb({ readable: Readable.toWeb(incoming), writable: Writable.toWeb(outgoing) }, { allowHalfOpen: true }), dispatched };
}

test("neutral registered provider delivers bidirectional extension asks, fast answers and retained threaded notifications", { timeout: 45_000 }, async () => {
  const { default: extension } = await import("./index.ts");
  const dirs = [0, 1].map(() => mkdtempSync(path.join(process.platform === "win32" ? tmpdir() : "/tmp", "pl-ext-")));
  const brokers: ChildProcess[] = [];
  const a = createExtensionHarness("consumer-a", { sessionId: "consumer-a" });
  const b = createExtensionHarness("consumer-b", { sessionId: "consumer-b" });
  const callA = caller(a), callB = caller(b);
  const controller = new PeerStreamController();
  const automaticReplies: Promise<CapturedToolResult>[] = [];
  try {
    for (const dir of dirs) brokers.push(await startBroker(dir));
    for (const [index, harness] of [a, b].entries()) {
      await inAgentDir(dirs[index]!, async () => {
        const childMetadata: Record<string, string> = index === 0 ? {
          PI_SUBAGENT_ORCHESTRATOR_TARGET: "consumer-b",
          PI_SUBAGENT_ORCHESTRATOR_SESSION_ID: "consumer-b",
          PI_SUBAGENT_RUN_ID: "fixture-run-1234",
          PI_SUBAGENT_CHILD_AGENT: "consumer",
          PI_SUBAGENT_CHILD_INDEX: "0",
          PI_SUBAGENT_PARLEY_SESSION_NAME: "subagent-consumer-fixture-run-1",
        } : {};
        try {
          Object.assign(process.env, childMetadata);
          extension(harness.pi as never);
          await harness.emitLifecycle("session_start");
          if (index === 0) {
            const advertised = await caller(harness)({ action: "advertise", name: "consumer-a" });
            assert.notEqual(advertised.details?.error, true, text(advertised));
          }
          const listed = await caller(harness)({ action: "list" });
          assert.notEqual(listed.details?.error, true);
        } finally {
          for (const key of Object.keys(childMetadata)) delete process.env[key];
        }
      });
    }
    let acquisitions = 0;
    let faults: ReturnType<typeof lossyProvider> | undefined;
    const provider = controller.registerProvider<{ socketPath: string }>(async (binding, signal) => {
      acquisitions++;
      const stream = net.connect({ path: binding.socketPath, allowHalfOpen: true });
      try { await once(stream, "connect", { signal }); }
      catch (error) {
        const closed = stream.closed ? Promise.resolve() : once(stream, "close");
        stream.destroy(); await closed;
        throw error;
      }
      faults = lossyProvider(stream);
      return faults.stream;
    });
    assert.equal(acquisitions, 0, "registration is explicit and never auto-connects");
    const attachment = await provider.attach({ socketPath: getBrokerSocketPath(process.platform, dirs[1]) }, {
      localBroker: getBrokerSocketPath(process.platform, dirs[0]),
      localOrigin: { id: "host:consumer-a" }, remoteOrigin: { id: "host:consumer-b" },
      localScopeBindings: [{ localScopeId: null, localScopeAlias: "a", remoteScopeAlias: "b" }],
      remoteScopeBindings: [{ localScopeId: null, localScopeAlias: "b", remoteScopeAlias: "a" }],
    });
    assert.equal(acquisitions, 1, "one explicit attachment performs one acquisition");
    assert.equal(attachment.remoteOrigin.id, "host:consumer-b");
    await waitUntil(async () => /consumer-b.*text conversations/.test(text(await callA({ action: "list" })))
      && /consumer-a.*text conversations/.test(text(await callB({ action: "list" }))), "both consumers must truthfully advertise negotiated conversations");

    // Answer as soon as the host observes the request: no manual relay or slow
    // model step may be needed to install the original sender's correlation.
    for (const [harness, call] of [[a, callA], [b, callB]] as const) {
      const capture = harness.pi.sendMessage;
      harness.pi.sendMessage = (envelope, options) => {
        capture(envelope, options);
        const details = envelope.details as { message?: Message } | undefined;
        if (details?.message?.expectsReply && details.message.content.text.includes("fast:")) {
          automaticReplies.push(call({ action: "reply", replyTo: details.message.id, message: `answer:${details.message.content.text}` }));
        }
      };
    }
    for (const [asker, responder, call, answerCall, target] of [[a, b, callA, callB, "consumer-b"], [b, a, callB, callA, "consumer-a"]] as const) {
      const result = await call({ action: "ask", to: target, message: `fast:${target}` });
      assert.notEqual(result.details?.error, true, text(result));
      assert.match(text(result), new RegExp(`answer:fast:${target}`));
      const authoredAnswer = await automaticReplies.at(-1)!;
      assert.equal(authoredAnswer.details?.delivered, true, text(authoredAnswer));
      const questionId = result.details?.messageId as string;
      const answerId = result.details?.replyMessageId as string;
      assert.match(questionId, /^oqm1\./);
      assert.match(answerId, /^oqm1\./);
      const history = asker.entries.find((entry) => entry.type === "parley_ask_pending" && (entry.data as { messageId?: string }).messageId === questionId);
      assert.ok((history?.data as { endpointEpoch?: string }).endpointEpoch);
      assert.ok((history?.data as { originEpoch?: string }).originEpoch);
      assert.match(text(await answerCall({ action: "read", messageId: questionId })), new RegExp(`fast:${target}`));
      assert.match(text(await call({ action: "read", messageId: answerId })), new RegExp(`answer:fast:${target}`));
      assert.equal(asker.sentMessages.some((entry) => (entry.message.details as { message?: Message })?.message?.id === answerId), false,
        "blocking answers return through the tool, not a duplicate wakeup");
      assert.doesNotMatch(text(await answerCall({ action: "pending" })), /awaiting your reply/);
      assert.ok(responder.entries.some((entry) => entry.type === "parley_inbound_settled" && (entry.data as { messageId?: string }).messageId === questionId));
    }
    const question = await callA({ action: "ask", to: "consumer-b", message: "async release decision", blocking: false });
    const questionId = question.details?.messageId as string;
    assert.match(questionId, /^oqm1\./);
    await waitUntil(() => b.sentMessages.some((entry) => (entry.message.details as { message?: Message })?.message?.id === questionId), "async question reaches the remote consumer");
    const progress = await callB({ action: "send", to: "consumer-a", replyTo: questionId, message: "still investigating" });
    assert.equal(progress.details?.delivered, true, text(progress));
    await waitUntil(() => a.sentMessages.some((entry) => entry.message.content?.includes("still investigating")), "progress is retained without completing the question");
    assert.equal(((await callA({ action: "status" })).details?.outstandingAsks as Array<{ messageId: string }>)[0]?.messageId, questionId);
    assert.match(text(await callB({ action: "pending" })), /async release decision/);
    const recovered = restoreConversationHistory(a.entries.map((entry) => ({ type: "custom", customType: entry.type, data: entry.data })));
    assert.equal(recovered.outgoing.size, 1);
    assert.ok(recovered.outgoing.get(questionId)?.endpointEpoch);
    assert.ok(recovered.outgoing.get(questionId)?.originEpoch);
    assert.equal(recovered.incoming.get(progress.details?.messageId as string)?.message.replyTo, questionId,
      "recovery preserves the author-qualified handle and original ask correlation");
    const answer = await callB({ action: "reply", replyTo: questionId, message: "release approved" });
    assert.equal(answer.details?.delivered, true, text(answer));
    await waitUntil(async () => ((await callA({ action: "status" })).details?.outstandingAsks as unknown[]).length === 0, "only the completing answer settles the original ask");

    const notice = await callA({ action: "send", to: "consumer-b", message: "notification with retained identity" });
    assert.equal(notice.details?.delivered, true, text(notice));
    const noticeId = notice.details?.messageId as string;
    assert.match(noticeId, /^oqm1\./);
    await waitUntil(() => b.sentMessages.some((entry) => (entry.message.details as { message?: Message })?.message?.id === noticeId), "remote notification carries its canonical retained handle");
    const noticeReply = await callB({ action: "reply", replyTo: noticeId, message: "notification acknowledged" });
    assert.equal(noticeReply.details?.delivered, true, text(noticeReply));
    await waitUntil(() => a.sentMessages.some((entry) => entry.message.content?.includes("notification acknowledged")), "notification reply uses its recorded reverse edge");
    assert.match(text(await callB({ action: "read", messageId: noticeId })), /notification with retained identity/);

    const unknownWithAnswer = await callA({ action: "ask", to: "consumer-b", message: "fast:lost-ack" });
    assert.match(text(unknownWithAnswer), /answer:fast:lost-ack/, "a real correlated answer is not swallowed by a missing ask ACK");
    assert.equal(unknownWithAnswer.details?.delivery, "unknown");
    assert.equal(unknownWithAnswer.details?.outcomeKnown, false, "the independently observed answer does not manufacture a transport ACK");
    assert.equal(unknownWithAnswer.details?.retryable, false);
    assert.match(unknownWithAnswer.details?.replyMessageId as string, /^oqm1\./);
    assert.equal((await automaticReplies.at(-1)!).details?.delivered, true);
    assert.deepEqual(faults!.dispatched, ["fast:lost-ack"], "unknown acceptance never automatically replays the ask");
    assert.deepEqual((await callA({ action: "status" })).details?.outstandingAsks, []);
    const answerHooks = await a.emitLifecycleResults("tool_result", {
      toolName: "parley", ...unknownWithAnswer, isError: false,
    });
    assert.equal(answerHooks.some((result) => (result as { isError?: boolean } | undefined)?.isError === true), false,
      "the host-visible operation succeeds with its received answer, independently of unknown acceptance");
    const theme = { fg: (_name: string, value: string) => value, bold: (value: string) => value };
    const renderedAnswer = a.tools.find((tool) => tool.name === "parley")!
      .renderResult!(unknownWithAnswer, { isPartial: false, expanded: true }, theme, { isError: false })
      .render(120).join("\n");
    assert.match(renderedAnswer, /^\? /, "the receipt still visibly carries its uncertainty");
    assert.doesNotMatch(renderedAnswer, /✗/);

    const supervisorTool = a.tools.find((tool) => tool.name === "contact_supervisor")!;
    for (const question of ["fast:supervisor decision", "fast:lost-ack"]) {
      const result = await supervisorTool.execute("remote-supervisor-call", { reason: "need_decision", message: question },
        new AbortController().signal, undefined, a.ctx);
      assert.notEqual(result.details?.error, true, text(result));
      assert.match(text(result), /\*\*Reply from supervisor:\*\*/);
      assert.match(text(result), new RegExp(question));
      assert.match(result.details?.messageId as string, /^oqm1\./);
      assert.match(result.details?.replyMessageId as string, /^oqm1\./);
      const pending = a.entries.find((entry) => entry.type === "parley_ask_pending"
        && (entry.data as { messageId?: string }).messageId === result.details?.messageId);
      assert.ok((pending?.data as { endpointEpoch?: string }).endpointEpoch);
      assert.ok((pending?.data as { originEpoch?: string }).originEpoch);
      if (question === "fast:lost-ack") {
        assert.equal(result.details?.delivery, "unknown");
        assert.equal(result.details?.outcomeKnown, false);
        assert.equal(result.details?.retryable, false);
        assert.equal(faults!.dispatched.length, 2, "one dispatch per lost-ACK ask, with no replay");
      }
      const hooks = await a.emitLifecycleResults("tool_result", { toolName: "contact_supervisor", ...result, isError: false });
      assert.equal(hooks.some((hook) => (hook as { isError?: boolean } | undefined)?.isError === true), false);
      const rendered = supervisorTool.renderResult!(result, { isPartial: false, expanded: true }, theme, { isError: false })
        .render(120).join("\n");
      assert.match(rendered, question === "fast:lost-ack" ? /^\? / : /^✓ /);
      assert.doesNotMatch(rendered, /✗/);
      assert.deepEqual((await callA({ action: "status" })).details?.outstandingAsks, []);
    }
  } finally {
    await controller.close();
    for (const harness of [a, b]) await harness.emitLifecycle("session_shutdown");
    for (const broker of brokers) await stopBroker(broker);
    for (const dir of dirs) rmSync(dir, { recursive: true, force: true });
  }
});
