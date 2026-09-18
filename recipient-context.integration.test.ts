import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createExtensionHarness } from "./test/extension-harness.ts";
import type { Message, SessionInfo } from "./types.ts";

const home = mkdtempSync(path.join(tmpdir(), "ic-r-"));
process.env.HOME = home;
process.env.USERPROFILE = home;
for (const key of Object.keys(process.env)) {
  if (key.startsWith("PI_SUBAGENT_") || key.startsWith("PI_PARLEY_") || key.startsWith("PI_INTERCOM_") || key === "PI_CODING_AGENT_DIR") delete process.env[key];
}
const { ParleyClient } = await import("./broker/client.ts");
const { getTsxCliPath } = await import("./broker/spawn.ts");
const { default: extension } = await import("./index.ts");
const broker = spawn(process.execPath, [getTsxCliPath(), path.resolve("broker/broker.ts")], { env: process.env, stdio: ["ignore", "pipe", "pipe"] });
let brokerErrors = "";
broker.stderr.on("data", (data) => { brokerErrors = (brokerErrors + String(data)).slice(-4000); });
const ready = new Promise<void>((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error("Recipient test broker startup timed out")), 10_000);
  broker.stdout.on("data", (data) => { if (String(data).includes("Parley broker started")) { clearTimeout(timer); resolve(); } });
  broker.once("exit", () => { clearTimeout(timer); reject(new Error(`Broker exited: ${brokerErrors}`)); });
});
test.before(() => ready);
test.after(async () => {
  if (broker.exitCode === null && broker.signalCode === null) { broker.kill("SIGTERM"); await once(broker, "exit"); }
  rmSync(home, { recursive: true, force: true });
});

async function until(check: () => boolean | Promise<boolean>, label: string) {
  const deadline = Date.now() + 4000;
  while (!(await check())) {
    assert.ok(Date.now() < deadline, `Timed out: ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
async function colleague(name: string) {
  const peer = new ParleyClient();
  await peer.connect({ name, cwd: process.cwd(), model: "test", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now(), description: "coordinating the database migration review" });
  return peer;
}
async function start(harness: ReturnType<typeof createExtensionHarness>, peer: InstanceType<typeof ParleyClient>, name: string) {
  extension(harness.pi as never);
  await harness.emitLifecycle("session_start");
  let target: SessionInfo | undefined;
  await until(async () => Boolean(target = (await peer.listSessions()).find((item) => item.name === name)), `${name} registration`);
  return target!;
}
function call(harness: ReturnType<typeof createExtensionHarness>, params: Record<string, unknown>) {
  return harness.tools.find((tool) => tool.name === "parley")!.execute("scenario", params, AbortSignal.timeout(8_000), undefined, harness.ctx);
}
function text(result: { content: { text: string }[] }) { return result.content.map((item) => item.text).join("\n"); }
async function modelContext(harness: ReturnType<typeof createExtensionHarness>, messages: unknown[] = []) {
  const result = await harness.emitLifecycleResults("context", { messages });
  return (result.find(Boolean) as { messages: Array<{ role: string; content: string; details?: unknown }> }).messages;
}

test("busy headless answers retain correlation and survive fire-and-forget host rejection without treating progress as an answer", async () => {
  const peer = await colleague("context-planner");
  const harness = createExtensionHarness("context-worker", { sessionId: "context-worker-id", isIdle: () => false, hasUI: false, persistMessages: false });
  const asked: Message[] = [];
  peer.on("message", (_from, message) => asked.push(message));
  try {
    const target = await start(harness, peer, "context-worker");
    await call(harness, { action: "ask", to: "context-planner", message: "Can the index be built online?", blocking: false });
    await call(harness, { action: "ask", to: "context-planner", message: "Is rollback safe?", blocking: false });
    await until(() => asked.length === 2, "two questions");
    const [first, second] = asked;
    // This models the real void-returning host API: a later rejection never reaches the extension caller.
    let asynchronousFailures = 0;
    const sendMessage = harness.pi.sendMessage;
    harness.pi.sendMessage = (message, options) => {
      sendMessage(message, options);
      void Promise.reject(new Error("Host queue failed asynchronously")).catch(() => { asynchronousFailures++; });
    };
    await peer.send(target.id, { text: "Still checking the rollout", replyTo: first!.id, completesAsk: false });
    await peer.send(target.id, {
      text: "Yes, with the concurrent option", replyTo: first!.id, completesAsk: true,
      attachments: [{ type: "file", name: "/remote/migration.sql", language: "sql", content: "CREATE INDEX CONCURRENTLY example ON records (id);" }],
    });
    await until(() => asynchronousFailures === 2, "asynchronous host failures");
    assert.ok(harness.sentMessages.every((entry) => entry.options?.deliverAs === "steer"));
    const beforeModel = text(await call(harness, { action: "status" }));
    assert.ok(beforeModel.includes(first!.id), "void send return must not settle the question");
    assert.ok(beforeModel.includes(second!.id));
    const messages = await modelContext(harness);
    const answer = messages.find((item) => item.content.includes("Yes, with"))!.content;
    assert.ok(answer.includes(first!.id));
    assert.match(answer, /Can the index be built online/);
    assert.match(answer, /coordinating the database migration/);
    assert.match(answer, /snapshot/i);
    assert.match(answer, /CREATE INDEX CONCURRENTLY example ON records \(id\);/);
    assert.doesNotMatch(answer, /To reply, use|parley\(\{|broker delivered|receiver received|seq \d/);
    const afterModel = text(await call(harness, { action: "status" }));
    assert.ok(!afterModel.includes(first!.id));
    assert.ok(afterModel.includes(second!.id));
    assert.equal((await modelContext(harness)).filter((item) => item.content.includes("Yes, with")).length, 1, "fallback remains context when the failed host never persisted it");
    await peer.send(target.id, { text: "Rollback is still under review", replyTo: second!.id, completesAsk: false });
    await until(() => asynchronousFailures === 3, "progress delivered");
    await modelContext(harness);
    assert.ok(text(await call(harness, { action: "status" })).includes(second!.id), "threaded progress is not answer intent");
  } finally { await harness.emitLifecycle("session_shutdown"); await peer.disconnect(); }
});

test("a busy headless recipient learns a withdrawal even when its host drops the cancellation injection", async () => {
  const peer = await colleague("withdrawal-planner");
  const harness = createExtensionHarness("withdrawal-worker", { sessionId: "withdrawal-worker-id", isIdle: () => false, persistMessages: false });
  try {
    const target = await start(harness, peer, "withdrawal-worker");
    const sent = await peer.send(target.id, { text: "Start the migration after review", expectsReply: true, senderWaitMode: "nonblocking" });
    await until(() => harness.sentMessages.length === 1, "request steering");
    const request = (await modelContext(harness))[0]!.content;
    assert.match(request, /Reply requested · async ask/);
    assert.match(request, /Start the migration after review/);
    assert.equal((await peer.cancelMessage(sent.id)).delivered, true);
    await until(() => harness.sentMessages.some((item) => item.message.customType === "parley_message_control"), "withdrawal steering");
    const withdrawal = (await modelContext(harness)).find((item) => item.content.includes("withdrawn by its sender"))!.content;
    assert.ok(withdrawal.includes(sent.id));
    assert.match(withdrawal, /Start the migration after review/);
    assert.match(withdrawal, /does not undo/);
    assert.match(text(await call(harness, { action: "read", messageId: sent.id })), /Status: withdrawn/);
    assert.doesNotMatch(text(await call(harness, { action: "pending" })), /awaiting your reply/);
    assert.ok(harness.sentMessages.every((entry) => entry.options?.deliverAs === "steer"));
    const laterContext = await modelContext(harness);
    assert.ok(laterContext.every((item) => !item.content.includes("Reply requested")), "withdrawn fallback must not reappear as a fresh request");
  } finally { await harness.emitLifecycle("session_shutdown"); await peer.disconnect(); }
});

test("restart recovers full pending snapshots, unsurfaced answers and outstanding questions without leaking them to another session", async () => {
  const peer = await colleague("recovery-planner");
  const first = createExtensionHarness("recovery-worker", { sessionId: "recovery-worker-id", isIdle: () => false, persistMessages: false });
  const second = createExtensionHarness("recovery-worker", { sessionId: "recovery-worker-id", isIdle: () => false, persistMessages: false });
  const fresh = createExtensionHarness("fresh-worker", { sessionId: "fresh-worker-id" });
  const legacy = createExtensionHarness("legacy-worker", { sessionId: "legacy-worker-id" });
  const asked: Message[] = [];
  peer.on("message", (_from, message) => asked.push(message));
  try {
    const target = await start(first, peer, "recovery-worker");
    const request = await peer.send(target.id, { text: "Review the entire migration plan, including recovery", expectsReply: true,
      attachments: [{ type: "context", name: "recovery details", content: "The complete recovery protocol is retained, not just the first eighty characters." }] });
    await call(first, { action: "ask", to: "recovery-planner", message: "Can the maintenance window move?", blocking: false });
    await until(() => asked.length === 1, "outgoing question");
    await peer.send(target.id, { text: "The window can move to Sunday", replyTo: asked[0]!.id, completesAsk: true });
    await until(() => first.sentMessages.length === 2, "answer received but not persisted");
    second.entries.push(...structuredClone(first.entries));
    // Resume a journal whose broker reply window has since elapsed: the work is still unresolved.
    const oldRequest = second.entries.find((entry) => entry.type === "parley_inbound_received" && (entry.data as { message: Message }).message.id === request.id)!;
    (oldRequest.data as { message: Message }).message.replyDeadline = Date.now() - 1;
    await first.emitLifecycle("session_shutdown");
    await start(second, peer, "recovery-worker");
    assert.ok(text(await call(second, { action: "status" })).includes(asked[0]!.id), "unobserved answer must not settle on restart");
    const recovered = await modelContext(second);
    assert.match(recovered.map((message) => message.content).join("\n"), /The window can move to Sunday/);
    assert.ok(!text(await call(second, { action: "status" })).includes(asked[0]!.id));
    const full = text(await call(second, { action: "read", messageId: request.id }));
    assert.match(full, /complete recovery protocol is retained/);
    assert.match(full, /snapshot/i);
    const pending = text(await call(second, { action: "pending" }));
    assert.ok(pending.includes(request.id));
    assert.match(pending, /reply window elapsed, not withdrawn/);
    await call(second, { action: "reply", replyTo: request.id, message: "Reviewed the migration and recovery protocol" });
    // Older sessions have only nested parley_sent.message.replyTo, not a separate settlement event.
    legacy.entries.push(...structuredClone(second.entries.filter((entry) => entry.type !== "parley_inbound_settled")));
    await start(legacy, peer, "legacy-worker");
    assert.ok(!text(await call(legacy, { action: "pending" })).includes(request.id), "legacy answered requests do not resurrect");
    await start(fresh, peer, "fresh-worker");
    assert.ok(!text(await call(fresh, { action: "pending" })).includes(request.id));
    assert.ok(!text(await call(fresh, { action: "status" })).includes(asked[0]!.id));
  } finally {
    await first.emitLifecycle("session_shutdown"); await second.emitLifecycle("session_shutdown"); await fresh.emitLifecycle("session_shutdown"); await legacy.emitLifecycle("session_shutdown"); await peer.disconnect();
  }
});

test("implicit replies survive inspection without guessing between fresh conversations or leaking into another run", async () => {
  const alpha = await colleague("boundary-alpha");
  const beta = await colleague("boundary-beta");
  const harness = createExtensionHarness("boundary-worker", { sessionId: "boundary-worker-id", isIdle: () => false, persistMessages: false });
  const alphaReplies: Message[] = [];
  const betaReplies: Message[] = [];
  alpha.on("message", (_from, message) => alphaReplies.push(message));
  beta.on("message", (_from, message) => betaReplies.push(message));
  try {
    const target = await start(harness, alpha, "boundary-worker");
    const prior = await alpha.send(target.id, { text: "The earlier schema notes are ready" });
    await until(() => harness.sentMessages.length === 1, "earlier notification");
    await harness.emitLifecycle("agent_start");
    await harness.emitLifecycle("turn_start");
    const history = await modelContext(harness);
    // The live trial failed here: inspecting status is another model/tool iteration,
    // not the end of the conversation that prompted it.
    await call(harness, { action: "status" });
    await harness.emitLifecycle("turn_end");
    await harness.emitLifecycle("turn_start");
    await modelContext(harness, history);
    await call(harness, { action: "pending" });
    await harness.emitLifecycle("turn_end");
    await harness.emitLifecycle("turn_start");
    await modelContext(harness, history);
    const firstReply = await call(harness, { action: "reply", message: "Thanks for the schema notes" });
    assert.equal(firstReply.details?.error, undefined);
    await until(() => alphaReplies.length === 1, "first ordinary reply");
    assert.equal(alphaReplies[0]!.replyTo, prior.id);
    await harness.emitLifecycle("turn_end");

    const current = await beta.send(target.id, { text: "The current release notes are ready" });
    await until(() => harness.sentMessages.length === 2, "current notification");
    // Actual Pi ordering: turn_start runs before steering has been consumed into model context.
    await harness.emitLifecycle("turn_start");
    const nextHistory = await modelContext(harness, history);
    assert.ok(text(await call(harness, { action: "pending" })).includes(current.id));
    // Leave this ordinary note unanswered: a later unrelated run must not inherit it.
    await harness.emitLifecycle("turn_end");
    await harness.emitLifecycle("agent_end");
    await harness.emitLifecycle("agent_start");
    await harness.emitLifecycle("turn_start");
    await modelContext(harness, nextHistory);
    assert.match(text(await call(harness, { action: "reply", message: "No current colleague" })), /No active parley context/);
    assert.equal(betaReplies.length, 0);

    const update = await alpha.send(target.id, { text: "The schema note has an addendum" });
    const question = await beta.send(target.id, { text: "Can the release proceed?", expectsReply: true });
    await until(() => harness.sentMessages.length === 4, "simultaneous note and question");
    await harness.emitLifecycle("turn_end");
    await harness.emitLifecycle("turn_start");
    const mixedHistory = await modelContext(harness, nextHistory);
    await call(harness, { action: "status" });
    await harness.emitLifecycle("turn_end");
    await harness.emitLifecycle("turn_start");
    await modelContext(harness, mixedHistory);
    const ambiguous = await call(harness, { action: "reply", message: "This needs a target" });
    assert.equal(ambiguous.details?.error, true, "the sole pending ask does not disambiguate two fresh conversations");
    assert.ok(text(ambiguous).includes(update.id), "the ordinary message is a visible candidate");
    assert.ok(text(ambiguous).includes(question.id), "the question is a visible candidate");
    assert.equal(alphaReplies.length, 1);
    assert.equal(betaReplies.length, 0);
    const targeted = await call(harness, { action: "reply", to: "boundary-alpha", message: "Thanks for the addendum" });
    assert.notEqual(targeted.details?.error, true);
    await until(() => alphaReplies.length === 2, "explicitly targeted ordinary reply");
    assert.equal(alphaReplies[1]!.replyTo, update.id);
    assert.ok(text(await call(harness, { action: "pending" })).includes(question.id));
    await call(harness, { action: "reply", replyTo: question.id, message: "Yes, the release can proceed" });
    await until(() => betaReplies.length === 1, "explicitly targeted answer");
    assert.equal(betaReplies[0]!.replyTo, question.id);

    const olderAsk = await alpha.send(target.id, { text: "Can you approve the schema?", expectsReply: true });
    await until(() => harness.sentMessages.length === 5, "older question");
    const olderHistory = await modelContext(harness, mixedHistory);
    const newerNote = await alpha.send(target.id, { text: "An unrelated schema note is available" });
    await until(() => harness.sentMessages.length === 6, "new note from the same sender");
    await modelContext(harness, olderHistory);
    await call(harness, { action: "reply", to: "boundary-alpha", message: "Thanks for the new note" });
    await until(() => alphaReplies.length === 3, "reply with an explicit sender");
    assert.equal(alphaReplies[2]!.replyTo, newerNote.id, "specifying the sender must not switch to an older question");
    assert.ok(text(await call(harness, { action: "pending" })).includes(olderAsk.id), "acknowledging a note does not approve the older request");
  } finally { await harness.emitLifecycle("session_shutdown"); await alpha.disconnect(); await beta.disconnect(); }
});

test("history write failures preserve live messages, withdrawals, answer settlement and delivered replies while warning about recovery", async () => {
  const peer = await colleague("history-failure-planner");
  let historyBroken = false;
  let failedWrites = 0;
  const harness = createExtensionHarness("history-failure-worker", {
    sessionId: "history-failure-worker-id", isIdle: () => false, persistMessages: false,
    appendEntryError: () => { if (historyBroken) { failedWrites++; return new Error("session history disk unavailable"); } },
  });
  const received: Message[] = [];
  peer.on("message", (_from, message) => received.push(message));
  try {
    const target = await start(harness, peer, "history-failure-worker");
    await call(harness, { action: "ask", to: "history-failure-planner", message: "When is the final deployment date?", blocking: false });
    await until(() => received.length === 1, "outgoing question accepted before disk failure");
    const outgoingQuestion = received[0]!;
    historyBroken = true;
    const request = await peer.send(target.id, { text: "Review the migration before release", expectsReply: true });
    await until(() => harness.sentMessages.length === 1, "live request despite history failure");
    assert.match(text(await call(harness, { action: "read", messageId: request.id })), /Review the migration before release/);
    const context = await modelContext(harness);
    assert.match(context.map((item) => item.content).join("\n"), /Review the migration before release/);
    assert.match(context.map((item) => item.content).join("\n"), /history was not fully persisted.*recovery after restart may be incomplete/s);
    assert.ok(failedWrites > 0, "the scenario actually exercised failing host history writes");
    assert.equal((await peer.cancelMessage(request.id)).delivered, true);
    await until(() => harness.sentMessages.some((item) => item.message.customType === "parley_message_control"), "withdrawal despite history failure");
    const cancelled = await modelContext(harness);
    assert.match(cancelled.map((item) => item.content).join("\n"), /withdrawn by its sender/);
    assert.ok(!text(await call(harness, { action: "pending" })).includes(request.id));

    await peer.send(target.id, { text: "The deployment date is Sunday", replyTo: outgoingQuestion.id, completesAsk: true });
    await until(() => harness.sentMessages.some((item) => item.message.content?.includes("The deployment date is Sunday")), "requested answer despite history failure");
    assert.ok(text(await call(harness, { action: "status" })).includes(outgoingQuestion.id));
    assert.match((await modelContext(harness)).map((item) => item.content).join("\n"), /The deployment date is Sunday/);
    assert.ok(!text(await call(harness, { action: "status" })).includes(outgoingQuestion.id), "observed answer settles locally even if its journal write fails");

    const finalRequest = await peer.send(target.id, { text: "Can the release proceed?", expectsReply: true });
    await until(() => harness.sentMessages.some((item) => item.message.content?.includes("Can the release proceed?")), "final request");
    await modelContext(harness);
    const reply = await call(harness, { action: "reply", replyTo: finalRequest.id, message: "Yes, the review is complete" });
    await until(() => received.some((item) => item.replyTo === finalRequest.id), "reply delivered to colleague");
    assert.notEqual(reply.details?.error, true, "history failure must not turn delivered reply into tool failure");
    assert.equal(reply.details?.delivered, true);
    assert.match(text(reply), /history.*not fully persisted|recovery.*incomplete/i);
    assert.ok(!text(await call(harness, { action: "pending" })).includes(finalRequest.id));
  } finally { await harness.emitLifecycle("session_shutdown"); await peer.disconnect(); }
});

test("blocking answers retain full snapshots for exact-ID read and follow-up across reload without an extra incoming copy", async () => {
  const peer = await colleague("blocking-context-planner");
  const first = createExtensionHarness("blocking-context-worker", { sessionId: "blocking-context-worker-id", isIdle: () => false });
  const restored = createExtensionHarness("blocking-context-worker", { sessionId: "blocking-context-worker-id", isIdle: () => false });
  try {
    await start(first, peer, "blocking-context-worker");
    const questionReceived = once(peer, "message", { signal: AbortSignal.timeout(8_000) }) as Promise<[SessionInfo, Message]>;
    const answerResult = call(first, { action: "ask", to: "blocking-context-planner", message: "What is the complete rollback plan?" });
    const [asker, question] = await questionReceived;
    const response = await peer.send(asker.id, {
      text: "Rollback uses the retained index", replyTo: question.id, completesAsk: true,
      attachments: [{ type: "context", name: "complete rollback plan", content: "Retain the original index until validation completes. Restore writes to the original table, then remove the new index." }],
    });
    const answer = await answerResult;
    assert.notEqual(answer.details?.error, true);
    const answerId = text(answer).match(/Reply message ID: ([^\n]+)/)?.[1];
    assert.equal(answerId, response.id, "the model-visible answer identifies the retained reply");
    assert.match(text(await call(first, { action: "read", messageId: answerId })), /Restore writes to the original table, then remove the new index/);
    assert.equal(first.sentMessages.filter((item) => item.message.customType === "parley_message").length, 0, "blocking tool result is the only answer delivery");

    restored.entries.push(...structuredClone(first.entries));
    restored.toolResults.push(...structuredClone(first.toolResults));
    await first.emitLifecycle("session_shutdown");
    await start(restored, peer, "blocking-context-worker");
    const recovered = text(await call(restored, { action: "read", messageId: answerId }));
    assert.match(recovered, /blocking-context-planner/);
    assert.ok(recovered.includes(question.id), "reply correlation survives reload");
    assert.match(recovered, /Restore writes to the original table, then remove the new index/);
    assert.equal(restored.sentMessages.filter((item) => item.message.customType === "parley_message").length, 0, "reload does not inject a second copy of a blocking answer");
    const followUpReceived = once(peer, "message", { signal: AbortSignal.timeout(8_000) }) as Promise<[SessionInfo, Message]>;
    const followUp = await call(restored, { action: "reply", replyTo: answerId, message: "Thanks; how long should we retain the original index?" });
    assert.equal(followUp.details?.delivered, true, text(followUp));
    const [, followUpMessage] = await followUpReceived;
    assert.equal(followUpMessage.replyTo, answerId);
    assert.match(followUpMessage.content.text, /how long should we retain/);
  } finally { await first.emitLifecycle("session_shutdown"); await restored.emitLifecycle("session_shutdown"); await peer.disconnect(); }
});

test("recovery reconciles a host-persisted answer even when the settlement record was never written", async () => {
  const peer = await colleague("persisted-answer-planner");
  const first = createExtensionHarness("persisted-answer-worker", { sessionId: "persisted-answer-worker-id", isIdle: () => false, persistMessages: false });
  const restored = createExtensionHarness("persisted-answer-worker", { sessionId: "persisted-answer-worker-id", isIdle: () => false });
  try {
    const target = await start(first, peer, "persisted-answer-worker");
    const questionReceived = once(peer, "message", { signal: AbortSignal.timeout(8_000) }) as Promise<[SessionInfo, Message]>;
    await call(first, { action: "ask", to: "persisted-answer-planner", message: "Which maintenance window was approved?", blocking: false });
    const [, question] = await questionReceived;
    await peer.send(target.id, { text: "Sunday morning is approved", replyTo: question.id, completesAsk: true });
    await until(() => first.sentMessages.some((item) => item.message.content?.includes("Sunday morning is approved")), "answer offered to host");
    assert.ok(text(await call(first, { action: "status" })).includes(question.id), "the unconfirmed send remains outstanding");
    restored.entries.push(...structuredClone(first.entries));
    // The host persisted the answer immediately before process loss, without returning a confirmation event.
    restored.persistedMessages.push(...structuredClone(first.sentMessages.map((item) => item.message)));
    await first.emitLifecycle("session_shutdown");
    await start(restored, peer, "persisted-answer-worker");
    assert.ok(!text(await call(restored, { action: "status" })).includes(question.id), "the persisted answer settles the recovered question");
    assert.equal(restored.sentMessages.length, 0, "already-persisted answer is not reinjected");
  } finally { await first.emitLifecycle("session_shutdown"); await restored.emitLifecycle("session_shutdown"); await peer.disconnect(); }
});

test("a persisted withdrawal survives reload even when custom history writes failed", async () => {
  const peer = await colleague("persisted-withdrawal-planner");
  let brokenHistory = false;
  const first = createExtensionHarness("persisted-withdrawal-worker", {
    sessionId: "persisted-withdrawal-worker-id", isIdle: () => false,
    appendEntryError: () => brokenHistory ? new Error("custom history unavailable") : undefined,
  });
  const restored = createExtensionHarness("persisted-withdrawal-worker", { sessionId: "persisted-withdrawal-worker-id", isIdle: () => false });
  try {
    const target = await start(first, peer, "persisted-withdrawal-worker");
    brokenHistory = true;
    const request = await peer.send(target.id, { text: "Prepare the release checklist", expectsReply: true });
    await until(() => first.persistedMessages.some((item) => item.customType === "parley_message"), "host-persisted request");
    await peer.cancelMessage(request.id);
    await until(() => first.persistedMessages.some((item) => item.customType === "parley_message_control"), "host-persisted withdrawal");
    restored.entries.push(...structuredClone(first.entries));
    restored.persistedMessages.push(...structuredClone(first.persistedMessages));
    await first.emitLifecycle("session_shutdown");
    await start(restored, peer, "persisted-withdrawal-worker");
    assert.ok(!text(await call(restored, { action: "pending" })).includes(request.id), "durable withdrawal does not resurrect as pending work");
    assert.match(text(await call(restored, { action: "read", messageId: request.id })), /Prepare the release checklist/);
    assert.match(text(await call(restored, { action: "read", messageId: request.id })), /Status: withdrawn/);
    assert.equal(restored.sentMessages.length, 0, "neither historical request nor withdrawal is injected as new work");
  } finally { await first.emitLifecycle("session_shutdown"); await restored.emitLifecycle("session_shutdown"); await peer.disconnect(); }
});

test("an interrupted blocking answer is recovered when the host never persisted its tool result", async () => {
  const peer = await colleague("interrupted-answer-planner");
  const first = createExtensionHarness("interrupted-answer-worker", { sessionId: "interrupted-answer-worker-id", isIdle: () => false });
  const restored = createExtensionHarness("interrupted-answer-worker", { sessionId: "interrupted-answer-worker-id", isIdle: () => false, persistMessages: false });
  try {
    await start(first, peer, "interrupted-answer-worker");
    const questionReceived = once(peer, "message", { signal: AbortSignal.timeout(8_000) }) as Promise<[SessionInfo, Message]>;
    const answerResult = call(first, { action: "ask", to: "interrupted-answer-planner", message: "Which backup should we use?" });
    const [asker, question] = await questionReceived;
    await peer.send(asker.id, { text: "Use the verified Sunday backup", replyTo: question.id, completesAsk: true });
    await answerResult;
    // Keep the durable received answer, but model a crash before its final tool-result/settlement persistence.
    restored.entries.push(...structuredClone(first.entries.filter((entry) => entry.type !== "parley_ask_settled")));
    await first.emitLifecycle("session_shutdown");
    await start(restored, peer, "interrupted-answer-worker");
    assert.ok(text(await call(restored, { action: "status" })).includes(question.id));
    const recovered = await modelContext(restored);
    assert.match(recovered.map((item) => item.content).join("\n"), /Use the verified Sunday backup/);
    assert.ok(!text(await call(restored, { action: "status" })).includes(question.id), "recovered answer settles after model-context delivery");
  } finally { await first.emitLifecycle("session_shutdown"); await restored.emitLifecycle("session_shutdown"); await peer.disconnect(); }
});

test("a reply to a notification sent by name carries its original topic", async () => {
  const peer = await colleague("notification-topic-planner");
  const harness = createExtensionHarness("notification-topic-worker", { sessionId: "notification-topic-worker-id", isIdle: () => false, persistMessages: false });
  try {
    await start(harness, peer, "notification-topic-worker");
    const noticeReceived = once(peer, "message", { signal: AbortSignal.timeout(8_000) }) as Promise<[SessionInfo, Message]>;
    await call(harness, { action: "send", to: "notification-topic-planner", message: "The migration contract is ready for review" });
    const [sender, notice] = await noticeReceived;
    await peer.send(sender.id, { text: "Thanks, I will review the index change", replyTo: notice.id, completesAsk: true });
    await until(() => harness.sentMessages.length > 0, "notification reply");
    const context = await modelContext(harness);
    assert.match(context.map((item) => item.content).join("\n"), /Reply to your message: "The migration contract is ready for review"/);
  } finally { await harness.emitLifecycle("session_shutdown"); await peer.disconnect(); }
});
