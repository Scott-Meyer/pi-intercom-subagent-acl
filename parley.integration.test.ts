import test from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { EventEmitter, once } from "node:events";
import { spawn, type ChildProcess } from "node:child_process";
import { ReplyTracker } from "./reply-tracker.ts";
import type { BrokerMessage, Message, SessionInfo } from "./types.ts";
import {
  PARLEY_EXTENSION_REGISTER_EVENT,
  PARLEY_OUTBOX_REQUEST_EVENT,
  PARLEY_OUTBOX_RESULT_EVENT,
  type ParleyExtensionChannel,
  type ParleyOutboxResultV1,
} from "./extension-api.ts";

const repoDir = process.cwd();
const childEnvKeys = [
  "PI_SUBAGENT_ORCHESTRATOR_TARGET",
  "PI_SUBAGENT_ORCHESTRATOR_SESSION_ID",
  "PI_PARLEY_SESSION_ID",
  "PI_SUBAGENT_RUN_ID",
  "PI_SUBAGENT_CHILD_AGENT",
  "PI_SUBAGENT_CHILD_INDEX",
  "PI_SUBAGENT_PARLEY_SESSION_NAME",
  "PI_SUBAGENT_SUPERVISOR_CHANNEL_DIR",
] as const;
const sharedHomeDir = mkdtempSync(path.join(tmpdir(), "pi-parley-home-"));
const previousHome = process.env.HOME;
const previousUserProfile = process.env.USERPROFILE;
process.env.HOME = sharedHomeDir;
process.env.USERPROFILE = sharedHomeDir;
// Inherited overrides leak past the HOME pin: getAgentDirPath() gives
// PI_CODING_AGENT_DIR precedence, and inherited PI_PARLEY_* / PI_SUBAGENT_*
// vars would change routing and ACL behavior inside these tests.
delete process.env.PI_CODING_AGENT_DIR;
for (const key of Object.keys(process.env)) {
  if (key.startsWith("PI_SUBAGENT_") || key.startsWith("PI_PARLEY_")) delete process.env[key];
}
const { ParleyClient } = await import("./broker/client.ts");
const { getTsxCliPath } = await import("./broker/spawn.ts");
const { getAskTimeoutMs, getConfigPath } = await import("./config.ts");
process.on("exit", () => {
  process.env.HOME = previousHome;
  process.env.USERPROFILE = previousUserProfile;
  rmSync(sharedHomeDir, { recursive: true, force: true });
});

async function withParleyConfig<T>(config: Record<string, unknown> | string, fn: () => T | Promise<T>): Promise<T> {
  const configPath = getConfigPath();
  const previous = existsSync(configPath) ? readFileSync(configPath, "utf-8") : undefined;
  mkdirSync(path.dirname(configPath), { recursive: true });
  writeFileSync(configPath, typeof config === "string" ? config : JSON.stringify(config));
  try {
    return await fn();
  } finally {
    if (previous === undefined) rmSync(configPath, { force: true });
    else writeFileSync(configPath, previous);
  }
}

async function withParleyScope<T>(scopeId: string | undefined, fn: () => T | Promise<T>): Promise<T> {
  const previous = process.env.PI_PARLEY_SCOPE_ID;
  if (scopeId === undefined) delete process.env.PI_PARLEY_SCOPE_ID;
  else process.env.PI_PARLEY_SCOPE_ID = scopeId;
  try {
    return await fn();
  } finally {
    if (previous === undefined) delete process.env.PI_PARLEY_SCOPE_ID;
    else process.env.PI_PARLEY_SCOPE_ID = previous;
  }
}

async function waitForBrokerReady(broker: ChildProcess): Promise<void> {
  const stdout = broker.stdout;
  if (!stdout) throw new Error("Broker stdout is unavailable");

  const ready = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error("Broker startup timed out"));
    }, 10000);
    const onStdout = (chunk: Buffer) => {
      if (chunk.toString().includes("Parley broker started")) {
        cleanup();
        resolve();
      }
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(new Error(`Broker exited before startup (code=${code}, signal=${signal})`));
    };
    const cleanup = () => {
      clearTimeout(timeout);
      stdout.off("data", onStdout);
      broker.off("exit", onExit);
    };

    stdout.on("data", onStdout);
    broker.once("exit", onExit);
  });

  await ready;
}

async function withChildOrchestratorEnv<T>(metadata: {
  orchestratorTarget?: string;
  orchestratorSessionId?: string;
  inheritedParleySessionId?: string;
  runId?: string;
  agent?: string;
  index?: string;
  sessionName?: string;
  supervisorChannelDir?: string;
}, fn: () => T | Promise<T>): Promise<T> {
  const previous = new Map<string, string | undefined>();
  for (const key of childEnvKeys) {
    previous.set(key, process.env[key]);
    delete process.env[key];
  }
  if (metadata.orchestratorTarget !== undefined) process.env.PI_SUBAGENT_ORCHESTRATOR_TARGET = metadata.orchestratorTarget;
  if (metadata.orchestratorSessionId !== undefined) process.env.PI_SUBAGENT_ORCHESTRATOR_SESSION_ID = metadata.orchestratorSessionId;
  if (metadata.inheritedParleySessionId !== undefined) process.env.PI_PARLEY_SESSION_ID = metadata.inheritedParleySessionId;
  if (metadata.runId !== undefined) process.env.PI_SUBAGENT_RUN_ID = metadata.runId;
  if (metadata.agent !== undefined) process.env.PI_SUBAGENT_CHILD_AGENT = metadata.agent;
  if (metadata.index !== undefined) process.env.PI_SUBAGENT_CHILD_INDEX = metadata.index;
  if (metadata.sessionName !== undefined) process.env.PI_SUBAGENT_PARLEY_SESSION_NAME = metadata.sessionName;
  if (metadata.supervisorChannelDir !== undefined) process.env.PI_SUBAGENT_SUPERVISOR_CHANNEL_DIR = metadata.supervisorChannelDir;
  try {
    return await fn();
  } finally {
    for (const key of childEnvKeys) {
      const value = previous.get(key);
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

import { createExtensionHarness, type CapturedToolResult, type CapturedTool, type RenderToolResult, type RenderedComponent, type RenderTheme } from "./test/extension-harness.ts";

const renderTheme: RenderTheme = {
  fg: (_name, text) => text,
  bold: (text) => text,
};

function renderToText(component: RenderedComponent): string {
  return component.render(120).map((line) => line.trimEnd()).join("\n");
}

async function connectRawRegistered(sessionId: string, name: string, sessionOverrides: Record<string, unknown> = {}) {
  const net = await import("node:net");
  const { getBrokerSocketPath } = await import("./broker/paths.ts");
  const { createMessageReader, writeMessage } = await import("./broker/framing.ts");
  const socket = net.connect(getBrokerSocketPath());
  await once(socket, "connect");
  const registered = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Raw register timed out")), 2000);
    const reader = createMessageReader((msg) => {
      if (typeof msg === "object" && msg !== null && "type" in msg && msg.type === "registered") {
        clearTimeout(timeout);
        socket.off("data", reader);
        resolve();
      }
    }, reject);
    socket.on("data", reader);
  });
  writeMessage(socket, {
    type: "register",
    sessionId,
    session: {
      name,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
      ...sessionOverrides,
    },
  });
  await registered;
  return { socket, writeMessage };
}

test("opt-in TCP broker requires endpoint state for health and registration", { concurrency: false }, async () => {
  const net = await import("node:net");
  const { readFileSync } = await import("node:fs");
  const { createMessageReader, writeMessage } = await import("./broker/framing.ts");
  const agentDir = mkdtempSync(path.join(tmpdir(), "pi-parley-tcp-agent-"));
  const broker = spawn(process.execPath, ["--import", "tsx", path.join(repoDir, "broker", "broker.ts")], {
    cwd: repoDir,
    env: {
      ...process.env,
      HOME: agentDir,
      USERPROFILE: agentDir,
      PI_CODING_AGENT_DIR: agentDir,
      PI_PARLEY_TRANSPORT: "tcp",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  const exchange = async (message: unknown, waitForResponse: boolean): Promise<unknown[]> => {
    const socket = net.connect({ host, port });
    const messages: unknown[] = [];
    return await new Promise((resolve, reject) => {
      let settled = false;
      const finish = () => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeout);
        socket.off("data", reader);
        socket.off("close", finish);
        socket.off("error", onSocketError);
        socket.destroy();
        resolve(messages);
      };
      const onSocketError = () => finish();
      const reader = createMessageReader((received) => {
        messages.push(received);
        if (waitForResponse) {
          finish();
        }
      }, reject);
      const timeout = setTimeout(finish, 500);
      socket.once("connect", () => writeMessage(socket, message));
      socket.on("data", reader);
      socket.once("close", finish);
      socket.once("error", onSocketError);
    });
  };

  let host = "";
  let port = 0;
  let stateId = "";
  try {
    await waitForBrokerReady(broker);
    const endpoint: unknown = JSON.parse(readFileSync(path.join(agentDir, "parley", "broker.port.json"), "utf-8"));
    if (typeof endpoint !== "object" || endpoint === null || Array.isArray(endpoint)) {
      throw new Error("Invalid TCP endpoint fixture");
    }
    const endpointRecord = endpoint as Record<string, unknown>;
    if (endpointRecord.host !== "127.0.0.1" || typeof endpointRecord.port !== "number" || typeof endpointRecord.stateId !== "string") {
      throw new Error(`Invalid TCP endpoint fixture: ${JSON.stringify(endpointRecord)}`);
    }
    host = endpointRecord.host;
    port = endpointRecord.port;
    stateId = endpointRecord.stateId;

    assert.deepEqual(await exchange({ type: "health", requestId: "unauthorized-health" }, false), []);
    assert.deepEqual(await exchange({
      type: "register",
      sessionId: "unauthorized-tcp-client",
      session: {
        name: "unauthorized",
        cwd: repoDir,
        model: "test-model",
        pid: process.pid,
        startedAt: Date.now(),
        lastActivity: Date.now(),
      },
    }, false), []);

    const healthMessages = await exchange({ type: "health", requestId: "authorized-health", stateId }, true);
    assert.equal(healthMessages.length, 1);
    const healthy = healthMessages[0] as { type: string; requestId: string; protocol: string; version: number; broker: { pid: number; instanceId: string; sourceId: string } };
    assert.equal(healthy.type, "health_ok");
    assert.equal(healthy.requestId, "authorized-health");
    assert.equal(healthy.protocol, "pi-parley");
    assert.equal(healthy.version, 1);
    assert.equal(healthy.broker.pid, broker.pid);
    assert.match(healthy.broker.instanceId, /^[0-9a-f-]{36}$/);
    assert.match(healthy.broker.sourceId, /^[0-9a-f]{64}$/);
    assert.equal(JSON.stringify(healthy).includes(stateId), false, "health must not publish endpoint credentials");

    const registerMessages = await exchange({
      type: "register",
      sessionId: "authorized-tcp-client",
      stateId,
      session: {
        name: "authorized",
        cwd: repoDir,
        model: "test-model",
        pid: process.pid,
        startedAt: Date.now(),
        lastActivity: Date.now(),
      },
    }, true);
    assert.equal(registerMessages.length, 1);
    const registered = registerMessages[0] as { type: string; sessionId: string; features: string[]; session: SessionInfo };
    assert.equal(registered.type, "registered");
    assert.equal(registered.sessionId, "authorized-tcp-client");
    for (const feature of ["extension-bus-v1", "exact-send-v1", "compaction-awareness-v1", "session-profile-v1", "conversation-contract-v1"]) {
      assert.ok(registered.features.includes(feature), `the TCP endpoint must advertise ${feature}`);
    }
    assert.equal(registered.session.id, "authorized-tcp-client");
    assert.equal(registered.session.name, "authorized");
  } finally {
    if (broker.exitCode === null && broker.signalCode === null) {
      broker.kill("SIGTERM");
      await once(broker, "exit").catch(() => undefined);
    }
    rmSync(agentDir, { recursive: true, force: true });
  }
});

async function setupClients() {
  const broker = spawn(process.execPath, [getTsxCliPath(), path.join(repoDir, "broker", "broker.ts")], {
    cwd: repoDir,
    env: { ...process.env, HOME: sharedHomeDir, USERPROFILE: sharedHomeDir },
    stdio: ["ignore", "pipe", "pipe"],
  });

  try {
    await waitForBrokerReady(broker);
    const planner = new ParleyClient();
    const orchestrator = new ParleyClient();

    await planner.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    await orchestrator.connect({
      name: "orchestrator",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    return {
      planner,
      orchestrator,
      cleanup: async () => {
        await planner.disconnect().catch(() => undefined);
        await orchestrator.disconnect().catch(() => undefined);
        broker.kill("SIGTERM");
        await once(broker, "exit").catch(() => undefined);
      },
    };
  } catch (error) {
    broker.kill("SIGTERM");
    await once(broker, "exit").catch(() => undefined);
    throw error;
  }
}

// Simulated model decisions use only text the host passes to the model, never details.
function modelText(result: Pick<CapturedToolResult, "content">): string {
  return result.content.filter((part) => part.type === "text").map((part) => part.text).join("\n");
}

function visibleMessageId(text: string): string {
  const id = text.match(/^Message(?: ID)?: (\S+)/m)?.[1];
  assert.ok(id, `Expected an actionable message ID in model-visible text: ${text}`);
  return id;
}

function pendingMessageId(text: string, question: string): string {
  const line = text.split("\n").find((line) => line.includes(question));
  const id = line?.match(/, message (\S+) —/)?.[1];
  assert.ok(id, `Expected the full pending ID beside ${JSON.stringify(question)}: ${text}`);
  return id;
}

async function waitForVisibleText(harness: ReturnType<typeof createExtensionHarness>, text: string): Promise<string> {
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) {
    const message = harness.sentMessages.find((entry) => entry.message.content?.includes(text));
    if (message) return message.message.content!;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail(`Expected model-visible message containing ${JSON.stringify(text)}`);
}

async function connectClientWithScope(client: InstanceType<typeof ParleyClient>, scopeId: string | undefined, sessionId: string, name: string): Promise<void> {
  await withParleyScope(scopeId, () => client.connect({
    name,
    cwd: repoDir,
    model: "test-model",
    pid: process.pid,
    startedAt: Date.now(),
    lastActivity: Date.now(),
  }, sessionId));
}

function waitForReply(client: InstanceType<typeof ParleyClient>, replyTo: string, timeoutMs = 5000): Promise<{ from: SessionInfo; message: Message; }> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      client.off("message", handler);
      reject(new Error(`Timed out waiting for reply to ${replyTo}`));
    }, timeoutMs);
    const handler = (from: SessionInfo, message: Message) => {
      if (message.replyTo !== replyTo) {
        return;
      }
      clearTimeout(timeout);
      client.off("message", handler);
      resolve({ from, message });
    };
    client.on("message", handler);
  });
}

function waitForOutboxResults(results: ParleyOutboxResultV1[], count: number, timeoutMs = 3000): Promise<ParleyOutboxResultV1[]> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;
    const check = () => {
      if (results.length >= count) {
        resolve(results.slice(0, count));
        return;
      }
      if (Date.now() >= deadline) {
        reject(new Error(`Timed out waiting for ${count} outbox result(s); saw ${JSON.stringify(results)}`));
        return;
      }
      setTimeout(check, 10);
    };
    check();
  });
}

async function waitForReplyMessage(messages: Message[], messageId: string, timeoutMs = 3000): Promise<Message> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const message = messages.find((candidate) => candidate.id === messageId);
    if (message) return message;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Timed out waiting for message ${messageId}`);
}

function pendingAskRecordPath(messageId: string): string {
  return path.join(sharedHomeDir, ".pi", "agent", "parley", "pending-asks", `${encodeURIComponent(messageId)}.json`);
}

function readPendingAskRecord(messageId: string): Record<string, unknown> {
  return JSON.parse(readFileSync(pendingAskRecordPath(messageId), "utf-8")) as Record<string, unknown>;
}

async function waitForPendingAskRecordRemoved(messageId: string): Promise<void> {
  const filePath = pendingAskRecordPath(messageId);
  const deadline = Date.now() + 1000;
  while (Date.now() < deadline) {
    if (!existsSync(filePath)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.equal(existsSync(filePath), false);
}

async function waitForSessionByName(client: InstanceType<typeof ParleyClient>, name: string): Promise<SessionInfo> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const session = (await client.listSessions()).find((candidate) => candidate.name === name);
    if (session) {
      return session;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const sessions = await client.listSessions();
  throw new Error(`Timed out waiting for ${name}; saw ${JSON.stringify(sessions.map((session) => session.name))}`);
}

async function waitForSessionDescription(
  client: InstanceType<typeof ParleyClient>,
  name: string,
  description: string | undefined,
): Promise<SessionInfo> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const session = (await client.listSessions()).find((candidate) => candidate.name === name);
    if (session && session.description === description) return session;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const sessions = await client.listSessions();
  throw new Error(`Timed out waiting for ${name} description ${String(description)}; saw ${JSON.stringify(sessions.map((session) => ({ name: session.name, description: session.description })))}`);
}

async function waitForSessionStatus(client: InstanceType<typeof ParleyClient>, name: string, status: string): Promise<SessionInfo> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const session = (await client.listSessions()).find((candidate) => candidate.name === name);
    if (session?.status === status) {
      return session;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const sessions = await client.listSessions();
  throw new Error(`Timed out waiting for ${name} status ${status}; saw ${JSON.stringify(sessions.map((session) => ({ name: session.name, status: session.status })))}`);
}

async function waitForSessionModel(client: InstanceType<typeof ParleyClient>, name: string, model: string): Promise<SessionInfo> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const session = (await client.listSessions()).find((candidate) => candidate.name === name);
    if (session?.model === model) {
      return session;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const sessions = await client.listSessions();
  throw new Error(`Timed out waiting for ${name} model ${model}; saw ${JSON.stringify(sessions.map((session) => ({ name: session.name, model: session.model })))}`);
}

async function withConfirmSendEnabled<T>(fn: () => T | Promise<T>): Promise<T> {
  const { getParleyDirPath } = await import("./broker/paths.ts");
  const { getConfigPath } = await import("./config.ts");
  const { mkdirSync, writeFileSync, existsSync, rmSync: removeSync } = await import("node:fs");
  const parleyDir = getParleyDirPath();
  mkdirSync(parleyDir, { recursive: true });
  const configPath = getConfigPath(parleyDir);
  const existed = existsSync(configPath);
  writeFileSync(configPath, JSON.stringify({ confirmSend: true }), "utf-8");
  try {
    return await fn();
  } finally {
    if (existed) {
      writeFileSync(configPath, JSON.stringify({ confirmSend: false }), "utf-8");
    } else {
      removeSync(configPath, { force: true });
    }
  }
}

async function withAskTimeoutMs<T>(timeoutMs: number, fn: () => T | Promise<T>): Promise<T> {
  const previous = process.env.PI_PARLEY_ASK_TIMEOUT_MS;
  process.env.PI_PARLEY_ASK_TIMEOUT_MS = String(timeoutMs);
  try {
    return await fn();
  } finally {
    if (previous === undefined) delete process.env.PI_PARLEY_ASK_TIMEOUT_MS;
    else process.env.PI_PARLEY_ASK_TIMEOUT_MS = previous;
  }
}

async function waitForSessionId(client: InstanceType<typeof ParleyClient>, sessionId: string): Promise<SessionInfo> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const session = (await client.listSessions()).find((candidate) => candidate.id === sessionId);
    if (session) {
      return session;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const sessions = await client.listSessions();
  throw new Error(`Timed out waiting for ${sessionId}; saw ${JSON.stringify(sessions.map((session) => session.id))}`);
}

async function waitForNoSessionId(client: InstanceType<typeof ParleyClient>, sessionId: string): Promise<void> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    if (!(await client.listSessions()).some((candidate) => candidate.id === sessionId)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`Timed out waiting for ${sessionId} to leave`);
}

test("broker accepts caller supplied stable IDs across reconnect", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const worker = new ParleyClient();

  try {
    await worker.connect({
      name: "stable-worker",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, "stable-session-id");
    assert.equal(worker.sessionId, "stable-session-id");
    await waitForSessionId(planner, "stable-session-id");
    await worker.disconnect();
    await waitForNoSessionId(planner, "stable-session-id");

    const reconnected = new ParleyClient();
    await reconnected.connect({
      name: "stable-worker",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, "stable-session-id");
    assert.equal(reconnected.sessionId, "stable-session-id");
    await waitForSessionId(planner, "stable-session-id");
    await reconnected.disconnect();
  } finally {
    await worker.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker scopes discovery, routing, mailbox, and presence", { concurrency: false }, async () => {
  const broker = spawn(process.execPath, [getTsxCliPath(), path.join(repoDir, "broker", "broker.ts")], {
    cwd: repoDir,
    env: { ...process.env, HOME: sharedHomeDir, USERPROFILE: sharedHomeDir },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const clients: Array<InstanceType<typeof ParleyClient>> = [];
  try {
    await waitForBrokerReady(broker);
    const unscopedA = new ParleyClient();
    const unscopedB = new ParleyClient();
    const alphaSender = new ParleyClient();
    const alphaTarget = new ParleyClient();
    const betaTarget = new ParleyClient();
    clients.push(unscopedA, unscopedB, alphaSender, alphaTarget, betaTarget);

    const alphaEvents: BrokerMessage[] = [];
    const alphaMessages: Message[] = [];
    const betaMessages: Message[] = [];
    alphaSender.onBrokerMessage((message) => alphaEvents.push(message));
    alphaTarget.on("message", (_from: SessionInfo, message: Message) => alphaMessages.push(message));
    betaTarget.on("message", (_from: SessionInfo, message: Message) => betaMessages.push(message));

    await connectClientWithScope(unscopedA, undefined, "unscoped-a", "unscoped-a");
    await connectClientWithScope(unscopedB, undefined, "unscoped-b", "unscoped-b");
    await connectClientWithScope(alphaSender, "  alpha-scope  ", "alpha-sender", "alpha-sender");
    await connectClientWithScope(betaTarget, "beta-scope", "shared-target-id", "shared-target");
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(alphaEvents.some((message) => message.type === "session_joined" && message.session.id === "shared-target-id"), false);

    await connectClientWithScope(alphaTarget, "alpha-scope", "shared-target-id", "shared-target");
    await waitForSessionByName(alphaSender, "shared-target");
    assert.equal((await unscopedA.listSessions()).some((session) => session.id === "shared-target-id"), false);
    assert.equal((await alphaSender.listSessions()).some((session) => session.id === "unscoped-b"), false);
    assert.equal((await unscopedA.listSessions()).some((session) => session.id === "unscoped-b"), true);
    assert.equal((await alphaSender.listSessions()).filter((session) => session.cwd === repoDir && session.name === "shared-target").length, 1);

    betaTarget.updatePresence({ status: "beta-only" });
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(alphaEvents.some((message) => message.type === "presence_update" && message.session.status === "beta-only"), false);
    alphaTarget.updatePresence({ status: "alpha-visible" });
    await waitForSessionStatus(alphaSender, "shared-target", "alpha-visible");

    assert.equal((await unscopedA.send("shared-target-id", { text: "full id must not cross" })).delivered, false);
    assert.equal((await alphaSender.send("unscoped-b", { text: "unscoped id must not cross" })).delivered, false);
    assert.equal((await alphaSender.send("shared-target", { messageId: "alpha-name-scope", text: "name stays in scope" })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(alphaMessages.some((message) => message.id === "alpha-name-scope"), true);
    assert.equal(betaMessages.some((message) => message.id === "alpha-name-scope"), false);

    await alphaTarget.disconnect();
    const crossScopeDisconnected = await unscopedA.send("shared-target-id", {
      messageId: "unscoped-to-scoped-disconnected",
      text: "must not queue across scope",
    });
    assert.equal(crossScopeDisconnected.delivered, false);
    assert.match(crossScopeDisconnected.reason ?? "", /Session not found/);

    const queued = await alphaSender.send("shared-target-id", {
      messageId: "alpha-scoped-mailbox",
      text: "queued only for alpha",
    });
    assert.equal(queued.delivery, "queued");
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(betaMessages.some((message) => message.id === "alpha-scoped-mailbox"), false);

    const alphaReplacement = new ParleyClient();
    clients.push(alphaReplacement);
    const recovered: Message[] = [];
    alphaReplacement.on("message", (_from: SessionInfo, message: Message) => recovered.push(message));
    await connectClientWithScope(alphaReplacement, "alpha-scope", "shared-target-id", "shared-target");
    await waitForReplyMessage(recovered, "alpha-scoped-mailbox");
  } finally {
    await Promise.all(clients.map((client) => client.disconnect().catch(() => undefined)));
    if (broker.exitCode === null && broker.signalCode === null) broker.kill("SIGTERM");
    await once(broker, "exit").catch(() => undefined);
  }
});

test("broker rotates endpoint epochs and replays same message ids without duplicate delivery", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new ParleyClient();
  const received: Message[] = [];
  orchestrator.on("message", (_from: SessionInfo, message: Message) => received.push(message));

  try {
    const firstEndpoint = await waitForSessionByName(planner, "orchestrator");
    assert.equal(typeof firstEndpoint.endpointEpoch, "string");

    const messageId = "endpoint-epoch-replay";
    const first = await planner.send(orchestrator.sessionId!, { text: "deliver once", messageId });
    const replay = await planner.send(orchestrator.sessionId!, { text: "deliver once", messageId });
    assert.deepEqual([first.delivery, replay.delivery], ["socket_delivered", "socket_delivered"]);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(received.filter((message) => message.id === messageId).length, 1);

    await replacement.connect({
      name: "orchestrator-replacement",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, orchestrator.sessionId!);
    const replacedEndpoint = await waitForSessionId(planner, orchestrator.sessionId!);
    assert.notEqual(replacedEndpoint.endpointEpoch, firstEndpoint.endpointEpoch);
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("delivery records keep colon-containing sender and message IDs distinct", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const first = new ParleyClient();
  const second = new ParleyClient();
  const received: Message[] = [];
  orchestrator.on("message", (_from: SessionInfo, message: Message) => received.push(message));

  try {
    await first.connect({ name: "record-key-first", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "a:b");
    await second.connect({ name: "record-key-second", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "a");

    assert.equal((await first.send(orchestrator.sessionId!, { messageId: "c", text: "same fingerprint" })).delivered, true);
    assert.equal((await second.send(orchestrator.sessionId!, { messageId: "b:c", text: "same fingerprint" })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(received.map((message) => message.id).sort(), ["b:c", "c"]);
  } finally {
    await first.disconnect().catch(() => undefined);
    await second.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("client re-resolves a rebound exact target once with the same message id", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new ParleyClient();
  const replacementReceived = once(replacement, "message") as Promise<[SessionInfo, Message]>;
  const listSessions = planner.listSessions.bind(planner);
  let listCalls = 0;

  try {
    (planner as unknown as { listSessions: () => Promise<SessionInfo[]> }).listSessions = async () => {
      const sessions = await listSessions();
      listCalls += 1;
      if (listCalls === 1) {
        await replacement.connect({
          name: "orchestrator-replacement",
          cwd: repoDir,
          model: "test-model",
          pid: process.pid,
          startedAt: Date.now(),
          lastActivity: Date.now(),
        }, orchestrator.sessionId!);
      }
      return sessions;
    };

    const result = await planner.send(orchestrator.sessionId!, { text: "retry after rebound", messageId: "endpoint-rebound-retry" });
    assert.equal(result.delivered, true);
    assert.equal(result.delivery, "socket_delivered");
    const [, message] = await replacementReceived;
    assert.equal(message.id, "endpoint-rebound-retry");
    assert.equal(listCalls, 2);
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker rejects malformed exact target fields instead of falling back to name routing", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const raw = await connectRawRegistered("malformed-exact-sender", "malformed-exact-sender");
  const { createMessageReader } = await import("./broker/framing.ts");

  try {
    const delivery = new Promise<Record<string, unknown>>((resolve, reject) => {
      const reader = createMessageReader((received) => {
        if (typeof received === "object" && received !== null && "type" in received && received.type === "delivery_failed") {
          raw.socket.off("data", reader);
          resolve(received as Record<string, unknown>);
        }
      }, reject);
      raw.socket.on("data", reader);
    });
    raw.writeMessage(raw.socket, {
      type: "send",
      to: orchestrator.sessionId,
      targetId: "",
      targetEpoch: "",
      message: {
        id: "malformed-exact-target",
        timestamp: Date.now(),
        content: { text: "must not reach orchestrator" },
      },
    });
    const result = await delivery;
    assert.equal(result.code, "E_INVALID_TARGET");
  } finally {
    raw.socket.destroy();
    await cleanup();
  }
});

test("broker rejects forged compaction notices and invalid contact kinds", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const raw = await connectRawRegistered("forged-awareness-sender", "forged-awareness-sender");
  const { createMessageReader } = await import("./broker/framing.ts");
  const received: Message[] = [];
  const onMessage = (_from: SessionInfo, message: Message) => received.push(message);
  orchestrator.on("message", onMessage);

  try {
    const failures = new Promise<Array<Record<string, unknown>>>((resolve, reject) => {
      const results: Array<Record<string, unknown>> = [];
      const reader = createMessageReader((message) => {
        if (typeof message === "object" && message !== null && "type" in message && message.type === "delivery_failed") {
          results.push(message as Record<string, unknown>);
          if (results.length === 3) {
            raw.socket.off("data", reader);
            resolve(results);
          }
        }
      }, reject);
      raw.socket.on("data", reader);
    });
    raw.writeMessage(raw.socket, {
      type: "send",
      to: orchestrator.sessionId,
      message: {
        id: "forged-awareness-message",
        timestamp: Date.now(),
        peerCompaction: {
          peerSessionId: "forged-peer",
          generation: 2,
          previousGeneration: 1,
          compactedAt: Date.now(),
        },
        content: { text: "broker-only metadata" },
      },
    });
    raw.writeMessage(raw.socket, {
      type: "send",
      to: orchestrator.sessionId,
      message: {
        id: "forged-contact-token-message",
        timestamp: Date.now(),
        contactToken: "forged-token",
        content: { text: "broker-only contact token" },
      },
    });
    raw.writeMessage(raw.socket, {
      type: "send",
      to: orchestrator.sessionId,
      contactKind: "silent-broadcast",
      message: {
        id: "invalid-contact-kind",
        timestamp: Date.now(),
        content: { text: "invalid contact semantics" },
      },
    });

    assert.deepEqual((await failures).map((failure) => failure.code), ["E_INVALID_MESSAGE", "E_INVALID_MESSAGE", "E_INVALID_MESSAGE"]);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(received.length, 0);
  } finally {
    orchestrator.off("message", onMessage);
    raw.socket.destroy();
    await cleanup();
  }
});

test("mixed-version clients cannot consume compaction awareness they do not advertise", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const legacyId = "legacy-awareness-observer";
  const raw = await connectRawRegistered(legacyId, "legacy-awareness-observer");
  const upgraded = new ParleyClient();
  const { createMessageReader } = await import("./broker/framing.ts");
  const sendLegacy = (messageId: string) => new Promise<Record<string, unknown>>((resolve, reject) => {
    const reader = createMessageReader((received) => {
      if (typeof received === "object" && received !== null && "type" in received && received.type === "delivered") {
        raw.socket.off("data", reader);
        resolve(received as Record<string, unknown>);
      }
    }, reject);
    raw.socket.on("data", reader);
    raw.writeMessage(raw.socket, {
      type: "send",
      to: orchestrator.sessionId,
      contactKind: "direct",
      message: { id: messageId, timestamp: Date.now(), content: { text: messageId } },
    });
  });

  try {
    const legacyBaseline = await sendLegacy("legacy-baseline");
    assert.equal(legacyBaseline.contactToken, undefined);
    await orchestrator.reportCompactionCompleted();
    const legacyAfterCompaction = await sendLegacy("legacy-after-compaction");
    assert.equal(legacyAfterCompaction.peerCompaction, undefined);
    assert.equal(legacyAfterCompaction.contactToken, undefined);

    await upgraded.connect({
      name: "upgraded-awareness-observer",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, legacyId);
    const upgradedBaseline = await upgraded.send(orchestrator.sessionId!, { text: "capable baseline", contactKind: "direct" });
    assert.equal(upgradedBaseline.peerCompaction, undefined, "first capable contact establishes the baseline without a historical claim");
    const nextCompaction = await orchestrator.reportCompactionCompleted();
    const noticed = await upgraded.send(orchestrator.sessionId!, { text: "capable notice", contactKind: "direct" });
    assert.equal(noticed.peerCompaction?.generation, nextCompaction.generation);
    upgraded.acknowledgeSendContact(noticed);
  } finally {
    raw.socket.destroy();
    await upgraded.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker rejects changed message content after a rebound exact-target failure", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const raw = await connectRawRegistered("rebound-reuse-sender", "rebound-reuse-sender");
  const replacement = new ParleyClient();
  const { createMessageReader } = await import("./broker/framing.ts");

  try {
    const targetId = orchestrator.sessionId!;
    const oldTarget = await waitForSessionId(planner, targetId);
    await replacement.connect({
      name: "rebound-reuse-replacement",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, targetId);
    const receiveDelivery = () => new Promise<Record<string, unknown>>((resolve, reject) => {
      const reader = createMessageReader((received) => {
        if (typeof received === "object" && received !== null && "type" in received && (received.type === "delivered" || received.type === "delivery_failed")) {
          raw.socket.off("data", reader);
          resolve(received as Record<string, unknown>);
        }
      }, reject);
      raw.socket.on("data", reader);
    });
    const send = (text: string) => raw.writeMessage(raw.socket, {
      type: "send",
      to: targetId,
      targetId,
      targetEpoch: oldTarget.endpointEpoch,
      message: { id: "rebound-id-reuse", timestamp: Date.now(), content: { text } },
    });

    const firstDelivery = receiveDelivery();
    send("first content");
    assert.equal((await firstDelivery).code, "E_TARGET_REBOUND");
    const secondDelivery = receiveDelivery();
    send("changed content");
    assert.equal((await secondDelivery).code, "E_MESSAGE_ID_REUSE");
  } finally {
    raw.socket.destroy();
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker propagates a session's tmux pane id into the roster", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const worker = new ParleyClient();

  try {
    await worker.connect({
      name: "tmux-worker",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
      tmuxPane: "%212",
    });
    const session = await waitForSessionByName(planner, "tmux-worker");
    assert.equal(session.tmuxPane, "%212");
  } finally {
    await worker.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker omits tmux pane id for sessions outside tmux", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const worker = new ParleyClient();

  try {
    await worker.connect({
      name: "paneless-worker",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    const session = await waitForSessionByName(planner, "paneless-worker");
    assert.equal(session.tmuxPane, undefined);
  } finally {
    await worker.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker owns local trust metadata instead of trusting registration payloads", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const raw = await connectRawRegistered("trust-metadata-worker-id", "trust-metadata-worker", {
    peerUid: 0,
    trustedLocal: false,
  });

  try {
    const session = await waitForSessionId(planner, "trust-metadata-worker-id");
    assert.equal(session.trustedLocal, process.platform !== "win32");
    assert.equal(session.peerUid, undefined);
  } finally {
    raw.socket.destroy();
    await cleanup();
  }
});

test("broker rejects unknown replyTo values instead of delivering forged replies", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();

  try {
    const result = await planner.send(orchestrator.sessionId!, {
      text: "This is not a real reply.",
      replyTo: "not-a-pending-ask",
    });
    assert.equal(result.delivered, false);
    assert.match(result.reason ?? "", /previous message/i);
  } finally {
    await cleanup();
  }
});

test("broker disconnects a connection that exceeds the local rate limit", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const raw = await connectRawRegistered("rate-limit-worker-id", "rate-limit-worker");

  try {
    raw.socket.on("error", () => undefined);
    // Linux may reset a flooded socket; the promise is closure, not a clean EOF.
    const closed = new Promise<void>((resolve) => raw.socket.once("close", () => resolve()));
    for (let i = 0; i < 300; i += 1) {
      raw.writeMessage(raw.socket, { type: "list", requestId: `flood-${i}` });
    }
    await closed;
    assert.equal(raw.socket.destroyed, true);

    const unsafePresence = await connectRawRegistered("unsafe-presence-id", "safe-presence-name");
    unsafePresence.socket.on("error", () => undefined);
    const unsafeClosed = new Promise<void>((resolve) => unsafePresence.socket.once("close", () => resolve()));
    unsafePresence.writeMessage(unsafePresence.socket, { type: "presence", name: "unsafe\u001b[2J-name" });
    await unsafeClosed;
    assert.equal(unsafePresence.socket.destroyed, true);
  } finally {
    raw.socket.destroy();
    await cleanup();
  }
});

test("broker idle raw connections cannot block legitimate registration", { concurrency: false }, async () => {
  const net = await import("node:net");
  const { getBrokerSocketPath } = await import("./broker/paths.ts");
  const { cleanup } = await setupClients();
  const sockets: ReturnType<typeof net.connect>[] = [];
  const legitimate = new ParleyClient();

  try {
    for (let i = 0; i < 140; i += 1) {
      const socket = net.connect(getBrokerSocketPath());
      socket.on("error", () => undefined);
      sockets.push(socket);
      await Promise.race([
        once(socket, "connect").catch(() => undefined),
        once(socket, "close").catch(() => undefined),
        new Promise((resolve) => setTimeout(resolve, 200)),
      ]);
    }

    await legitimate.connect({
      name: "legitimate-after-idle-flood",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    assert.equal(legitimate.isConnected(), true);
  } finally {
    for (const socket of sockets) {
      socket.destroy();
    }
    await legitimate.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker times out sockets that unregister and go idle", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const raws: Array<Awaited<ReturnType<typeof connectRawRegistered>>> = [];
  const legitimate = new ParleyClient();

  try {
    for (let i = 0; i < 40; i += 1) {
      const raw = await connectRawRegistered(`unregister-idle-${i}`, `unregister-idle-${i}`);
      raw.socket.on("error", () => undefined);
      raw.writeMessage(raw.socket, { type: "unregister" });
      raws.push(raw);
    }
    await new Promise((resolve) => setTimeout(resolve, 1200));

    await legitimate.connect({
      name: "legitimate-after-unregister-idle-flood",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    assert.equal(legitimate.isConnected(), true);
  } finally {
    for (const raw of raws) {
      raw.socket.destroy();
    }
    await legitimate.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("unnamed sessions use a neutral collision-resistant runtime alias", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const firstSessionId = "019fe418-248e-7447-9379-fdce6e91dcba";
  const secondSessionId = "019fe418-248e-7abc-8123-111111111111";
  const firstHarness = createExtensionHarness("", { sessionId: firstSessionId });
  const secondHarness = createExtensionHarness("", { sessionId: secondSessionId });

  try {
    piParleyExtension(firstHarness.pi as never);
    piParleyExtension(secondHarness.pi as never);
    await firstHarness.emitLifecycle("session_start");
    await secondHarness.emitLifecycle("session_start");
    const first = await waitForSessionId(planner, firstSessionId);
    const second = await waitForSessionId(planner, secondSessionId);
    assert.equal(first.name, "session-019fe418-248e-7447");
    assert.equal(second.name, "session-019fe418-248e-7abc");
    assert.equal(first.runtimeFallbackAlias, true);
    assert.equal(second.runtimeFallbackAlias, true);
    assert.notEqual(first.isSubagent, true);
    assert.notEqual(second.isSubagent, true);
    assert.notEqual(first.name, second.name);
  } finally {
    await firstHarness.emitLifecycle("session_shutdown");
    await secondHarness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("broker coalesces no-op presence floods", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const worker = new ParleyClient();
  const updates: SessionInfo[] = [];
  planner.on("presence_update", (session: SessionInfo) => {
    if (session.name === "presence-worker") {
      updates.push(session);
    }
  });

  try {
    await worker.connect({
      name: "presence-worker",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    worker.updatePresence({ status: "idle" });
    for (let i = 0; i < 20; i += 1) {
      worker.updatePresence({ status: "idle" });
    }
    await new Promise((resolve) => setTimeout(resolve, 150));
    assert.equal(updates.length, 1);
  } finally {
    await worker.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("old stable-ID socket cannot mutate the replacement session", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const first = await connectRawRegistered("replaceable-session-id", "replaceable-worker-old");
  const replacement = new ParleyClient();

  try {
    await replacement.connect({
      name: "replaceable-worker-new",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, "replaceable-session-id");

    first.writeMessage(first.socket, { type: "presence", name: "stale-name" });
    first.writeMessage(first.socket, { type: "unregister" });
    await new Promise((resolve) => setTimeout(resolve, 50));
    const replacementSession = await waitForSessionId(planner, "replaceable-session-id");
    assert.equal(replacementSession.name, "replaceable-worker-new");

    const received = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    const sent = await planner.send("replaceable-session-id", { text: "still there" });
    assert.equal(sent.delivered, true);
    const [, message] = await received;
    assert.equal(message.content.text, "still there");
  } finally {
    first.socket.destroy();
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("stable-ID replacement preserves old ask edges and ignores stale cancels", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const first = await connectRawRegistered("replaceable-asker-id", "replaceable-asker-old");
  const replacement = new ParleyClient();

  try {
    first.writeMessage(first.socket, {
      type: "send",
      to: orchestrator.sessionId,
      message: { id: "old-ask-edge", timestamp: Date.now(), expectsReply: true, content: { text: "Old ask" } },
    });
    await new Promise((resolve) => setTimeout(resolve, 50));

    await replacement.connect({
      name: "replaceable-asker-new",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, "replaceable-asker-id");

    const oldReply = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await orchestrator.send("replaceable-asker-id", {
      text: "Old ask answered after replacement.",
      replyTo: "old-ask-edge",
    })).delivered, true);
    assert.equal((await oldReply)[1].replyTo, "old-ask-edge");

    const reverseAfterReplace = await orchestrator.send("replaceable-asker-id", {
      messageId: "reverse-after-replace",
      text: "Can I ask the replacement?",
      expectsReply: true,
    });
    assert.equal(reverseAfterReplace.delivered, true);
    assert.equal((await replacement.send(orchestrator.sessionId!, {
      text: "Replacement answered.",
      replyTo: "reverse-after-replace",
    })).delivered, true);

    const replacementAsk = await replacement.send(orchestrator.sessionId!, {
      messageId: "replacement-ask-edge",
      text: "Replacement ask",
      expectsReply: true,
    });
    assert.equal(replacementAsk.delivered, true);
    first.writeMessage(first.socket, { type: "cancel_ask", messageId: "replacement-ask-edge" });
    await new Promise((resolve) => setTimeout(resolve, 50));

    const reverseWhileReplacementWaits = await orchestrator.send("replaceable-asker-id", {
      messageId: "reverse-while-replacement-waits",
      text: "Can I ask while replacement waits?",
      expectsReply: true,
    });
    assert.equal(reverseWhileReplacementWaits.delivered, true);
    assert.equal(existsSync(pendingAskRecordPath("replacement-ask-edge")), true,
      "a stale socket's cancellation and a reverse ask must not settle the replacement's request");
    assert.equal((await orchestrator.send("replaceable-asker-id", {
      text: "The replacement's request is now answered.", replyTo: "replacement-ask-edge",
    })).delivered, true);
    assert.equal(existsSync(pendingAskRecordPath("replacement-ask-edge")), false);
  } finally {
    first.socket.destroy();
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker resolves unique short IDs and rejects ambiguous prefixes", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const first = new ParleyClient();
  const second = new ParleyClient();
  const evilPrefix = new ParleyClient();

  try {
    await first.connect({ name: "short-id-one", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "abcdef12-session");
    await second.connect({ name: "short-id-two", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "abcdef99-session");
    await evilPrefix.connect({ name: "evil-prefix", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "orchestrator-evil");

    const received = once(first, "message") as Promise<[SessionInfo, Message]>;
    const unique = await planner.send("abcdef12", { text: "prefix works" });
    assert.equal(unique.delivered, true);
    const [, message] = await received;
    assert.equal(message.content.text, "prefix works");

    const ambiguous = await planner.send("abcdef", { text: "ambiguous" });
    assert.equal(ambiguous.delivered, false);
    assert.match(ambiguous.reason ?? "", /Multiple sessions/);

    const exactNameReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const exactName = await planner.send("orchestrator", { text: "exact name wins" });
    assert.equal(exactName.delivered, true);
    const [, exactNameMessage] = await exactNameReceived;
    assert.equal(exactNameMessage.content.text, "exact name wins");
  } finally {
    await first.disconnect().catch(() => undefined);
    await second.disconnect().catch(() => undefined);
    await evilPrefix.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("parley tool prefers exact names over ID prefixes", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const evilPrefix = new ParleyClient();
  const harness = createExtensionHarness("exact-name-worker");

  try {
    await evilPrefix.connect({ name: "evil-prefix", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "orchestrator-evil");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const exactNameReceived = Promise.race([
      once(orchestrator, "message") as Promise<[SessionInfo, Message]>,
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 500)),
    ]);
    const result = await parleyTool.execute("send-exact-name", { action: "send", to: "orchestrator", message: "exact name wins" }, new AbortController().signal, undefined, harness.ctx);
    assert.notEqual(result.details?.error, true);
    assert.equal(result.details?.delivery, "socket_delivered");
    assert.equal(result.details?.retryable, false);
    assert.equal(result.details?.outcomeKnown, true);

    const received = await exactNameReceived;
    assert.notEqual(received, null);
    assert.equal(received![1].content.text, "exact name wins");
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await evilPrefix.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("send accepts multiple explicit targets, reports partial failure, and delivers only once per session", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("multicast-worker");
  const plannerMessages: Message[] = [];
  const orchestratorMessages: Message[] = [];
  const onPlannerMessage = (_from: SessionInfo, message: Message) => plannerMessages.push(message);
  const onOrchestratorMessage = (_from: SessionInfo, message: Message) => orchestratorMessages.push(message);

  planner.on("message", onPlannerMessage);
  orchestrator.on("message", onOrchestratorMessage);
  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const result = await parleyTool.execute("send-many", {
      action: "send",
      targets: ["planner", orchestrator.sessionId!, "missing-peer", planner.sessionId!],
      message: "Shared update",
    }, new AbortController().signal, undefined, harness.ctx);

    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(plannerMessages.length, 1, "name and id aliases for one session must not duplicate delivery");
    assert.equal(orchestratorMessages.length, 1);
    assert.equal(plannerMessages[0]?.content.text, "Shared update");
    assert.equal(orchestratorMessages[0]?.content.text, "Shared update");
    assert.match(result.content[0]?.text ?? "", /accepted for 2 of 3 targets/i);
    assert.equal(result.details?.batch, true);
    assert.equal(result.details?.requestedTargetCount, 4);
    assert.equal(result.details?.recipientCount, 3);
    assert.equal(result.details?.acceptedCount, 2);
    assert.equal(result.details?.failedCount, 1);
    assert.equal(result.details?.duplicateCount, 1);
    const outcomes = result.details?.outcomes as Array<{ to: string; delivered: boolean; messageId?: string; reason?: string }>;
    assert.equal(outcomes.filter((outcome) => outcome.delivered).length, 2);
    assert.match(outcomes.find((outcome) => outcome.to === "missing-peer")?.reason ?? "", /not found/i);

    const receipt = modelText(result);
    assert.match(receipt, /Sent as multicast-worker/);
    const plannerLine = receipt.split("\n").find((line) => line.includes("planner:"))!;
    const plannerId = plannerLine.match(/\(([^()]+)\)$/)?.[1];
    assert.ok(plannerId, "each recipient's full message ID must be visible to the caller");
    assert.equal(plannerId, plannerMessages[0]?.id);
    assert.ok(receipt.includes(orchestratorMessages[0]!.id));
    const cancellation = await parleyTool.execute("cancel-one-multicast", { action: "cancel", messageId: plannerId }, new AbortController().signal, undefined, harness.ctx);
    assert.match(modelText(cancellation), new RegExp(plannerId));
    assert.doesNotMatch(modelText(cancellation), /not accepted|not found/i);
  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    planner.off("message", onPlannerMessage);
    orchestrator.off("message", onOrchestratorMessage);
    await cleanup();
  }
});

test("confirmed multi-target sends preserve the resolved recipient snapshot", { concurrency: false }, async () => {
  await withConfirmSendEnabled(async () => {
    const { cleanup } = await setupClients();
    const { default: piParleyExtension } = await import("./index.ts");
    const original = new ParleyClient();
    const replacement = new ParleyClient();
    const originalId = "confirmed-original-id";
    const replacementMessages: Message[] = [];
    const onReplacementMessage = (_from: SessionInfo, message: Message) => replacementMessages.push(message);
    replacement.on("message", onReplacementMessage);
    let confirmationText = "";
    const registration = {
      name: "confirmed-target",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    };
    const harness = createExtensionHarness("confirmed-multicast-worker", {
      hasUI: true,
      ui: {
        confirm: async (_title: string, text: string) => {
          confirmationText = text;
          await original.disconnect();
          await replacement.connect({ ...registration, startedAt: Date.now(), lastActivity: Date.now() }, "confirmed-replacement-id");
          return true;
        },
      },
    });

    try {
      await original.connect(registration, originalId);
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
      const result = await parleyTool.execute("confirmed-snapshot", {
        action: "send",
        targets: ["confirmed-target"],
        message: "Only for the approved endpoint",
      }, new AbortController().signal, undefined, harness.ctx);

      assert.match(confirmationText, new RegExp(originalId));
      assert.equal(result.details?.acceptedCount, 0);
      const outcomes = result.details?.outcomes as Array<{ targetId?: string; code?: string }>;
      assert.equal(outcomes[0]?.targetId, originalId);
      assert.match(outcomes[0]?.code ?? "", /E_TARGET_(?:NOT_FOUND|REBOUND)/);
      await new Promise((resolve) => setTimeout(resolve, 50));
      assert.equal(replacementMessages.length, 0, "a newly rebound alias must not receive a previously confirmed send");
    } finally {
      await harness.emitLifecycle("session_shutdown").catch(() => undefined);
      replacement.off("message", onReplacementMessage);
      await original.disconnect().catch(() => undefined);
      await replacement.disconnect().catch(() => undefined);
      await cleanup();
    }
  });
});

test("multi-target send preserves ordinary queued-mail delivery for a disconnected recipient", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const disconnected = new ParleyClient();
  const reconnected = new ParleyClient();
  const harness = createExtensionHarness("multicast-mailbox-worker");
  const disconnectedId = "multicast-offline-target";
  const registration = {
    name: "multicast-offline",
    cwd: repoDir,
    model: "test-model",
    pid: process.pid,
    startedAt: Date.now(),
    lastActivity: Date.now(),
  };

  try {
    await disconnected.connect(registration, disconnectedId);
    await disconnected.disconnect();
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const plannerReceived = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const result = await parleyTool.execute("send-many-mailbox", {
      action: "send",
      targets: ["planner", disconnectedId],
      message: "Live and queued update",
    }, new AbortController().signal, undefined, harness.ctx);

    const [, plannerMessage] = await plannerReceived;
    assert.equal(plannerMessage.content.text, "Live and queued update");
    const outcomes = result.details?.outcomes as Array<{ to: string; delivery: string; delivered: boolean }>;
    assert.equal(outcomes.find((outcome) => outcome.to === "planner")?.delivery, "socket_delivered");
    assert.equal(outcomes.find((outcome) => outcome.to === "multicast-offline")?.delivery, "queued");
    assert.match(modelText(result), /multicast-offline: queued/);
    assert.match(result.content[0]?.text ?? "", /queued for offline delivery \(up to 24h while this broker remains running\)/i);

    const queuedReceived = once(reconnected, "message") as Promise<[SessionInfo, Message]>;
    await reconnected.connect({ ...registration, startedAt: Date.now(), lastActivity: Date.now() }, disconnectedId);
    const [, queuedMessage] = await queuedReceived;
    assert.equal(queuedMessage.content.text, "Live and queued update");
  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await disconnected.disconnect().catch(() => undefined);
    await reconnected.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("multi-target send keeps case-sensitive disconnected IDs distinct", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const upper = new ParleyClient();
  const lower = new ParleyClient();
  const harness = createExtensionHarness("case-sensitive-target-worker");
  const registration = (name: string) => ({
    name,
    cwd: repoDir,
    model: "test-model",
    pid: process.pid,
    startedAt: Date.now(),
    lastActivity: Date.now(),
  });

  try {
    await upper.connect(registration("case-upper"), "Worker-A");
    await lower.connect(registration("case-lower"), "worker-a");
    await upper.disconnect();
    await lower.disconnect();
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const result = await parleyTool.execute("case-sensitive-targets", {
      action: "send",
      targets: ["Worker-A", "worker-a"],
      message: "Separate offline identities",
    }, new AbortController().signal, undefined, harness.ctx);

    assert.equal(result.details?.recipientCount, 2);
    assert.equal(result.details?.acceptedCount, 2);
    assert.equal(result.details?.duplicateCount, 0);
    const receipt = modelText(result);
    assert.match(receipt, /case-upper: queued/);
    assert.match(receipt, /case-lower: queued/);
    const upperReceived = once(upper, "message") as Promise<[SessionInfo, Message]>;
    const lowerReceived = once(lower, "message") as Promise<[SessionInfo, Message]>;
    await upper.connect(registration("case-upper"), "Worker-A");
    await lower.connect(registration("case-lower"), "worker-a");
    const upperMessage = (await upperReceived)[1];
    const lowerMessage = (await lowerReceived)[1];
    assert.equal(upperMessage.content.text, "Separate offline identities");
    assert.equal(lowerMessage.content.text, "Separate offline identities");
    assert.notEqual(upperMessage.id, lowerMessage.id);
    assert.ok(receipt.includes(upperMessage.id));
    assert.ok(receipt.includes(lowerMessage.id));
  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await upper.disconnect().catch(() => undefined);
    await lower.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broadcast reaches visible local sessions across working directories and reports its scope", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const otherProject = new ParleyClient();
  const harness = createExtensionHarness("broadcast-worker");

  try {
    await otherProject.connect({
      name: "other-project-worker",
      cwd: path.join(repoDir, "other-project"),
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const plannerReceived = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const orchestratorReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const otherProjectReceived = once(otherProject, "message") as Promise<[SessionInfo, Message]>;
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const result = await parleyTool.execute("broadcast", {
      action: "broadcast",
      message: "Machine-wide maintenance notice",
    }, new AbortController().signal, undefined, harness.ctx);

    const received = await Promise.race([
      Promise.all([plannerReceived, orchestratorReceived, otherProjectReceived]),
      new Promise<never>((_resolve, reject) => {
        setTimeout(() => reject(new Error("Timed out waiting for every broadcast recipient")), 2000).unref();
      }),
    ]);
    assert.deepEqual(received.map(([, message]) => message.content.text), [
      "Machine-wide maintenance notice",
      "Machine-wide maintenance notice",
      "Machine-wide maintenance notice",
    ]);
    assert.equal(result.details?.broadcast, true);
    assert.equal(result.details?.recipientCount, 3);
    assert.equal(result.details?.acceptedCount, 3);
    assert.equal(result.details?.failedCount, 0);
    assert.match(result.content[0]?.text ?? "", /broadcast accepted for 3 of 3 visible sessions/i);
    assert.match(modelText(result), /Host-local broadcast; 0 visible remote peer\(s\) were not included/i);

    const sentEntries = harness.entries.filter((entry) => entry.type === "parley_sent");
    assert.equal(sentEntries.length, 3);
  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await otherProject.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("direct contact reports later compactions once while broadcast neither reports nor consumes them", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("awareness-direct-worker", {
    sessionId: "awareness-direct-worker-id",
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;

    const baseline = await parleyTool.execute("awareness-baseline", {
      action: "send",
      to: "planner",
      message: "Establish direct-contact baseline",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(baseline.details?.peerCompaction, undefined, "first contact establishes a baseline without a historical claim");

    planner.updatePresence({ contextPct: 37 });
    const firstCompaction = await planner.reportCompactionCompleted();
    const nextGeneration = firstCompaction.generation;

    const broadcast = await parleyTool.execute("awareness-broadcast", {
      action: "broadcast",
      message: "Do not consume direct-contact awareness",
    }, new AbortController().signal, undefined, harness.ctx);
    const broadcastOutcomes = broadcast.details?.outcomes as Array<{ to: string; peerCompaction?: unknown }>;
    assert.equal(broadcastOutcomes.find((outcome) => outcome.to.startsWith("planner "))?.peerCompaction, undefined);

    const noticed = await parleyTool.execute("awareness-noticed", {
      action: "send",
      to: "planner",
      message: "Direct contact after compaction",
    }, new AbortController().signal, undefined, harness.ctx);
    const notice = noticed.details?.peerCompaction as { generation: number; previousGeneration: number; contextPct?: number };
    assert.equal(notice.generation, nextGeneration);
    assert.equal(notice.previousGeneration, nextGeneration - 1);
    assert.equal(notice.contextPct, 37);
    assert.match(noticed.content[0]?.text ?? "", /compacted context since your last direct contact/i);
    assert.match(noticed.content[0]?.text ?? "", /context usage is 37%/i);

    const repeated = await parleyTool.execute("awareness-repeated", {
      action: "send",
      to: "planner",
      message: "Same generation again",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(repeated.details?.peerCompaction, undefined, "one generation is reported only once per observer/peer watermark");

    await planner.reportCompactionCompleted();
    const thirdCompaction = await planner.reportCompactionCompleted();
    assert.equal(thirdCompaction.generation, nextGeneration + 2);
    const multiple = await parleyTool.execute("awareness-multiple", {
      action: "send",
      to: "planner",
      message: "Two compactions later",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(multiple.content[0]?.text ?? "", /\(2 compactions\)/i);
  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await cleanup();
  }
});

test("direct compaction notices remain pending until the capable sender acknowledges them", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const received: Message[] = [];
  const onMessage = (_from: SessionInfo, message: Message) => received.push(message);
  orchestrator.on("message", onMessage);
  try {
    const baseline = await planner.send(orchestrator.sessionId!, { text: "baseline", contactKind: "direct" });
    assert.equal(baseline.peerCompaction, undefined);
    await new Promise((resolve) => setTimeout(resolve, 30));

    const compacted = await orchestrator.reportCompactionCompleted();
    const first = await planner.send(orchestrator.sessionId!, { text: "first notice", contactKind: "direct" });
    assert.equal(first.peerCompaction?.generation, compacted.generation);

    const repeated = await planner.send(orchestrator.sessionId!, { text: "repeat before ack", contactKind: "direct" });
    assert.equal(repeated.peerCompaction?.generation, compacted.generation, "unacknowledged notice must repeat rather than be consumed");
    planner.acknowledgeSendContact(repeated);

    const afterAck = await planner.send(orchestrator.sessionId!, { text: "after ack", contactKind: "direct" });
    assert.equal(afterAck.peerCompaction, undefined);
    assert.equal(received.length, 4);
  } finally {
    orchestrator.off("message", onMessage);
    await cleanup();
  }
});

test("queued awareness omits stale context usage from a disconnected peer snapshot", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  try {
    await planner.send(orchestrator.sessionId!, { text: "baseline", contactKind: "direct" });
    await new Promise((resolve) => setTimeout(resolve, 30));
    orchestrator.updatePresence({ contextPct: 90 });
    const compacted = await orchestrator.reportCompactionCompleted();
    const targetId = orchestrator.sessionId!;
    await orchestrator.disconnect();
    await waitForNoSessionId(planner, targetId);

    const queued = await planner.send(targetId, { text: "offline notice", contactKind: "direct" });
    assert.equal(queued.delivery, "queued");
    assert.equal(queued.peerCompaction?.generation, compacted.generation);
    assert.equal(queued.peerCompaction?.contextPct, undefined, `disconnected presence is not described as current: ${JSON.stringify(queued)}`);
    planner.acknowledgeSendContact(queued);
  } finally {
    await cleanup();
  }
});

test("mailbox rebound awareness names the actual peer and preserves the requested stable ID", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new ParleyClient();
  try {
    const departedId = orchestrator.sessionId!;
    await orchestrator.disconnect();
    await waitForNoSessionId(planner, departedId);
    await replacement.connect({
      name: "orchestrator",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    const replacementId = replacement.sessionId!;
    await planner.send(replacementId, { text: "replacement baseline", contactKind: "direct" });
    const compacted = await replacement.reportCompactionCompleted();

    const rebound = await planner.send(departedId, { text: "exact old mailbox identity", contactKind: "direct" });
    assert.equal(rebound.delivery, "socket_delivered");
    assert.equal(rebound.peerCompaction?.generation, compacted.generation);
    assert.equal(rebound.peerCompaction?.peerSessionId, replacementId);
    assert.equal(rebound.peerCompaction?.requestedPeerSessionId, departedId);
    planner.acknowledgeSendContact(rebound);
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("explicit multicast tracks each recipient compaction independently", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("awareness-multicast-worker", {
    sessionId: "awareness-multicast-worker-id",
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    await parleyTool.execute("awareness-multicast-baseline", {
      action: "send",
      targets: ["planner", "orchestrator"],
      message: "Establish independent baselines",
    }, new AbortController().signal, undefined, harness.ctx);

    const plannerGeneration = (await planner.reportCompactionCompleted()).generation;
    const orchestratorGeneration = (await orchestrator.reportCompactionCompleted()).generation;

    const result = await parleyTool.execute("awareness-multicast", {
      action: "send",
      targets: ["planner", "orchestrator"],
      message: "Each recipient has compacted",
    }, new AbortController().signal, undefined, harness.ctx);
    const outcomes = result.details?.outcomes as Array<{ to: string; peerCompaction?: { generation: number } }>;
    assert.equal(outcomes.find((outcome) => outcome.to === "planner")?.peerCompaction?.generation, plannerGeneration);
    assert.equal(outcomes.find((outcome) => outcome.to === "orchestrator")?.peerCompaction?.generation, orchestratorGeneration);
    assert.equal((result.content[0]?.text.match(/compacted context since your last direct contact/gi) ?? []).length, 2);
  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await cleanup();
  }
});

test("incoming direct contact carries compaction awareness without an unsolicited wake", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("awareness-inbound-worker", {
    sessionId: "awareness-inbound-worker-id",
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "awareness-inbound-worker");

    await planner.send(worker.id, { text: "Establish inbound baseline", contactKind: "direct" });
    const firstDeadline = Date.now() + 2000;
    while (harness.sentMessages.length < 1 && Date.now() < firstDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(harness.sentMessages.length, 1);
    assert.doesNotMatch(harness.sentMessages[0]?.message.content ?? "", /compacted context since your last direct contact/i);
    const baselineAckDeadline = Date.now() + 2_000;
    while (!harness.entries.some((entry) => entry.type === "parley_receiver_baseline_recorded") && Date.now() < baselineAckDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    const pendingBaseline = harness.entries.find((entry) => entry.type === "parley_receiver_baseline_pending");
    const recordedBaseline = harness.entries.find((entry) => entry.type === "parley_receiver_baseline_recorded");
    assert.ok(pendingBaseline, "receiver journals the staged baseline token after surfacing the first message");
    assert.ok(recordedBaseline, "receiver journals the broker's durable baseline acknowledgement");
    assert.equal(
      (pendingBaseline.data as { token: string }).token,
      (recordedBaseline.data as { token: string }).token,
    );

    const plannerGeneration = (await planner.reportCompactionCompleted()).generation;
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(harness.sentMessages.length, 1, "compaction itself must not inject or wake a collaborator");

    await planner.send(worker.id, { text: "Actual direct contact", expectsReply: true, contactKind: "direct" });
    const secondDeadline = Date.now() + 2000;
    while (harness.sentMessages.length < 2 && Date.now() < secondDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(harness.sentMessages.length, 2);
    assert.match(harness.sentMessages[1]?.message.content ?? "", /planner compacted context since your last direct contact/i);
    assert.match(harness.sentMessages[1]?.message.content ?? "", /Actual direct contact/);
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const pending = await parleyTool.execute("awareness-pending", {
      action: "pending",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(pending.content[0]?.text ?? "", /sender compacted since prior direct contact/i);
  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await cleanup();
  }
});

test("ask retains contact-time compaction awareness in its eventual reply result", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("awareness-ask-worker", {
    sessionId: "awareness-ask-worker-id",
  });
  const replyToAsk = async (from: SessionInfo, message: Message) => {
    if (!message.expectsReply) return;
    await planner.send(from.id, {
      text: "Current answer",
      replyTo: message.id,
      contactKind: "direct",
    });
  };
  planner.on("message", replyToAsk);

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    await parleyTool.execute("awareness-ask-baseline", {
      action: "send",
      to: "planner",
      message: "Establish ask baseline",
    }, new AbortController().signal, undefined, harness.ctx);

    const plannerGeneration = (await planner.reportCompactionCompleted()).generation;

    const result = await parleyTool.execute("awareness-ask", {
      action: "ask",
      to: "planner",
      message: "What is current?",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(result.content[0]?.text ?? "", /compacted context since your last direct contact/i);
    assert.match(modelText(result), /\*\*Reply from planner\*\* \(asked as awareness-ask-worker\)/);
    assert.match(modelText(result), /Current answer/);
    assert.match(modelText(result), /Question message ID: \S+/);
    assert.match(modelText(result), /Reply message ID: \S+/);
    assert.equal((result.details?.peerCompaction as { generation?: number })?.generation, plannerGeneration);
  } finally {
    planner.off("message", replyToAsk);
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await cleanup();
  }
});

test("broadcast preserves subagent visibility instead of disclosing or messaging hidden sessions", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const plannerMessages: Message[] = [];
  const onPlannerMessage = (_from: SessionInfo, message: Message) => plannerMessages.push(message);
  planner.on("message", onPlannerMessage);

  try {
    await withChildOrchestratorEnv({
      orchestratorTarget: "orchestrator",
      runId: "broadcast-acl-run",
      agent: "worker",
      index: "0",
    }, async () => {
      const { default: piParleyExtension } = await import("./index.ts");
      const harness = createExtensionHarness("broadcast-acl-child");
      try {
        piParleyExtension(harness.pi as never);
        await harness.emitLifecycle("session_start");
        const supervisorReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
        const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
        const result = await parleyTool.execute("broadcast-acl", {
          action: "broadcast",
          message: "Visible collaborators only",
        }, new AbortController().signal, undefined, harness.ctx);

        const [, supervisorMessage] = await supervisorReceived;
        await new Promise((resolve) => setTimeout(resolve, 50));
        assert.equal(supervisorMessage.content.text, "Visible collaborators only");
        assert.equal(plannerMessages.length, 0, "a restricted child must not reach or discover an unrelated main");
        assert.equal(result.details?.recipientCount, 1);
        assert.equal(result.details?.acceptedCount, 1);
      } finally {
        await harness.emitLifecycle("session_shutdown").catch(() => undefined);
      }
    });
  } finally {
    planner.off("message", onPlannerMessage);
    await cleanup();
  }
});

test("multi-target and broadcast sends reject ambiguous targeting and conversation-specific metadata", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("multicast-validation-worker");
  const receivedMessages: Message[] = [];
  const onMessage = (_from: SessionInfo, message: Message) => receivedMessages.push(message);
  planner.on("message", onMessage);
  orchestrator.on("message", onMessage);

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const toolSchema = parleyTool.parameters as {
      required?: string[];
      properties?: { profile?: { required?: string[] } };
    };
    assert.deepEqual(toolSchema.required, ["action"], "the extension itself keeps addon fields optional");
    assert.equal(toolSchema.properties?.profile?.required, undefined);

    const placeholderDelivered = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const placeholderResult = await parleyTool.execute("schema-placeholder-targets", {
      action: "send",
      to: "planner",
      targets: [""],
      message: "Optional schema placeholders should be ignored",
      profile: { name: "", description: "" },
    }, new AbortController().signal, undefined, harness.ctx);
    const [, placeholderMessage] = await placeholderDelivered;
    assert.equal(placeholderMessage.content.text, "Optional schema placeholders should be ignored");
    assert.equal(placeholderResult.details?.error, undefined);
    assert.match(placeholderResult.content[0]?.text ?? "", /Message sent as multicast-validation-worker to planner/);

    // Some adapters duplicate the recipient into both optional fields; a
    // single identical target is the same singular delivery intent.
    const duplicatedDelivered = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const duplicatedResult = await parleyTool.execute("duplicated-singular-target", {
      action: "send",
      to: "planner",
      targets: ["planner"],
      message: "Duplicated singular recipient should deliver once",
      profile: { name: "", description: "Auditing leaderboard claims and statistical trust" },
    }, new AbortController().signal, undefined, harness.ctx);
    const [, duplicatedMessage] = await duplicatedDelivered;
    assert.equal(duplicatedMessage.content.text, "Duplicated singular recipient should deliver once");
    assert.equal(duplicatedResult.details?.error, undefined);
    assert.match(duplicatedResult.content[0]?.text ?? "", /Message sent as multicast-validation-worker to planner/);

    const mixedBlankTargets = await parleyTool.execute("mixed-blank-targets", {
      action: "send",
      targets: ["planner", ""],
      message: "Malformed mixed recipients",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(mixedBlankTargets.details?.error, true);
    assert.match(mixedBlankTargets.content[0]?.text ?? "", /1-32 non-empty/i);

    const bothTargetForms = await parleyTool.execute("invalid-targets", {
      action: "send",
      to: "planner",
      targets: ["orchestrator"],
      message: "Ambiguous recipients",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(bothTargetForms.details?.error, true);
    assert.match(bothTargetForms.content[0]?.text ?? "", /either 'to' or 'targets'/i);

    const threadedBatch = await parleyTool.execute("invalid-thread", {
      action: "send",
      targets: ["planner", "orchestrator"],
      message: "Not a valid shared reply",
      replyTo: "one-conversation-only",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(threadedBatch.details?.error, true);
    assert.match(threadedBatch.content[0]?.text ?? "", /cannot use replyTo, supersedes, or retryOf/i);

    const addressedBroadcast = await parleyTool.execute("invalid-broadcast", {
      action: "broadcast",
      to: "planner",
      message: "Not actually a broadcast",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(addressedBroadcast.details?.error, true);
    assert.match(addressedBroadcast.content[0]?.text ?? "", /does not accept 'to'/i);

    const threadedBroadcast = await parleyTool.execute("invalid-broadcast-thread", {
      action: "broadcast",
      message: "Not a shared reply",
      retryOf: "one-recipient-message",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(threadedBroadcast.details?.error, true);
    assert.match(threadedBroadcast.content[0]?.text ?? "", /cannot use replyTo, supersedes, or retryOf/i);

    const callerSuppliedMessageId = await parleyTool.execute("invalid-message-id", {
      action: "send",
      to: "planner",
      message: "Do not reuse this ID",
      messageId: "caller-owned-id",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(callerSuppliedMessageId.details?.error, true);
    assert.match(modelText(callerSuppliedMessageId), /retained message for read or cancel/i);
    assert.match(modelText(callerSuppliedMessageId), /sends and asks create a new message ID/i);

    const oversizedTargets = await parleyTool.execute("too-many-targets", {
      action: "send",
      targets: Array.from({ length: 33 }, (_, index) => `worker-${index}`),
      message: "Too broad",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(oversizedTargets.details?.error, true);
    assert.match(oversizedTargets.content[0]?.text ?? "", /1-32 non-empty/i);

    const batchAsk = await parleyTool.execute("invalid-batch-ask", {
      action: "ask",
      to: "planner",
      targets: ["planner", "orchestrator"],
      message: "Everyone answer",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(batchAsk.details?.error, true);
    assert.match(batchAsk.content[0]?.text ?? "", /ask accepts one recipient/i);

    const abortController = new AbortController();
    abortController.abort();
    const cancelledBatch = await parleyTool.execute("cancelled-batch", {
      action: "send",
      targets: ["planner", "orchestrator"],
      message: "Do not deliver",
    }, abortController.signal, undefined, harness.ctx);
    assert.equal(cancelledBatch.details?.acceptedCount, 0);
    assert.deepEqual(
      (cancelledBatch.details?.outcomes as Array<{ code: string }>).map((outcome) => outcome.code),
      ["E_CANCELLED", "E_CANCELLED"],
    );
    await new Promise((resolve) => setTimeout(resolve, 25));
    assert.equal(
      receivedMessages.length,
      2,
      "only the placeholder regression send and the duplicated-singular send should be delivered",
    );
    assert.equal(receivedMessages[0]?.content.text, "Optional schema placeholders should be ignored");
    assert.equal(receivedMessages[1]?.content.text, "Duplicated singular recipient should deliver once");

  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    planner.off("message", onMessage);
    orchestrator.off("message", onMessage);
    await cleanup();
  }
});

test("extension can pin a restart-stable parley session id", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const previousStableId = process.env.PI_PARLEY_STABLE_ID;
  const previousPublishedId = process.env.PI_PARLEY_SESSION_ID;
  process.env.PI_PARLEY_STABLE_ID = "pinned-worker-session";
  const harness = createExtensionHarness("pinned-worker", { sessionId: "transient-pi-session" });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const session = await waitForSessionId(planner, "pinned-worker-session");
    assert.equal(session.name, "pinned-worker");
    assert.equal(process.env.PI_PARLEY_SESSION_ID, "pinned-worker-session");
    await harness.emitLifecycle("session_shutdown");
  } finally {
    if (previousStableId === undefined) delete process.env.PI_PARLEY_STABLE_ID;
    else process.env.PI_PARLEY_STABLE_ID = previousStableId;
    if (previousPublishedId === undefined) delete process.env.PI_PARLEY_SESSION_ID;
    else process.env.PI_PARLEY_SESSION_ID = previousPublishedId;
    await cleanup();
  }
});

test("parley-id inserts a stable handoff snippet into the editor", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  let editorText = "Existing note";
  const notifications: string[] = [];
  const harness = createExtensionHarness("handoff-worker", {
    hasUI: true,
    ui: {
      getEditorText: () => editorText,
      setEditorText: (text: string) => { editorText = text; },
      notify: (message: string) => { notifications.push(message); },
    },
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await harness.commands.get("parley-id")!("", harness.ctx);
    assert.match(editorText, /Existing note\n\nPi parley target: session-child-test/);
    assert.doesNotMatch(editorText, /action:/, "the handoff supplies an address without choosing a communication action");
    assert.match(notifications.at(-1) ?? "", /Inserted parley contact target: session-child-test/);
  } finally {
    // Shutdown must run even when an assertion fails: otherwise the extension's
    // reconnect timer outlives the broker and can attach to later tests.
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("alias names the current session, opens the local input menu, and appears in parley displays", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const inputCalls: Array<[string, string | undefined]> = [];
  const inputValues = ["menu-worker", "no-arg-worker"];
  const harness = createExtensionHarness("alias-worker", {
    hasUI: true,
    ui: {
      input: async (title: string, placeholder?: string) => {
        inputCalls.push([title, placeholder]);
        return inputValues.shift();
      },
      notify: () => undefined,
    },
  });

  try {
    await withChildOrchestratorEnv({}, async () => {
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const initial = await waitForSessionByName(planner, "alias-worker");
      const aliasCommand = harness.commands.get("alias")!;

      await aliasCommand("direct-worker", harness.ctx);
      const direct = await waitForSessionByName(planner, "direct-worker");
      assert.equal(direct.id, initial.id);
      assert.equal(harness.pi.getSessionName(), "direct-worker");
      assert.equal(inputCalls.length, 0);

      await aliasCommand("menu", harness.ctx);
      await waitForSessionByName(planner, "menu-worker");
      await aliasCommand("", harness.ctx);
      const current = await waitForSessionByName(planner, "no-arg-worker");
      assert.equal(current.id, initial.id);
      assert.deepEqual(inputCalls, [
        ["Set session alias", "Current alias: direct-worker"],
        ["Set session alias", "Current alias: menu-worker"],
      ]);

      const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
      const listed = await parleyTool.execute("alias-list", { action: "list" }, new AbortController().signal, undefined, harness.ctx);
      assert.match(listed.content[0]?.text ?? "", /no-arg-worker/);

      orchestrator.updatePresence({ name: "alias-orchestrator" });
      await waitForSessionByName(planner, "alias-orchestrator");
      const outgoing = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const sendResult = await parleyTool.execute("alias-send", {
        action: "send",
        to: "alias-orchestrator",
        message: "Alias display check.",
      }, new AbortController().signal, undefined, harness.ctx);
      assert.match(modelText(sendResult), /Message sent as no-arg-worker to alias-orchestrator/);
      assert.equal((await outgoing)[1].content.text, "Alias display check.");

      const askId = "alias-reply-ask";
      assert.equal((await orchestrator.send(initial.id, {
        messageId: askId,
        text: "Reply using the alias.",
        expectsReply: true,
      })).delivered, true);
      await new Promise((resolve) => setTimeout(resolve, 25));
      const replyReceived = waitForReply(orchestrator, askId);
      const replyResult = await parleyTool.execute("alias-reply", {
        action: "reply",
        replyTo: askId,
        message: "Alias reply display check.",
      }, new AbortController().signal, undefined, harness.ctx);
      assert.match(modelText(replyResult), /Reply sent as no-arg-worker to alias-orchestrator/);
      assert.equal((await replyReceived).message.content.text, "Alias reply display check.");
    });
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("alias reports no-UI usage and current alias without hanging", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const output: string[] = [];
  const previousConsoleError = console.error;
  console.error = (...args: unknown[]) => {
    output.push(args.map((value) => String(value)).join(" "));
  };
  const harness = createExtensionHarness("no-ui-worker");

  try {
    await withChildOrchestratorEnv({}, async () => {
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const initial = await waitForSessionByName(planner, "no-ui-worker");
      const aliasCommand = harness.commands.get("alias")!;

      await aliasCommand("no-ui-renamed", harness.ctx);
      const renamed = await waitForSessionByName(planner, "no-ui-renamed");
      assert.equal(renamed.id, initial.id);
      assert.equal(harness.pi.getSessionName(), "no-ui-renamed");
      assert.deepEqual(output, ["Session alias set: no-ui-renamed"]);

      output.length = 0;
      await aliasCommand("", harness.ctx);
      assert.deepEqual(output, ["Session alias: no-ui-renamed"]);

      output.length = 0;
      await aliasCommand("menu", harness.ctx);
      assert.deepEqual(output, ["The alias menu requires an interactive UI; use /alias <name>."]);
      assert.equal(harness.pi.getSessionName(), "no-ui-renamed");
    });
  } finally {
    console.error = previousConsoleError;
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("parley tool auto-suffixes colliding names so by-name targets stay unambiguous", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const twinA = new ParleyClient();
  const twinB = new ParleyClient();
  const harness = createExtensionHarness("collision-sender");

  try {
    await twinA.connect({ name: "twin", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "019fc92c-066f-755e-95d8-50ebb030d40d");
    await twinB.connect({ name: "twin", cwd: `${repoDir}/other`, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "019fc92c-b5f7-7536-b715-e41a4a6e9eb5");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const listed = await parleyTool.execute("list-twin", { action: "list" }, new AbortController().signal, undefined, harness.ctx);
    const listText = listed.content.map((part) => (part as { text?: string }).text ?? "").join("");
    // Fork: a colliding registration name is auto-suffixed instead of left
    // ambiguous until send time. Roster rows still carry ID prefixes.
    assert.match(listText, /019fc92c-066f/);
    assert.match(listText, /019fc92c-b5f7/);
    assert.match(listText, /twin-2/);

    const listedCwd = await parleyTool.execute("list-cwd-twin", { action: "list-cwd" }, new AbortController().signal, undefined, harness.ctx);
    const listCwdText = listedCwd.content.map((part) => (part as { text?: string }).text ?? "").join("");
    assert.match(listCwdText, /019fc92c-066f/);
    assert.doesNotMatch(listCwdText, /019fc92c-b5f7/);

    // By-name sends resolve unambiguously to each twin.
    const toFirst = await parleyTool.execute("send-twin", { action: "send", to: "twin", message: "the original" }, new AbortController().signal, undefined, harness.ctx);
    assert.notEqual(toFirst.details?.error, true);
    assert.equal(toFirst.details?.delivery, "socket_delivered");

    const toSecond = await parleyTool.execute("send-twin-2", { action: "send", to: "twin-2", message: "the suffixed twin" }, new AbortController().signal, undefined, harness.ctx);
    assert.notEqual(toSecond.details?.error, true);
    assert.equal(toSecond.details?.delivery, "socket_delivered");
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await twinA.disconnect().catch(() => undefined);
    await twinB.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("extension channels register locally without creating conversation messages", async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness();
  let channel: ParleyExtensionChannel | undefined;
  const extensionEvents: unknown[] = [];

  piParleyExtension(harness.pi as never);
  harness.pi.events.emit(PARLEY_EXTENSION_REGISTER_EVENT, {
    namespace: "test-extension/v1",
    ownerEligible: true,
    onReady: (value: ParleyExtensionChannel) => { channel = value; },
    onEvent: (event: unknown) => extensionEvents.push(event),
  });

  assert.equal(channel?.namespace, "test-extension/v1");
  assert.deepEqual(channel?.snapshot(), {
    connected: false,
    supported: false,
  });
  assert.deepEqual(extensionEvents, []);
  assert.deepEqual(harness.sentMessages, []);
  assert.deepEqual(harness.entries, []);
});

test("late extension registration advertises before an onReady publish", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const observer = new ParleyClient();
  const observerMessages: BrokerMessage[] = [];
  const harness = createExtensionHarness("late-extension-worker");
  const extensionEvents: unknown[] = [];

  try {
    observer.onBrokerMessage((message) => observerMessages.push(message));
    observer.on("error", () => {});
    await observer.connect({
      name: "extension-observer",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
      extensions: [{ namespace: "late-extension/v1", ownerEligible: false }],
    });

    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(observer, "late-extension-worker");

    harness.pi.events.emit(PARLEY_EXTENSION_REGISTER_EVENT, {
      namespace: "late-extension/v1",
      ownerEligible: true,
      onReady: (channel: ParleyExtensionChannel) => {
        channel.publish({ probe: "onReady" }, { audience: "capable" });
      },
      onEvent: (event: unknown) => extensionEvents.push(event),
    });

    const deadline = Date.now() + 3000;
    while (
      Date.now() < deadline
      && !observerMessages.some((message) => message.type === "extension_message"
        && (message.payload as { probe?: string }).probe === "onReady")
    ) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(
      observerMessages.some((message) => message.type === "extension_message"
        && (message.payload as { probe?: string }).probe === "onReady"),
      true,
    );
    assert.equal(
      extensionEvents.some((event) => typeof event === "object" && event !== null
        && (event as { type?: string }).type === "connection"
        && (event as { connected?: boolean }).connected === true),
      true,
    );
    assert.deepEqual(harness.sentMessages, []);
    assert.deepEqual(harness.entries, []);
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await observer.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("extension outbox sends notify-only messages with trace and provenance", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("outbox-worker");
  const results: ParleyOutboxResultV1[] = [];

  try {
    harness.pi.events.on(PARLEY_OUTBOX_RESULT_EVENT, (payload) => results.push(payload as ParleyOutboxResultV1));
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const delivered = once(planner, "message") as Promise<[SessionInfo, Message]>;

    harness.pi.events.emit(PARLEY_OUTBOX_REQUEST_EVENT, {
      version: 1,
      requestId: "outbox-success-1",
      extensionId: "example-extension",
      extensionName: "Example Extension",
      to: "planner",
      message: "Outbox hello.",
    });

    const [result] = await waitForOutboxResults(results, 1);
    assert.equal(result?.status, "sent");
    assert.equal(result?.requestId, "outbox-success-1");
    assert.ok(result?.messageId);
    assert.notEqual(result.messageId, result.requestId);
    const [, message] = await Promise.race([
      delivered,
      new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error("Timed out waiting for outbox delivery")), 3000)),
    ]);
    assert.equal(message.id, result.messageId);
    assert.equal(message.content.text, "Outbox hello.");
    assert.deepEqual(message.provenance, {
      type: "extension_outbox",
      extensionId: "example-extension",
      extensionName: "Example Extension",
      requestId: "outbox-success-1",
    });
    assert.equal(harness.entries.some((entry) => entry.type === "parley_sent"
      && (entry.data as { extension?: { requestId?: string } }).extension?.requestId === "outbox-success-1"), true);
    assert.equal(harness.entries.some((entry) => entry.type === "parley_outbox_result"
      && (entry.data as { requestId?: string; status?: string }).requestId === "outbox-success-1"
      && (entry.data as { requestId?: string; status?: string }).status === "sent"), true);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("extension outbox rejects duplicate request ids without duplicate delivery", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("outbox-duplicate-worker");
  const results: ParleyOutboxResultV1[] = [];
  const deliveredMessages: Message[] = [];

  try {
    planner.on("message", (_from: SessionInfo, message: Message) => deliveredMessages.push(message));
    harness.pi.events.on(PARLEY_OUTBOX_RESULT_EVENT, (payload) => results.push(payload as ParleyOutboxResultV1));
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const request = {
      version: 1,
      requestId: "outbox-duplicate-1",
      extensionId: "example-extension",
      extensionName: "Example Extension",
      to: "planner",
      message: "Deliver once.",
    };
    harness.pi.events.emit(PARLEY_OUTBOX_REQUEST_EVENT, request);
    await waitForOutboxResults(results, 1);
    harness.pi.events.emit(PARLEY_OUTBOX_REQUEST_EVENT, request);

    const [, duplicate] = await waitForOutboxResults(results, 2);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(duplicate?.status, "rejected");
    assert.equal(duplicate?.code, "duplicate_request");
    assert.equal(deliveredMessages.length, 1);
    assert.equal(deliveredMessages[0]?.id, results[0]?.messageId);
    assert.equal(deliveredMessages[0]?.provenance?.requestId, "outbox-duplicate-1");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("extension outbox fails closed when confirmation needs unavailable UI", async () => {
  await withConfirmSendEnabled(async () => {
    const { default: piParleyExtension } = await import("./index.ts");
    const harness = createExtensionHarness("outbox-no-ui", { hasUI: false });
    const results: ParleyOutboxResultV1[] = [];

    harness.pi.events.on(PARLEY_OUTBOX_RESULT_EVENT, (payload) => results.push(payload as ParleyOutboxResultV1));
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    harness.pi.events.emit(PARLEY_OUTBOX_REQUEST_EVENT, {
      version: 1,
      requestId: "outbox-no-ui-1",
      extensionId: "example-extension",
      extensionName: "Example Extension",
      to: "planner",
      message: "Needs confirmation.",
    });

    const [result] = await waitForOutboxResults(results, 1);
    assert.equal(result?.status, "blocked");
    assert.equal(result?.code, "confirmation_unavailable");
    assert.equal(harness.entries.some((entry) => entry.type === "parley_outbox_result"
      && (entry.data as { code?: string }).code === "confirmation_unavailable"), true);
    await harness.emitLifecycle("session_shutdown");
  });
});

test("extension outbox settles pending confirmation on session shutdown", async () => {
  await withConfirmSendEnabled(async () => {
    const { cleanup } = await setupClients();
    const { default: piParleyExtension } = await import("./index.ts");
    const confirmCalls: string[] = [];
    const harness = createExtensionHarness("outbox-shutdown", {
      hasUI: true,
      ui: {
        confirm: (title: string) => {
          confirmCalls.push(title);
          return new Promise<boolean>(() => undefined);
        },
      },
    });
    const results: ParleyOutboxResultV1[] = [];

    try {
      harness.pi.events.on(PARLEY_OUTBOX_RESULT_EVENT, (payload) => results.push(payload as ParleyOutboxResultV1));
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      harness.pi.events.emit(PARLEY_OUTBOX_REQUEST_EVENT, {
        version: 1,
        requestId: "outbox-shutdown-1",
        extensionId: "example-extension",
        extensionName: "Example Extension",
        to: "planner",
        message: "Will shut down.",
      });
      const deadline = Date.now() + 3000;
      while (confirmCalls.length === 0 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      assert.equal(confirmCalls.length, 1);
      await harness.emitLifecycle("session_shutdown");

      const [result] = await waitForOutboxResults(results, 1);
      assert.equal(result?.status, "failed");
      assert.equal(result?.code, "session_ended");
    } finally {
      await harness.emitLifecycle("session_shutdown");
      await cleanup();
    }
  });
});

test("parley tool renders compact call and result rows", async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness();

  piParleyExtension(harness.pi as never);
  const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;

  assert.ok(parleyTool.renderCall);
  assert.ok(parleyTool.renderResult);
  assert.match(renderToText(parleyTool.renderCall({
    action: "ask",
    to: "planner",
    message: "Need a decision before I continue with this implementation.",
    attachments: [{ type: "snippet", name: "note.ts", content: "const ok = true;" }],
  }, renderTheme, {})), /parley ask → planner \(1 attachment\)\n {2}Need a decision/);
  assert.match(renderToText(parleyTool.renderCall({
    action: "send",
    targets: ["planner", "reviewer", "worker"],
    message: "Shared update",
  }, renderTheme, {})), /parley send → planner, reviewer, worker\n {2}Shared update/);
  assert.match(renderToText(parleyTool.renderCall({
    action: "broadcast",
    message: "Machine-wide notice",
  }, renderTheme, {})), /parley broadcast → visible local sessions\n {2}Machine-wide notice/);

  const resultText = renderToText(parleyTool.renderResult({
    content: [{ type: "text", text: "Message sent to planner" }],
    details: { delivered: true, messageId: "abcdef123456" },
  }, { isPartial: false, expanded: false }, renderTheme, { isError: false, expanded: false }));
  assert.match(resultText, /✓ Message sent to planner \(abcdef12\)/);

  const errorText = renderToText(parleyTool.renderResult({
    content: [{ type: "text", text: "Missing 'to' or 'message' parameter" }],
    details: { error: true, reason: "Missing target" },
  }, { isPartial: false, expanded: true }, renderTheme, { isError: false, expanded: true }));
  assert.match(errorText, /✗ Missing 'to' or 'message' parameter/);
  assert.match(errorText, /Reason: Missing target/);
});

test("parley tool result hook marks failed details as errors", async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness();
  piParleyExtension(harness.pi as never);

  const errorResults = await harness.emitLifecycleResults("tool_result", {
    toolName: "parley",
    details: { error: true },
  });
  assert.deepEqual(errorResults.filter(Boolean), [{ isError: true }]);

  const deliveryResults = await harness.emitLifecycleResults("tool_result", {
    toolName: "contact_supervisor",
    details: { delivered: false },
  });
  assert.deepEqual(deliveryResults.filter(Boolean), [{ isError: true }]);

  const invalidAnswerResults = await harness.emitLifecycleResults("tool_result", {
    toolName: "contact_supervisor",
    details: { error: true, delivered: false, outcomeKnown: false, replyMessageId: "received-invalid-answer" },
  });
  assert.deepEqual(invalidAnswerResults.filter(Boolean), [{ isError: true }], "a received answer does not erase an explicit structured-answer validation failure");

  const okResults = await harness.emitLifecycleResults("tool_result", {
    toolName: "parley",
    details: { delivered: true },
  });
  assert.deepEqual(okResults.filter(Boolean), []);
});

test("obsolete toolVisibility config never hides or reveals the parley tool", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");

  await withParleyConfig({ toolVisibility: "after-first-use" }, async () => {
    const { planner, cleanup } = await setupClients();
    let selectedSession: SessionInfo | undefined;
    let overlayStep = 0;
    const overlayHarness = createExtensionHarness("lazy-overlay-worker", {
      hasUI: true,
      activeTools: ["read"],
      ui: {
        notify: () => undefined,
        custom: async () => {
          overlayStep += 1;
          return overlayStep === 1
            ? selectedSession
            : { sent: true, messageId: "overlay-message", text: "Hello from the overlay" };
        },
      },
    });
    const inboundHarness = createExtensionHarness("lazy-inbound-worker", {
      hasUI: true,
      activeTools: ["read"],
    });

    try {
      piParleyExtension(overlayHarness.pi as never);
      piParleyExtension(inboundHarness.pi as never);
      await overlayHarness.emitLifecycle("session_start");
      await inboundHarness.emitLifecycle("session_start");
      assert.equal(overlayHarness.getActiveTools().includes("parley"), true);
      assert.equal(inboundHarness.getActiveTools().includes("parley"), true);

      selectedSession = await waitForSessionByName(planner, "planner");
      const inboundSession = await waitForSessionByName(planner, "lazy-inbound-worker");
      const delivered = await planner.send(inboundSession.id, {
        messageId: "stable-inbound-message",
        text: "Keep parley active before injecting this message.",
      });
      assert.equal(delivered.delivered, true);
      const deadline = Date.now() + 1000;
      while (inboundHarness.sentMessages.length === 0 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      assert.equal(inboundHarness.sentMessages[0]?.activeTools.includes("parley"), true);

      await inboundHarness.emitLifecycle("tool_result", {
        toolName: "read",
        input: { path: path.join(repoDir, "skills", "pi-parley", "SKILL.md") },
        isError: false,
      });
      assert.equal(inboundHarness.getActiveTools().includes("parley"), true);

      await overlayHarness.commands.get("parley")!("", overlayHarness.ctx);
      assert.equal(overlayStep, 2);
      assert.equal(overlayHarness.getActiveTools().includes("parley"), true);
    } finally {
      await overlayHarness.emitLifecycle("session_shutdown");
      await inboundHarness.emitLifecycle("session_shutdown");
      await cleanup();
    }
  });
});

test("contact supervisor tool renders reason and reply state", async () => {
  const { default: piParleyExtension } = await import("./index.ts");

  await withChildOrchestratorEnv({
    orchestratorTarget: "orchestrator",
    runId: "78f659a3",
    agent: "worker",
    index: "0",
  }, () => {
    const harness = createExtensionHarness();
    piParleyExtension(harness.pi as never);
    const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;

    assert.ok(supervisorTool.renderCall);
    assert.ok(supervisorTool.renderResult);
    assert.match(renderToText(supervisorTool.renderCall({
      reason: "interview_request",
      message: "Please answer these before I continue.",
      interview: { title: "API migration", questions: [] },
    }, renderTheme, {})), /contact_supervisor interview_request API migration\n {2}Please answer/);

    const warningText = renderToText(supervisorTool.renderResult({
      content: [{ type: "text", text: "Reply from supervisor:\nUse stable API" }],
      details: { structuredReplyParseError: "reply JSON must include a responses array" },
    }, { isPartial: false }, renderTheme, { isError: false }));
    assert.match(warningText, /⚠ Reply from supervisor:\nUse stable API/);
    assert.match(warningText, /Structured reply parse issue: reply JSON must include a responses array/);

    const failureText = renderToText(supervisorTool.renderResult({
      content: [{ type: "text", text: "Invalid reason" }],
      details: { error: true },
    }, { isPartial: false }, renderTheme, { isError: false }));
    assert.match(failureText, /✗ Invalid reason/);

    const invalidAnswer = renderToText(supervisorTool.renderResult({
      content: [{ type: "text", text: "The received answer could not be validated" }],
      details: { error: true, delivered: false, outcomeKnown: false, replyMessageId: "received-invalid-answer",
        structuredReplyParseError: "reply JSON must include a responses array" },
    }, { isPartial: false }, renderTheme, { isError: true }));
    assert.match(invalidAnswer, /✗ The received answer could not be validated/);
    assert.match(invalidAnswer, /Structured reply parse issue/);
  });
});

test("hosts without compaction failure events do not publish stale compaction presence", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("legacy-compaction-worker", { hasUI: true });
  const hostPackageDir = path.join(sharedHomeDir, "legacy-compaction-host", "node_modules", "@earendil-works", "pi-coding-agent");
  mkdirSync(hostPackageDir, { recursive: true });
  writeFileSync(path.join(hostPackageDir, "package.json"), JSON.stringify({
    name: "@earendil-works/pi-coding-agent",
    version: "0.84.9",
  }));
  const originalHostEntry = process.argv[1];

  try {
    process.argv[1] = path.join(hostPackageDir, "dist", "cli.js");
    try {
      piParleyExtension(harness.pi as never);
    } finally {
      process.argv[1] = originalHostEntry;
    }
    await harness.emitLifecycle("session_start");
    await waitForSessionStatus(planner, "legacy-compaction-worker", "idle");

    await harness.emitLifecycle("session_before_compact", {
      signal: new AbortController().signal,
      reason: "manual",
    });
    await new Promise((resolve) => setTimeout(resolve, 50));
    await waitForSessionStatus(planner, "legacy-compaction-worker", "idle");
  } finally {
    process.argv[1] = originalHostEntry;
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await cleanup();
  }
});

test("sessions publish automatic lifecycle status", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("status-worker", { hasUI: true });
  const hostPackageDir = path.join(sharedHomeDir, "compaction-host", "node_modules", "@earendil-works", "pi-coding-agent");
  mkdirSync(hostPackageDir, { recursive: true });
  writeFileSync(path.join(hostPackageDir, "package.json"), JSON.stringify({
    name: "@earendil-works/pi-coding-agent",
    version: "0.85.1",
  }));
  const originalHostEntry = process.argv[1];

  try {
    process.argv[1] = path.join(hostPackageDir, "dist", "cli.js");
    try {
      piParleyExtension(harness.pi as never);
    } finally {
      process.argv[1] = originalHostEntry;
    }
    await harness.emitLifecycle("session_start");

    await waitForSessionStatus(planner, "status-worker", "idle");

    const idleCompaction = new AbortController();
    await harness.emitLifecycle("session_before_compact", { signal: idleCompaction.signal, reason: "manual" });
    await waitForSessionStatus(planner, "status-worker", "compacting");
    await harness.emitLifecycle("session_compact", { reason: "manual" });
    await waitForSessionStatus(planner, "status-worker", "idle");
    const reportDeadline = Date.now() + 2_000;
    while (!harness.entries.some((entry) => entry.type === "parley_compaction_recorded") && Date.now() < reportDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    const pendingReport = harness.entries.find((entry) => entry.type === "parley_compaction_pending");
    const recordedReport = harness.entries.find((entry) => entry.type === "parley_compaction_recorded");
    assert.ok(pendingReport, "successful compaction is durably queued in the Pi session before reporting");
    assert.ok(recordedReport, "broker acknowledgement is persisted after durable generation advancement");
    assert.equal(
      (pendingReport.data as { eventId: string }).eventId,
      (recordedReport.data as { eventId: string }).eventId,
      "the persisted event ID correlates broker retries and acknowledgements",
    );

    const freshEventContext = {
      ...harness.ctx,
      model: { id: "fresh-model" },
      sessionManager: { getSessionId: () => "session-child-test" },
    };
    await harness.emitLifecycle("model_select", { model: { id: "fresh-model" } }, freshEventContext);
    await waitForSessionModel(planner, "status-worker", "fresh-model");

    await harness.emitLifecycle("agent_start");
    await waitForSessionStatus(planner, "status-worker", "thinking");

    const failedCompaction = new AbortController();
    await harness.emitLifecycle("session_before_compact", { signal: failedCompaction.signal, reason: "threshold" });
    await waitForSessionStatus(planner, "status-worker", "compacting");
    await harness.emitLifecycle("session_compact_failed", { reason: "threshold", errorMessage: "summary failed" });
    await waitForSessionStatus(planner, "status-worker", "thinking");

    await harness.emitLifecycle("tool_execution_start", { toolCallId: "tool-1", toolName: "bash" });
    await waitForSessionStatus(planner, "status-worker", "tool:bash");
    const toolCompaction = new AbortController();
    await harness.emitLifecycle("session_before_compact", { signal: toolCompaction.signal, reason: "overflow" });
    await waitForSessionStatus(planner, "status-worker", "compacting");
    toolCompaction.abort();
    await waitForSessionStatus(planner, "status-worker", "tool:bash");
    await harness.emitLifecycle("tool_execution_start", { toolCallId: "tool-2", toolName: "read" });

    await harness.emitLifecycle("tool_execution_end", { toolCallId: "tool-1", toolName: "bash" });
    await waitForSessionStatus(planner, "status-worker", "tool:read");

    await harness.emitLifecycle("tool_execution_end", { toolCallId: "tool-2", toolName: "read" });
    await waitForSessionStatus(planner, "status-worker", "thinking");

    assert.equal(
      harness.entries.filter((entry) => entry.type === "parley_compaction_recorded").length,
      1,
      "failed and aborted compactions must not advance the durable generation",
    );

    await harness.emitLifecycle("agent_end");
    await waitForSessionStatus(planner, "status-worker", "idle");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("session_info_changed propagates /name changes without other activity", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  let sessionName = "idle-name-before";
  const harness = createExtensionHarness(() => sessionName, { hasUI: true });

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "idle-name-before");
    sessionName = "idle-name-after";
    await harness.emitLifecycle("session_info_changed", {
      type: "session_info_changed",
      name: sessionName,
    });
    await waitForSessionByName(planner, "idle-name-after");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("any parley call can publish a durable short self description and returns self-profile metadata", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("profile-worker", { hasUI: true });

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley");
    assert.ok(parleyTool);

    const result = await parleyTool.execute("profile-status", {
      action: "status",
      profile: { name: "", description: "Hardening lightweight peer discovery and profiles" },
    }, new AbortController().signal, undefined, harness.ctx);

    assert.deepEqual(result.details?.selfProfile, {
      name: "profile-worker",
      description: "Hardening lightweight peer discovery and profiles",
      parleyName: "profile-worker",
      descriptionPublished: true,
    });
    assert.equal(
      result.content.at(-1)?.text,
      "Self profile: profile-worker — Hardening lightweight peer discovery and profiles",
    );
    const published = await waitForSessionDescription(planner, "profile-worker", "Hardening lightweight peer discovery and profiles");
    assert.equal(published.description, "Hardening lightweight peer discovery and profiles");
    const listed = await parleyTool.execute("profile-list", { action: "list" }, new AbortController().signal, undefined, harness.ctx);
    assert.match(listed.content[0]?.text ?? "", /profile-worker .*Hardening lightweight peer discovery and profiles/);
    assert.ok(harness.entries.some((entry) => entry.type === "parley_profile_updated"));

    await harness.emitLifecycle("session_shutdown");
    await harness.emitLifecycle("session_start");
    const restored = await waitForSessionDescription(planner, "profile-worker", "Hardening lightweight peer discovery and profiles");
    assert.equal(restored.description, "Hardening lightweight peer discovery and profiles");

    const cleared = await parleyTool.execute("profile-clear", {
      action: "status",
      profile: { description: null },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal((cleared.details?.selfProfile as { description?: string }).description, undefined);
    const clearedRoster = await waitForSessionDescription(planner, "profile-worker", undefined);
    assert.equal(clearedRoster.description, undefined);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("profile names fill unnamed sessions but never replace an explicit Pi name", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const explicit = createExtensionHarness("FlightDeck Owner", { hasUI: true, sessionId: "profile-explicit" });
  const unnamed = createExtensionHarness("", { hasUI: true, sessionId: "profile-unnamed" });

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(explicit.pi as never);
    piParleyExtension(unnamed.pi as never);
    await explicit.emitLifecycle("session_start");
    await unnamed.emitLifecycle("session_start");
    await waitForSessionByName(planner, "FlightDeck Owner");
    const explicitTool = explicit.tools.find((tool) => tool.name === "parley");
    const unnamedTool = unnamed.tools.find((tool) => tool.name === "parley");
    assert.ok(explicitTool);
    assert.ok(unnamedTool);

    const refused = await explicitTool.execute("profile-refused", {
      action: "status",
      profile: { name: "automatic-worker" },
    }, new AbortController().signal, undefined, explicit.ctx);
    assert.match(refused.content[0]?.text ?? "", /cannot replace the explicit session name "FlightDeck Owner"/);
    assert.equal(explicit.pi.getSessionName(), "FlightDeck Owner");

    const filled = await unnamedTool.execute("profile-filled", {
      action: "status",
      profile: { name: "FlightDeck Owner" },
    }, new AbortController().signal, undefined, unnamed.ctx);
    assert.deepEqual(filled.details?.selfProfile, {
      name: "FlightDeck Owner",
      parleyName: "FlightDeck Owner-2",
      descriptionPublished: true,
    });
    assert.equal(unnamed.pi.getSessionName(), "FlightDeck Owner");
    const stableProjection = await unnamedTool.execute("profile-projection-stable", {
      action: "pending",
    }, new AbortController().signal, undefined, unnamed.ctx);
    assert.equal((stableProjection.details?.selfProfile as { parleyName?: string }).parleyName, "FlightDeck Owner-2");

    await explicit.emitLifecycle("session_shutdown");
    const healedProjection = await unnamedTool.execute("profile-projection-healed", {
      action: "pending",
    }, new AbortController().signal, undefined, unnamed.ctx);
    assert.equal((healedProjection.details?.selfProfile as { parleyName?: string }).parleyName, "FlightDeck Owner");

    const updated = await unnamedTool.execute("profile-updated", {
      action: "status",
      profile: { name: "discovery-reviewer" },
    }, new AbortController().signal, undefined, unnamed.ctx);
    assert.equal((updated.details?.selfProfile as { name?: string }).name, "discovery-reviewer");
    assert.equal(unnamed.pi.getSessionName(), "discovery-reviewer");

    unnamed.pi.setSessionName("Manual Owner");
    await unnamed.emitLifecycle("session_info_changed", { type: "session_info_changed", name: "Manual Owner" });
    const afterManualRename = await unnamedTool.execute("profile-after-manual", {
      action: "status",
      profile: { name: "automatic-reviewer" },
    }, new AbortController().signal, undefined, unnamed.ctx);
    assert.match(afterManualRename.content[0]?.text ?? "", /cannot replace the explicit session name "Manual Owner"/);
    assert.equal(unnamed.pi.getSessionName(), "Manual Owner");
    assert.ok(unnamed.entries.some((entry) =>
      entry.type === "parley_profile_updated"
      && (entry.data as { managedName?: unknown }).managedName === null
    ));

    unnamed.pi.setSessionName("discovery-reviewer");
    await unnamed.emitLifecycle("session_info_changed", { type: "session_info_changed", name: "discovery-reviewer" });
    await unnamed.emitLifecycle("session_shutdown");
    await unnamed.emitLifecycle("session_start");
    const afterRestart = await unnamedTool.execute("profile-after-explicit-restore", {
      action: "status",
      profile: { name: "automatic-after-restart" },
    }, new AbortController().signal, undefined, unnamed.ctx);
    assert.match(afterRestart.content[0]?.text ?? "", /cannot replace the explicit session name "discovery-reviewer"/);
    assert.equal(unnamed.pi.getSessionName(), "discovery-reviewer");
  } finally {
    await explicit.emitLifecycle("session_shutdown");
    await unnamed.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("profile naming cannot diverge an advertised subagent from its broker identity", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const harness = createExtensionHarness("child-canonical", {
    hasUI: true,
    sessionId: "profile-advertised-child",
  });

  try {
    await withChildOrchestratorEnv({
      orchestratorTarget: "orchestrator",
      orchestratorSessionId: orchestrator.sessionId ?? undefined,
      runId: "profile-advertise-run",
      agent: "reviewer",
      index: "0",
    }, async () => {
      const { default: piParleyExtension } = await import("./index.ts");
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const parleyTool = harness.tools.find((tool) => tool.name === "parley");
      assert.ok(parleyTool);

      const advertised = await parleyTool.execute("profile-advertise", {
        action: "advertise",
        name: "public-reviewer",
      }, new AbortController().signal, undefined, harness.ctx);
      assert.match(advertised.content[0]?.text ?? "", /Advertised as \"public-reviewer\"/);

      const refused = await parleyTool.execute("profile-advertised-rename", {
        action: "status",
        profile: { name: "different-canonical" },
      }, new AbortController().signal, undefined, harness.ctx);
      assert.match(refused.content[0]?.text ?? "", /cannot rename an advertised subagent/);
      assert.equal(harness.pi.getSessionName(), "child-canonical");
      assert.equal((refused.details?.selfProfile as { parleyName?: string }).parleyName, "public-reviewer");
    });
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("profile persistence failures stay retryable without publishing an undurable description", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  let failNextAppend = true;
  const harness = createExtensionHarness("", {
    hasUI: true,
    sessionId: "profile-persistence-retry",
    appendEntryError: () => {
      if (!failNextAppend) return undefined;
      failNextAppend = false;
      return new Error("journal unavailable");
    },
  });

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley");
    assert.ok(parleyTool);

    const failed = await parleyTool.execute("profile-persist-failed", {
      action: "status",
      profile: {
        name: "retry-profile",
        description: "Testing durable profile journal retry behavior",
      },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(failed.content[0]?.text ?? "", /Unable to persist self profile.*journal unavailable/);
    assert.equal((failed.details?.selfProfile as { description?: string }).description, undefined);
    assert.equal(harness.pi.getSessionName(), "");
    assert.equal((await planner.listSessions()).some((session) => session.name === "retry-profile"), false);

    const retried = await parleyTool.execute("profile-persist-retried", {
      action: "status",
      profile: {
        name: "retry-profile",
        description: "Testing durable profile journal retry behavior",
      },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal((retried.details?.selfProfile as { description?: string }).description, "Testing durable profile journal retry behavior");
    assert.ok(harness.entries.some((entry) => entry.type === "parley_profile_updated"));

    const renamed = await parleyTool.execute("profile-persist-renamed", {
      action: "status",
      profile: { name: "retry-profile-renamed" },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal((renamed.details?.selfProfile as { name?: string }).name, "retry-profile-renamed");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("durable pending profiles recover name ownership after a commit append failure and restart", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  let appendCalls = 0;
  const harness = createExtensionHarness("", {
    hasUI: true,
    sessionId: "profile-pending-recovery",
    appendEntryError: () => {
      appendCalls += 1;
      return appendCalls === 2 ? new Error("commit journal unavailable") : undefined;
    },
  });

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley");
    assert.ok(parleyTool);

    const staged = await parleyTool.execute("profile-staged", {
      action: "status",
      profile: {
        name: "staged-profile",
        description: "Recovering durable staged profile ownership after restart",
      },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal((staged.details?.selfProfile as { name?: string }).name, "staged-profile");
    assert.ok(harness.entries.some((entry) => entry.type === "parley_profile_pending"));
    assert.equal(harness.entries.some((entry) => entry.type === "parley_profile_updated"), false);
    await waitForSessionDescription(planner, "staged-profile", "Recovering durable staged profile ownership after restart");

    await harness.emitLifecycle("session_shutdown");
    await harness.emitLifecycle("session_start");
    await waitForSessionDescription(planner, "staged-profile", "Recovering durable staged profile ownership after restart");

    const renamed = await parleyTool.execute("profile-recovered-rename", {
      action: "status",
      profile: { name: "staged-profile-renamed" },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal((renamed.details?.selfProfile as { name?: string }).name, "staged-profile-renamed");
    assert.equal(harness.pi.getSessionName(), "staged-profile-renamed");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("description publication is local-only when the negotiated profile capability is absent", { concurrency: false }, async () => {
  const originalSupportsFeature = ParleyClient.prototype.supportsFeature;
  ParleyClient.prototype.supportsFeature = function (feature: string) {
    if (feature === "session-profile-v1") return false;
    return originalSupportsFeature.call(this, feature);
  };
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("limited-profile-provider", { hasUI: true });

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "limited-profile-provider");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley");
    assert.ok(parleyTool);

    const result = await parleyTool.execute("profile-capability-unavailable", {
      action: "status",
      profile: { description: "Reviewing optional profile publication capability behavior" },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal((result.details?.selfProfile as { descriptionPublished?: boolean }).descriptionPublished, false);
    assert.match(result.content.at(-1)?.text ?? "", /description local only: broker does not support profiles/);
    const peerView = await waitForSessionDescription(planner, "limited-profile-provider", undefined);
    assert.equal(peerView.description, undefined);
  } finally {
    ParleyClient.prototype.supportsFeature = originalSupportsFeature;
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("profile descriptions enforce concise 5-9 word display metadata", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const harness = createExtensionHarness("bounded-profile", { hasUI: true });

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley");
    assert.ok(parleyTool);

    const tooShort = await parleyTool.execute("profile-short", {
      action: "status",
      profile: { description: "Reviewing profiles now" },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(tooShort.content[0]?.text ?? "", /must contain 5-9 words \(received 3\)/);
    assert.deepEqual(tooShort.details?.selfProfile, {
      name: "bounded-profile",
      descriptionPublished: true,
    });
    const unsafe = await parleyTool.execute("profile-control", {
      action: "status",
      profile: { description: "Reviewing peer\u001b[2J discovery profile behavior" },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(unsafe.content[0]?.text ?? "", /unsupported control characters/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("hosts without extension name events reconcile names through the compatibility fallback", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  let sessionName = "compat-name-before";
  const harness = createExtensionHarness(() => sessionName, { hasUI: true });

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "compat-name-before");

    // Upstream Pi 0.73.1 emits session_info_changed to RPC/TUI consumers but
    // does not forward it through ExtensionAPI.on(), so no lifecycle event is
    // delivered to this harness after the underlying name changes.
    sessionName = "compat-name-after";
    await waitForSessionByName(planner, "compat-name-after");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("non-live name events do not suppress compatibility reconciliation", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  let sessionName = "stale-event-name-before";
  const harness = createExtensionHarness(() => sessionName, { hasUI: true });

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "stale-event-name-before");

    sessionName = "stale-event-name-after";
    const staleContext = {
      ...harness.ctx,
      sessionManager: { getSessionId: () => "different-session" },
    };
    await harness.emitLifecycle("session_info_changed", {
      type: "session_info_changed",
      name: sessionName,
    }, staleContext);

    await waitForSessionByName(planner, "stale-event-name-after");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("name changes during registration are replayed after the broker ACK", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  let sessionName = "handshake-name-before";
  const harness = createExtensionHarness(() => sessionName, { hasUI: true });
  const originalConnect = ParleyClient.prototype.connect;
  let connectEntered!: () => void;
  const entered = new Promise<void>((resolve) => { connectEntered = resolve; });
  let releaseConnect!: () => void;
  const mayConnect = new Promise<void>((resolve) => { releaseConnect = resolve; });

  ParleyClient.prototype.connect = function (session, sessionId) {
    connectEntered();
    return mayConnect.then(() => originalConnect.call(this, session, sessionId));
  };

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    const starting = harness.emitLifecycle("session_start");
    await entered;

    // Registration already captured the old name, but the client has no broker
    // session id yet, so this event's immediate updatePresence is a no-op.
    sessionName = "handshake-name-after";
    await harness.emitLifecycle("session_info_changed", {
      type: "session_info_changed",
      name: sessionName,
    });
    releaseConnect();
    await starting;

    await waitForSessionByName(planner, "handshake-name-after");
  } finally {
    ParleyClient.prototype.connect = originalConnect;
    releaseConnect();
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("turn_start re-registers when Pi replaces the session context", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  let sessionName = "fork-before";
  let sessionId = "session-fork-before";
  const harness = createExtensionHarness(() => sessionName, { hasUI: true, sessionId: () => sessionId });

  try {
    const { default: piParleyExtension } = await import("./index.ts");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionId(planner, "session-fork-before");

    sessionName = "fork-after";
    sessionId = "session-fork-after";
    await harness.emitLifecycle("turn_start");
    const replaced = await waitForSessionId(planner, "session-fork-after");
    assert.equal(replaced.name, "fork-after");
    await waitForNoSessionId(planner, "session-fork-before");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("busy interactive sessions steer top-level asks without aborting", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let abortCount = 0;
  let idle = false;
  const harness = createExtensionHarness("interactive-worker", {
    abort: () => { abortCount += 1; },
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const target = await waitForSessionByName(planner, "interactive-worker");

    const delivered = await planner.send(target.id, {
      messageId: 'interactive-busy-"ask',
      text: "Can you respond after your current turn?",
      expectsReply: true,
    });
    assert.equal(delivered.delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(abortCount, 0);
    assert.equal(harness.sentMessages.length, 1);
    assert.equal(harness.sentMessages[0]?.message.customType, "parley_message");
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");
    assert.match(harness.sentMessages[0]?.message.content ?? "", /Can you respond after your current turn/);
    assert.ok((harness.sentMessages[0]?.message.content ?? "").includes('Message: interactive-busy-"ask'));

    await harness.emitLifecycle("turn_end");
    assert.equal(harness.sentMessages.length, 1, "turn end must not inject the steered message again");

    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));

    assert.equal(abortCount, 0);
    assert.equal(harness.sentMessages.length, 1, "agent end must not inject the steered message again");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("idle interactive sessions trigger a new turn immediately", { concurrency: false }, async () => {
	const { default: piParleyExtension } = await import("./index.ts");
	const { planner, cleanup } = await setupClients();
	const harness = createExtensionHarness("idle-trigger-worker", {
		hasUI: true,
		isIdle: () => true,
	});

	try {
		piParleyExtension(harness.pi as never);
		await harness.emitLifecycle("session_start");
		const worker = await waitForSessionByName(planner, "idle-trigger-worker");

		assert.equal((await planner.send(worker.id, { messageId: "idle-trigger", text: "Handle this now" })).delivered, true);
		await new Promise((resolve) => setTimeout(resolve, 20));

		assert.equal(harness.sentMessages.length, 1);
		assert.equal(harness.sentMessages[0]?.options?.triggerTurn, true);
		assert.equal(harness.sentMessages[0]?.options?.deliverAs, undefined);
	} finally {
		await harness.emitLifecycle("session_shutdown");
		await cleanup();
	}
});

test("broker rejects changed duplicate message IDs and replays identical sends without reinjection", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("dedupe-worker", { hasUI: true });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "dedupe-worker");
    const receipts: string[] = [];
    const unsubscribeReceipts = planner.onMessageReceipt((_from, receipt) => {
      if (receipt.messageId === "duplicate-inbound") receipts.push(receipt.detail ? `${receipt.status}:${receipt.detail}` : receipt.status);
    });

    try {
      const first = await planner.send(worker.id, { messageId: "duplicate-inbound", text: "First copy" });
      const changed = await planner.send(worker.id, { messageId: "duplicate-inbound", text: "Second copy" });
      const replay = await planner.send(worker.id, { messageId: "duplicate-inbound", text: "First copy" });
      assert.equal(first.delivered, true);
      assert.equal(changed.delivered, false);
      assert.equal(changed.code, "E_MESSAGE_ID_REUSE");
      assert.equal(replay.delivered, true);
      await new Promise((resolve) => setTimeout(resolve, 100));
    } finally {
      unsubscribeReceipts();
    }

    assert.equal(harness.sentMessages.length, 1);
    assert.ok(receipts.includes("receiver_received"));
    assert.ok(receipts.includes("acknowledged:accepted by receiver"));
    assert.ok(receipts.some((receipt) => receipt.startsWith("injected:")));
    const sent = harness.sentMessages[0]!;
    assert.match(sent.message.content ?? "", /Message: duplicate-inbound/);
    assert.doesNotMatch(sent.message.content ?? "", /seq 1|broker delivered|receiver received|injected/);
    const details = sent.message.details as { message?: Message };
    assert.equal(details.message?.id, "duplicate-inbound");
    assert.equal(details.message?.senderSequence, 1);
    assert.equal(typeof details.message?.brokerReceivedAt, "number");
    assert.equal(typeof details.message?.brokerDeliveredAt, "number");
    assert.equal(typeof details.message?.receiverReceivedAt, "number");
    assert.equal(typeof details.message?.injectedAt, "number");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("busy interactive sessions steer same-sender messages in sequence order", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const harness = createExtensionHarness("sequence-worker", {
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "sequence-worker");
    const receipts = new Map<string, string[]>();
    const unsubscribeReceipts = planner.onMessageReceipt((_from, receipt) => {
      const statuses = receipts.get(receipt.messageId) ?? [];
      statuses.push(receipt.status);
      receipts.set(receipt.messageId, statuses);
    });

    assert.equal((await planner.send(worker.id, { messageId: "sequence-1", text: "First steered message" })).delivered, true);
    assert.equal((await planner.send(worker.id, { messageId: "sequence-2", text: "Second steered message" })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 2);
    assert.match(harness.sentMessages[0]?.message.content ?? "", /First steered message/);
    assert.match(harness.sentMessages[1]?.message.content ?? "", /Second steered message/);
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");
    assert.equal(harness.sentMessages[1]?.options?.deliverAs, "steer");

    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 2, "steered messages must not be injected again at idle");
    const firstDetails = harness.sentMessages[0]?.message.details as { message?: Message } | undefined;
    const secondDetails = harness.sentMessages[1]?.message.details as { message?: Message } | undefined;
    assert.equal(firstDetails?.message?.senderSequence, 1);
    assert.equal(secondDetails?.message?.senderSequence, 2);
    assert.deepEqual(receipts.get("sequence-1"), ["receiver_received", "acknowledged", "injected"]);
    assert.deepEqual(receipts.get("sequence-2"), ["receiver_received", "acknowledged", "injected"]);
    unsubscribeReceipts();
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("explicit cancel acknowledges that a steered inbound message may already be processed", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const harness = createExtensionHarness("cancel-worker", {
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "cancel-worker");
    const receipts: string[] = [];
    const unsubscribeReceipts = planner.onMessageReceipt((_from, receipt) => {
      if (receipt.messageId === "cancel-steered") receipts.push(receipt.status);
    });

    assert.equal((await planner.send(worker.id, { messageId: "cancel-steered", text: "Cancel after steering", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 1);
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");
    assert.equal((await planner.cancelMessage("cancel-steered")).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));

    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));

    assert.equal(harness.sentMessages.length, 2);
    assert.match(harness.sentMessages[1]?.message.content ?? "", /cancel-steered.*withdrawn by its sender/);
    assert.deepEqual(receipts, ["receiver_received", "acknowledged", "injected", "cancellation_requested"]);
    unsubscribeReceipts();
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("parley cancel action requests cancellation for a sent message", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const senderHarness = createExtensionHarness("cancel-sender", { sessionId: "session-cancel-sender" });
  const receiverHarness = createExtensionHarness("cancel-tool-worker", {
    hasUI: true,
    isIdle: () => idle,
    sessionId: "session-cancel-tool-worker",
  });

  try {
    piParleyExtension(senderHarness.pi as never);
    piParleyExtension(receiverHarness.pi as never);
    await senderHarness.emitLifecycle("session_start");
    await receiverHarness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "cancel-tool-worker");
    const parleyTool = senderHarness.tools.find((tool) => tool.name === "parley")!;

    const sendResult = await parleyTool.execute("send-before-cancel", { action: "send", to: "cancel-tool-worker", message: "Cancel this through the tool" }, new AbortController().signal, undefined, senderHarness.ctx);
    const messageId = String(sendResult.details?.messageId);
    assert.equal(sendResult.details?.delivered, true);
    assert.notEqual(messageId, "undefined");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(receiverHarness.sentMessages.length, 1);
    assert.equal(receiverHarness.sentMessages[0]?.options?.deliverAs, "steer");

    const cancelResult = await parleyTool.execute("cancel-message", { action: "cancel", messageId }, new AbortController().signal, undefined, senderHarness.ctx);
    assert.equal(cancelResult.details?.delivered, true);
    assert.match(cancelResult.content[0]?.text ?? "", /Withdrawal requested/);

    idle = true;
    await receiverHarness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(receiverHarness.sentMessages.length, 2);
    assert.ok(receiverHarness.sentMessages[1]?.message.content?.includes(messageId));
    assert.match(receiverHarness.sentMessages[1]?.message.content ?? "", /withdrawn by its sender/);
  } finally {
    await senderHarness.emitLifecycle("session_shutdown");
    await receiverHarness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("same-sender supersede reports an already-steered inbound message", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const harness = createExtensionHarness("supersede-worker", {
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "supersede-worker");
    const receipts = new Map<string, string[]>();
    const unsubscribeReceipts = planner.onMessageReceipt((_from, receipt) => {
      const statuses = receipts.get(receipt.messageId) ?? [];
      statuses.push(receipt.status);
      receipts.set(receipt.messageId, statuses);
    });

    assert.equal((await planner.send(worker.id, { messageId: "superseded-message", text: "Old steered message", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    const replacement = await planner.send(worker.id, { messageId: "replacement-message", text: "Replacement message", supersedes: "superseded-message" });
    assert.equal(replacement.delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));

    assert.equal(harness.sentMessages.length, 3);
    assert.match(harness.sentMessages[0]?.message.content ?? "", /Old steered message/);
    assert.match(harness.sentMessages[1]?.message.content ?? "", /superseded by replacement-message/);
    assert.match(harness.sentMessages[2]?.message.content ?? "", /Replacement message/);
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");
    assert.equal(harness.sentMessages[1]?.options?.deliverAs, "steer");
    const details = harness.sentMessages[2]?.message.details as { message?: Message } | undefined;
    assert.equal(details?.message?.supersedes, "superseded-message");

    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 3);
    assert.deepEqual(receipts.get("superseded-message"), ["receiver_received", "acknowledged", "injected", "superseded"]);
    assert.deepEqual(receipts.get("replacement-message"), ["receiver_received", "acknowledged", "injected"]);
    unsubscribeReceipts();
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("supersede is scoped to the same sender and receiver", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, orchestrator, cleanup } = await setupClients();
  const idle = false;
  const firstHarness = createExtensionHarness("supersede-first", {
    hasUI: true,
    isIdle: () => idle,
  });
  const secondHarness = createExtensionHarness("supersede-second", {
    hasUI: true,
    isIdle: () => idle,
    sessionId: "session-supersede-second",
  });

  try {
    piParleyExtension(firstHarness.pi as never);
    piParleyExtension(secondHarness.pi as never);
    await firstHarness.emitLifecycle("session_start");
    await secondHarness.emitLifecycle("session_start");
    const first = await waitForSessionByName(planner, "supersede-first");
    const second = await waitForSessionByName(planner, "supersede-second");

    assert.equal((await planner.send(first.id, { messageId: "wrong-target-old", text: "Old target" })).delivered, true);
    const wrongReceiver = await planner.send(second.id, { messageId: "wrong-target-new", text: "Wrong receiver", supersedes: "wrong-target-old" });
    assert.equal(wrongReceiver.delivered, false);
    assert.match(wrongReceiver.reason ?? "", /same sender and receiver|previous message/);

    const wrongSender = await orchestrator.send(first.id, { messageId: "wrong-sender-new", text: "Wrong sender", supersedes: "wrong-target-old" });
    assert.equal(wrongSender.delivered, false);
    assert.match(wrongSender.reason ?? "", /same sender and receiver|previous message/);
  } finally {
    await firstHarness.emitLifecycle("session_shutdown");
    await secondHarness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("replied steered asks are not injected again after the current turn", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const harness = createExtensionHarness("reply-while-busy-worker", {
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "reply-while-busy-worker");

    const askId = "reply-while-busy-ask";
    const replyReceived = waitForReply(planner, askId);
    assert.equal((await planner.send(worker.id, {
      messageId: askId,
      text: "Can you answer before this turn ends?",
      expectsReply: true,
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 1);
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");

    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const result = await parleyTool.execute("reply-while-busy", {
      action: "reply",
      message: "Answered during the current turn.",
      replyTo: askId,
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(result.details?.delivered, true);
    assert.equal((await replyReceived).message.replyTo, askId);

    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 1);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("deferred startup connect is cancelled on shutdown", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("shutdown-before-start", { hasUI: true });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await harness.emitLifecycle("session_shutdown");
    await new Promise((resolve) => setTimeout(resolve, 50));

    const sessions = await planner.listSessions();
    assert.equal(sessions.some((session) => session.name === "shutdown-before-start"), false);
  } finally {
    await cleanup();
  }
});

test("stale overlay work stops after same-session restart", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let customCalls = 0;
  let resolveFirstCustom: ((value: unknown) => void) | undefined;
  const ui = {
    notify: () => undefined,
    custom: async () => {
      customCalls += 1;
      if (customCalls > 1) {
        return { sent: false };
      }
      return new Promise((resolve) => {
        resolveFirstCustom = resolve;
      });
    },
  };
  const harness = createExtensionHarness("overlay-worker", { hasUI: true, ui });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "overlay-worker");

    const overlayPromise = Promise.resolve(harness.commands.get("parley")!("", harness.ctx));
    const deadline = Date.now() + 2000;
    while (!resolveFirstCustom && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    assert.ok(resolveFirstCustom, "overlay should reach the session picker");

    const plannerSession = await waitForSessionByName(planner, "planner");
    await harness.emitLifecycle("session_shutdown");
    await harness.emitLifecycle("session_start");
    resolveFirstCustom(plannerSession);
    await overlayPromise;

    assert.equal(customCalls, 1);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("steered inbound messages are not reinjected after shutdown", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const harness = createExtensionHarness("disposed-worker", {
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const target = await waitForSessionByName(planner, "disposed-worker");
    const receipts: string[] = [];
    const unsubscribeReceipts = planner.onMessageReceipt((_from, receipt) => {
      if (receipt.messageId === "disposed-ask") receipts.push(receipt.status);
    });

    const delivered = await planner.send(target.id, {
      messageId: "disposed-ask",
      text: "This should be steered before shutdown.",
      expectsReply: true,
    });
    assert.equal(delivered.delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 1);
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");

    await harness.emitLifecycle("session_shutdown");
    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));

    assert.equal(harness.sentMessages.length, 1);
    assert.deepEqual(receipts, ["receiver_received", "acknowledged", "injected"]);
    unsubscribeReceipts();
  } finally {
    await cleanup();
  }
});

test("busy non-interactive sessions steer top-level asks without aborting", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let abortCount = 0;
  const harness = createExtensionHarness("pipe-worker", {
    abort: () => { abortCount += 1; },
    hasUI: false,
    isIdle: () => false,
  });

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const target = await waitForSessionByName(planner, "pipe-worker");

    const askId = "pipe-mode-ask";
    const unexpectedReplies: Message[] = [];
    planner.on("message", (_from, message) => unexpectedReplies.push(message));
    const delivered = await planner.send(target.id, {
      messageId: askId,
      text: "Can you respond while busy?",
      expectsReply: true,
    });
    assert.equal(delivered.delivered, true);

    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(unexpectedReplies.length, 0, "the extension must not invent an answer for a busy colleague");
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");
    assert.match(harness.sentMessages[0]?.message.content ?? "", /Can you respond while busy/);
    assert.equal(abortCount, 0);

  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("Parley journal replay and model context ignore unrelated custom entry types", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("journal-worker");
  const rendererTypes: string[] = [];
  harness.pi.registerMessageRenderer = ((type: string) => { rendererTypes.push(type); }) as typeof harness.pi.registerMessageRenderer;
  const envelope = (customType: string, id: string) => ({
    customType, content: "Journal context", details: {
      from: { id: "peer-1", cwd: repoDir, model: "m", pid: 1, startedAt: 0, lastActivity: 0 },
      message: { id, timestamp: 1, content: { text: "Journal context" }, contactBaseline: true, contactToken: id },
    },
  });
  // The current entry comes last: its acknowledgement is a barrier after any
  // acknowledgements accidentally issued for the foreign journal entries.
  harness.persistedMessages.push(
    envelope("unrelated_message", "foreign-token"),
    envelope("other_message", "other-token"),
    envelope("parley_message", "current-token"),
  );
  try {
    piParleyExtension(harness.pi as never);
    assert.deepEqual(rendererTypes, ["parley_message"]);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "journal-worker");
    const deadline = Date.now() + 2_000;
    while (!harness.entries.some((entry) => entry.type === "parley_receiver_baseline_abandoned"
      && (entry.data as { token?: string }).token === "current-token") && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.deepEqual(harness.entries.filter((entry) => entry.type === "parley_receiver_baseline_abandoned")
      .map((entry) => (entry.data as { token: string }).token), ["current-token"]);

    const current = { role: "custom", ...envelope("parley_message", "context-id"), timestamp: 999 };
    const unrelated = { role: "custom", ...envelope("unrelated_message", "context-id") };
    const other = { role: "custom", ...envelope("other_message", "context-id") };
    const foreignNotice = { role: "custom", customType: "unrelated_persistence_notice", content: "Foreign notice" };
    const [result] = await harness.emitLifecycleResults("context", {
      messages: [current, current, unrelated, unrelated, other, other, foreignNotice],
    });
    assert.deepEqual((result as { messages: unknown[] }).messages,
      [{ ...current, timestamp: 1 }, unrelated, unrelated, other, other, foreignNotice],
      "only Parley messages are deduplicated and normalized to arrival timing; foreign entries remain untouched");
  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await cleanup();
  }
});

test("supervisor tool registers only when child metadata is present", async () => {
  const { default: piParleyExtension } = await import("./index.ts");

  await withChildOrchestratorEnv({}, () => {
    const harness = createExtensionHarness();
    piParleyExtension(harness.pi as never);
    assert.deepEqual(harness.tools.map((tool) => tool.name), ["parley"]);
  });

  await withChildOrchestratorEnv({
    orchestratorTarget: "orchestrator",
    runId: "78f659a3",
    agent: "worker",
    index: "0",
    sessionName: "subagent-worker-78f659a3-1",
  }, () => {
    const harness = createExtensionHarness();
    piParleyExtension(harness.pi as never);
    assert.deepEqual(harness.tools.map((tool) => tool.name), ["contact_supervisor", "parley"]);
    const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor");
    assert.match(JSON.stringify(supervisorTool?.parameters), /interview_request/);
    assert.match(JSON.stringify(supervisorTool?.parameters), /questions/);
  });

  await withChildOrchestratorEnv({
    orchestratorTarget: "orchestrator",
    runId: "78f659a3",
    agent: "worker",
    index: "0",
    supervisorChannelDir: path.join(sharedHomeDir, "native-supervisor-channel"),
  }, () => {
    const harness = createExtensionHarness();
    piParleyExtension(harness.pi as never);
    assert.deepEqual(harness.tools.map((tool) => tool.name), ["parley"]);
  });
});

test("child supervisor tool resolves target and includes run metadata", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { orchestrator, cleanup } = await setupClients();

  try {
    await withChildOrchestratorEnv({
      orchestratorTarget: "orchestrator",
      runId: "78f659a3",
      agent: "worker",
      index: "0",
      sessionName: "subagent-worker-78f659a3-1",
    }, async () => {
      const harness = createExtensionHarness("subagent-worker-78f659a3-1");
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");

      const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;

      const askReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const askResultPromise = supervisorTool.execute("ask-1", { reason: "need_decision", message: "Which API should I use?" }, new AbortController().signal, undefined, harness.ctx);
      const [askFrom, askMessage] = await askReceived;
      assert.equal(askMessage.expectsReply, true);
      assert.match(askMessage.content.text, /Subagent needs a supervisor decision/);
      assert.match(askMessage.content.text, /Run: 78f659a3/);
      assert.match(askMessage.content.text, /Agent: worker/);
      assert.match(askMessage.content.text, /Child index: 0/);
      assert.match(askMessage.content.text, /Which API should I use\?/);

      const replyCompaction = await orchestrator.reportCompactionCompleted();
      const reply = await orchestrator.send(askFrom.id, { text: "Use the stable API.", replyTo: askMessage.id });
      assert.equal(reply.delivered, true);
      const askResult = await askResultPromise;
      assert.notEqual(askResult.details?.error, true);
      assert.match(askResult.content[0]?.text ?? "", /Use the stable API/);
      assert.match(askResult.content[0]?.text ?? "", /orchestrator compacted context since your last direct contact/i);
      assert.equal(
        (askResult.details?.replyPeerCompaction as { generation?: number } | undefined)?.generation,
        replyCompaction.generation,
      );

      const supervisorCompaction = await orchestrator.reportCompactionCompleted();
      const updateReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const updateResult = await supervisorTool.execute("update-1", { reason: "progress_update", message: "Found a schema mismatch." }, new AbortController().signal, undefined, harness.ctx);
      const [_updateFrom, updateMessage] = await updateReceived;
      assert.equal(updateMessage.expectsReply, undefined);
      assert.match(updateMessage.content.text, /Subagent progress update/);
      assert.match(updateMessage.content.text, /Run: 78f659a3/);
      assert.match(updateMessage.content.text, /Agent: worker/);
      assert.match(updateMessage.content.text, /Found a schema mismatch/);
      assert.notEqual(updateResult.details?.error, true);
      assert.match(updateResult.content[0]?.text ?? "", /orchestrator compacted context since your last direct contact/i);
      assert.equal(
        (updateResult.details?.peerCompaction as { generation?: number } | undefined)?.generation,
        supervisorCompaction.generation,
      );

      const interviewReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const interview = {
        title: "API migration choices",
        description: "Choose the implementation path before edits continue.",
        questions: [
          { id: "context", type: "info", question: "Migration context", context: "Use the existing auth boundary." },
          { id: "api", type: "single", question: "Which API should I target?", options: [" Stable API ", "Experimental API"] },
          { id: "notes", type: "text", question: "Any constraints to preserve?" },
        ],
      };
      const interviewResultPromise = supervisorTool.execute("interview-1", {
        reason: "interview_request",
        message: "Please answer both so I can continue safely.",
        interview,
      }, new AbortController().signal, undefined, harness.ctx);
      const [interviewFrom, interviewMessage] = await interviewReceived;
      assert.equal(interviewMessage.expectsReply, true);
      assert.match(interviewMessage.content.text, /Subagent requests a structured supervisor interview/);
      assert.match(interviewMessage.content.text, /Interview: API migration choices/);
      assert.match(interviewMessage.content.text, /\[context\] \(info\) Migration context/);
      assert.match(interviewMessage.content.text, /\[api\] \(single\) Which API should I target\?/);
      assert.match(interviewMessage.content.text, / {3}- Stable API/);
      assert.match(interviewMessage.content.text, /\[notes\] \(text\) Any constraints to preserve\?/);
      assert.match(interviewMessage.content.text, /"responses"/);
      assert.doesNotMatch(interviewMessage.content.text, /"id": "context"/);

      const structuredReply = {
        responses: [
          { id: "api", value: "Stable API" },
          { id: "notes", value: "Keep the public error shape unchanged." },
        ],
      };
      const interviewReply = await orchestrator.send(interviewFrom.id, {
        text: `\`\`\`json\n${JSON.stringify(structuredReply, null, 2)}\n\`\`\``,
        replyTo: interviewMessage.id,
      });
      assert.equal(interviewReply.delivered, true);
      const interviewResult = await interviewResultPromise;
      assert.notEqual(interviewResult.details?.error, true);
      assert.match(interviewResult.content[0]?.text ?? "", /Stable API/);
      assert.deepEqual(interviewResult.details?.structuredReply, structuredReply);

      const invalidReplyReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const invalidReplyResultPromise = supervisorTool.execute("interview-invalid-reply", {
        reason: "interview_request",
        interview,
      }, new AbortController().signal, undefined, harness.ctx);
      const [invalidReplyFrom, invalidReplyMessage] = await invalidReplyReceived;
      const invalidReply = await orchestrator.send(invalidReplyFrom.id, {
        text: '{"responses":[{"id":"api","value":"Removed API"}]}',
        replyTo: invalidReplyMessage.id,
      });
      assert.equal(invalidReply.delivered, true);
      const invalidReplyResult = await invalidReplyResultPromise;
      assert.equal(invalidReplyResult.details?.error, true);
      assert.equal(invalidReplyResult.details?.structuredReply, undefined);
      assert.match(modelText(invalidReplyResult), /must match one of the question options/);
      assert.match(modelText(invalidReplyResult), /Removed API/, "the invalid answer is preserved for recovery, not reported as a successful interview");

      await harness.emitLifecycle("session_shutdown");
    });
  } finally {
    await cleanup();
  }
});

test("child supervisor tool uses stable supervisor ID when names are duplicated", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const duplicate = new ParleyClient();

  try {
    await duplicate.connect({
      name: "orchestrator",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, "duplicate-orchestrator-id");

    await withChildOrchestratorEnv({
      orchestratorTarget: "orchestrator",
      orchestratorSessionId: orchestrator.sessionId!,
      runId: "78f659a3",
      agent: "worker",
      index: "0",
    }, async () => {
      const { default: piParleyExtension } = await import("./index.ts");
      const harness = createExtensionHarness("duplicate-name-child");
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;

      const received = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const result = await supervisorTool.execute("update-duplicate", { reason: "progress_update", message: "Stable ID route." }, new AbortController().signal, undefined, harness.ctx);
      const [, message] = await received;
      assert.notEqual(result.details?.error, true);
      assert.match(message.content.text, /Stable ID route/);
      await harness.emitLifecycle("session_shutdown");
    });
  } finally {
    await duplicate.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("child supervisor tool rejects invalid reasons and interview payloads", async () => {
  const { default: piParleyExtension } = await import("./index.ts");

  await withChildOrchestratorEnv({
    orchestratorTarget: "orchestrator",
    runId: "78f659a3",
    agent: "worker",
    index: "0",
  }, async () => {
    const harness = createExtensionHarness();
    piParleyExtension(harness.pi as never);
    const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;
    const result = await supervisorTool.execute("invalid-1", { reason: "done", message: "Finished." }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(result.details?.error, true);
    assert.match(result.content[0]?.text ?? "", /Invalid reason/);

    const missingMessageResult = await supervisorTool.execute("invalid-message", { reason: "need_decision" }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(missingMessageResult.details?.error, true);
    assert.match(missingMessageResult.content[0]?.text ?? "", /Missing 'message'/);

    const invalidInterviewResult = await supervisorTool.execute("invalid-interview", { reason: "interview_request", interview: { title: "Bad" } }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(invalidInterviewResult.details?.error, true);
    assert.match(invalidInterviewResult.content[0]?.text ?? "", /interview\.questions must be a non-empty array/);

    const invalidInfoOptionsResult = await supervisorTool.execute("invalid-info-options", {
      reason: "interview_request",
      interview: {
        questions: [{ id: "context", type: "info", question: "Context", options: ["Not an answer"] }],
      },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(invalidInfoOptionsResult.details?.error, true);
    assert.match(invalidInfoOptionsResult.content[0]?.text ?? "", /options is only valid for single and multi questions/);
  });
});

test("child supervisor blocking requests fail fast when the supervisor is disconnected", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { cleanup } = await setupClients();

  try {
    await withChildOrchestratorEnv({
      orchestratorTarget: "missing-orchestrator",
      runId: "78f659a3",
      agent: "worker",
      index: "0",
    }, async () => {
      const harness = createExtensionHarness();
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;
      const updateResult = await supervisorTool.execute("update-1", { reason: "progress_update", message: "Blocked." }, new AbortController().signal, undefined, harness.ctx);
      assert.equal(updateResult.details?.delivered, false);
      assert.match(updateResult.content[0]?.text ?? "", /Session not found/);
      assert.equal(updateResult.details?.reason, "Session not found");

      const askResult = await supervisorTool.execute("ask-1", { reason: "need_decision", message: "Which path?" }, new AbortController().signal, undefined, harness.ctx);
      assert.equal(askResult.details?.error, true);
      assert.match(askResult.content[0]?.text ?? "", /not (?:currently )?connected/);

      const secondAskResult = await supervisorTool.execute("ask-2", { reason: "need_decision", message: "Still blocked." }, new AbortController().signal, undefined, harness.ctx);
      assert.equal(secondAskResult.details?.error, true);
      assert.match(secondAskResult.content[0]?.text ?? "", /not (?:currently )?connected/);
      assert.doesNotMatch(secondAskResult.content[0]?.text ?? "", /Already waiting/);
      await harness.emitLifecycle("session_shutdown");
    });
  } finally {
    await cleanup();
  }
});

test("regular parley asks fail safely when started concurrently", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { orchestrator, cleanup } = await setupClients();

  try {
    const harness = createExtensionHarness("regular-ask-worker");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(orchestrator, "regular-ask-worker");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;

    const firstMessage = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const firstAsk = parleyTool.execute("ask-1", { action: "ask", to: "orchestrator", message: "First?" }, new AbortController().signal, undefined, harness.ctx);
    const secondAsk = parleyTool.execute("ask-2", { action: "ask", to: "orchestrator", message: "Second?" }, new AbortController().signal, undefined, harness.ctx);
    const [from, askMessage] = await firstMessage;
    assert.equal(askMessage.expectsReply, true);

    const earlyResults = await Promise.race([
      Promise.all([firstAsk, secondAsk]),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 100)),
    ]);
    assert.equal(earlyResults, null);

    const pendingResult = await Promise.race([firstAsk, secondAsk]);
    assert.equal(pendingResult.details?.error, true);
    assert.match(pendingResult.content[0]?.text ?? "", /Already waiting/);

    const reply = await orchestrator.send(from.id, { text: "First answer.", replyTo: askMessage.id });
    assert.equal(reply.delivered, true);

    const results = await Promise.all([firstAsk, secondAsk]);
    assert.equal(results.filter((result) => result.details?.error === true).length, 1);
    assert.equal(results.filter((result) => /First answer/.test(result.content[0]?.text ?? "")).length, 1);
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await cleanup();
  }
});

test("broker writes a local pending ask record for delivered blocking asks", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const askId = "pending-record-live-ask";

  try {
    const result = await planner.send(orchestrator.sessionId!, {
      messageId: askId,
      text: "Can you decide?",
      expectsReply: true,
    });

    assert.equal(result.delivered, true);
    const record = readPendingAskRecord(askId);
    assert.equal(record.askId, askId);
    assert.equal(record.messageId, askId);
    assert.deepEqual(record.asker, { sessionId: planner.sessionId, name: "planner" });
    assert.deepEqual(record.target, { sessionId: orchestrator.sessionId, name: "orchestrator" });
    assert.equal(record.question, "Can you decide?");
    assert.equal(Number(record.expiresAt) - Number(record.createdAt), getAskTimeoutMs());
    if (process.platform !== "win32") {
      assert.equal(statSync(pendingAskRecordPath(askId)).mode & 0o777, 0o600);
    }
  } finally {
    await cleanup();
  }
});

test("broker removes a pending ask record after a delivered reply", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const askId = "pending-record-replied-ask";

  try {
    assert.equal((await planner.send(orchestrator.sessionId!, {
      messageId: askId,
      text: "Can you answer?",
      expectsReply: true,
    })).delivered, true);
    assert.equal(existsSync(pendingAskRecordPath(askId)), true);

    assert.equal((await orchestrator.send(planner.sessionId!, {
      text: "Answered.",
      replyTo: askId,
    })).delivered, true);

    assert.equal(existsSync(pendingAskRecordPath(askId)), false);
  } finally {
    await cleanup();
  }
});

test("broker removes a pending ask record after asker cancellation", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const askId = "pending-record-cancelled-ask";

  try {
    assert.equal((await planner.send(orchestrator.sessionId!, {
      messageId: askId,
      text: "Should I stop?",
      expectsReply: true,
    })).delivered, true);
    assert.equal(existsSync(pendingAskRecordPath(askId)), true);

    planner.cancelAsk(askId);
    await waitForPendingAskRecordRemoved(askId);
  } finally {
    await cleanup();
  }
});

test("broker removes a pending ask record during timeout pruning", { concurrency: false }, async () => {
  const previousTimeout = process.env.PI_PARLEY_ASK_TIMEOUT_MS;
  process.env.PI_PARLEY_ASK_TIMEOUT_MS = "50";
  const { planner, orchestrator, cleanup } = await setupClients();
  const askId = "pending-record-timeout-ask";

  try {
    assert.equal((await planner.send(orchestrator.sessionId!, {
      messageId: askId,
      text: "Will this expire?",
      expectsReply: true,
    })).delivered, true);
    assert.equal(existsSync(pendingAskRecordPath(askId)), true);

    await new Promise((resolve) => setTimeout(resolve, 80));
    assert.equal((await planner.send(orchestrator.sessionId!, { text: "Prune asks." })).delivered, true);

    assert.equal(existsSync(pendingAskRecordPath(askId)), false);
  } finally {
    if (previousTimeout === undefined) {
      delete process.env.PI_PARLEY_ASK_TIMEOUT_MS;
    } else {
      process.env.PI_PARLEY_ASK_TIMEOUT_MS = previousTimeout;
    }
    await cleanup();
  }
});

test("reverse and clarification asks leave earlier requests open until explicitly answered", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  try {
    const originalId = "release-approval-question";
    assert.equal((await planner.send(orchestrator.sessionId!, {
      messageId: originalId, text: "Can we release?", expectsReply: true,
    })).delivered, true);

    const clarificationId = "release-region-question";
    const clarification = once(planner, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await orchestrator.send(planner.sessionId!, {
      messageId: clarificationId, text: "Which region?", replyTo: originalId, expectsReply: true,
    })).delivered, true);
    assert.equal((await clarification)[1].replyTo, originalId);
    assert.equal(existsSync(pendingAskRecordPath(originalId)), true,
      "a threaded question is not a completed answer, including for older clients omitting completesAsk");

    const reverseId = "release-docs-question";
    assert.equal((await orchestrator.send(planner.sessionId!, {
      messageId: reverseId, text: "Are the docs published?", expectsReply: true,
    })).delivered, true);
    assert.equal(existsSync(pendingAskRecordPath(originalId)), true);

    const failure = await orchestrator.send("missing-session", {
      text: "This cannot answer the release question.", replyTo: originalId,
    });
    assert.equal(failure.delivered, false);
    assert.equal(failure.outcomeKnown, true);
    assert.equal(existsSync(pendingAskRecordPath(originalId)), true);

    assert.equal((await planner.send(orchestrator.sessionId!, {
      text: "EU region.", replyTo: clarificationId,
    })).delivered, true);
    assert.equal(existsSync(pendingAskRecordPath(clarificationId)), false);
    assert.equal(existsSync(pendingAskRecordPath(originalId)), true);
    assert.equal(existsSync(pendingAskRecordPath(reverseId)), true);

    assert.equal((await orchestrator.send(planner.sessionId!, {
      text: "Approved for EU.", replyTo: originalId,
    })).delivered, true);
    assert.equal(existsSync(pendingAskRecordPath(originalId)), false);
    assert.equal(existsSync(pendingAskRecordPath(reverseId)), true,
      "answering one request must not remove the independent reverse question");
  } finally {
    await cleanup();
  }
});

test("regular parley ask timeout reports message id and delivery state", { concurrency: false }, async () => {
  const previousTimeout = process.env.PI_PARLEY_ASK_TIMEOUT_MS;
  process.env.PI_PARLEY_ASK_TIMEOUT_MS = "500";
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const senderHarness = createExtensionHarness("timeout-worker", { sessionId: "session-timeout-worker" });
  const receiverHarness = createExtensionHarness("timeout-target", { sessionId: "session-timeout-target", hasUI: true });

  try {
    piParleyExtension(senderHarness.pi as never);
    piParleyExtension(receiverHarness.pi as never);
    await senderHarness.emitLifecycle("session_start");
    await receiverHarness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "timeout-target");
    const parleyTool = senderHarness.tools.find((tool) => tool.name === "parley")!;

    const result = await parleyTool.execute("ask-timeout", { action: "ask", to: "timeout-target", message: "Will this time out?" }, new AbortController().signal, undefined, senderHarness.ctx);

    assert.equal(result.details?.error, true);
    assert.equal(result.details?.deliveryState, "injected");
    assert.match(result.content[0]?.text ?? "", new RegExp(String(result.details?.messageId)));
    assert.match(result.content[0]?.text ?? "", /Last known delivery state: injected/);
    assert.match(result.content[0]?.text ?? "", /not cancellation/);
    assert.equal(receiverHarness.sentMessages.length, 1);
  } finally {
    if (previousTimeout === undefined) {
      delete process.env.PI_PARLEY_ASK_TIMEOUT_MS;
    } else {
      process.env.PI_PARLEY_ASK_TIMEOUT_MS = previousTimeout;
    }
    await senderHarness.emitLifecycle("session_shutdown");
    await receiverHarness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("regular parley ask cancellation withdraws the request without preventing reverse collaboration", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { orchestrator, cleanup } = await setupClients();

  try {
    const harness = createExtensionHarness("cancel-cleanup-worker");
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(orchestrator, "cancel-cleanup-worker");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;

    const controller = new AbortController();
    const cancelledMessage = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const cancelledResultPromise = parleyTool.execute("ask-cancelled", { action: "ask", to: "orchestrator", message: "Should I continue?" }, controller.signal, undefined, harness.ctx);
    await cancelledMessage;
    controller.abort();
    const cancelledResult = await cancelledResultPromise;
    assert.equal(cancelledResult.details?.error, true);
    assert.match(cancelledResult.content[0]?.text ?? "", /Cancelled/);

    const reverseAsk = await orchestrator.send(worker.id, {
      messageId: "reverse-after-cancel",
      text: "Can I ask after your cancellation?",
      expectsReply: true,
    });
    assert.equal(reverseAsk.delivered, true);
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await cleanup();
  }
});

test("child supervisor tool clears reply waiter when cancelled", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { orchestrator, cleanup } = await setupClients();

  try {
    await withChildOrchestratorEnv({
      orchestratorTarget: "orchestrator",
      runId: "78f659a3",
      agent: "worker",
      index: "0",
      sessionName: "subagent-worker-78f659a3-1",
    }, async () => {
      const harness = createExtensionHarness("subagent-worker-78f659a3-1");
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;

      const controller = new AbortController();
      const cancelledMessage = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const cancelledResultPromise = supervisorTool.execute("ask-cancelled", { reason: "need_decision", message: "Should I continue?" }, controller.signal, undefined, harness.ctx);
      await cancelledMessage;
      controller.abort();
      const cancelledResult = await cancelledResultPromise;
      assert.equal(cancelledResult.details?.error, true);
      assert.match(cancelledResult.content[0]?.text ?? "", /Cancelled/);

      const nextMessage = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const nextResultPromise = supervisorTool.execute("ask-next", { reason: "need_decision", message: "Can I ask again?" }, new AbortController().signal, undefined, harness.ctx);
      const [from, message] = await nextMessage;
      assert.match(message.content.text, /Can I ask again/);
      const reply = await orchestrator.send(from.id, { text: "Yes.", replyTo: message.id });
      assert.equal(reply.delivered, true);
      const nextResult = await nextResultPromise;
      assert.notEqual(nextResult.details?.error, true);
      assert.match(nextResult.content[0]?.text ?? "", /Yes\./);
      await harness.emitLifecycle("session_shutdown");
    });
  } finally {
    await cleanup();
  }
});

test("non-blocking asks return immediately, surface outstanding state, and resolve on reply", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("nonblocking-asker");

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;

    const askDelivered = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const askResult = await parleyTool.execute("nonblocking-ask-1", {
      action: "ask",
      to: "planner",
      message: "What ships next?",
      blocking: false,
      // Real adapters scaffold every optional field.
      targets: [""],
      profile: { name: "", description: "" },
    }, new AbortController().signal, undefined, harness.ctx);

    assert.equal(askResult.details?.error, undefined);
    assert.equal(askResult.details?.nonBlocking, true);
    const askId = visibleMessageId(modelText(askResult));
    assert.match(modelText(askResult), /non-blocking/i);

    const [, askMessage] = await askDelivered;
    assert.equal(askMessage.expectsReply, true, "the wire ask keeps full ask semantics");
    assert.equal(askMessage.id, askId);

    const statusResult = await parleyTool.execute("nonblocking-status-1", {
      action: "status",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(modelText(statusResult), /Outstanding asks/);
    assert.match(modelText(statusResult), /planner.*messageId /);
    assert.match(statusResult.content[0]?.text ?? "", new RegExp(askId), "status shows the full messageId for chaining");
    assert.match(statusResult.content[0]?.text ?? "", /What ships next\?/);
    assert.ok(modelText(statusResult).includes(askId));

    // A second outstanding non-blocking ask is allowed while the first waits.
    const secondDelivered = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const secondResult = await parleyTool.execute("nonblocking-ask-2", {
      action: "ask",
      to: "planner",
      message: "And what about the docs?",
      blocking: false,
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(secondResult.details?.error, undefined);
    const secondId = visibleMessageId(modelText(secondResult));
    await secondDelivered;

    // The reply arrives as an ordinary injected message and resolves tracking.
    const sentCount = harness.sentMessages.length;
    const replySent = await planner.send("session-child-test", {
      messageId: `reply-${askId}`,
      text: "Ship the router first.",
      replyTo: askId,
    });
    assert.equal(replySent.delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.ok(
      harness.sentMessages.length > sentCount,
      "the reply is injected into the asker session like any inbound message",
    );
    assert.match(
      harness.sentMessages.at(-1)?.message.content ?? "",
      /Ship the router first\./,
    );

    const afterReplyStatus = await parleyTool.execute("nonblocking-status-2", {
      action: "status",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.doesNotMatch(modelText(afterReplyStatus), new RegExp(askId));
    assert.ok(modelText(afterReplyStatus).includes(secondId), "the unanswered question remains actionable");

    // Cancelling the outstanding ask clears tracking.
    const cancelResult = await parleyTool.execute("nonblocking-cancel-1", {
      action: "cancel",
      messageId: secondId,
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(cancelResult.details?.error, undefined);
    const finalStatus = await parleyTool.execute("nonblocking-status-3", {
      action: "status",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(modelText(finalStatus), /Outstanding asks: none/);
  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await cleanup();
  }
});

test("an independent non-blocking ask failure cannot terminate a blocking request or its other async questions", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("parallel-asker");
  const controller = new AbortController();
  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const tool = harness.tools.find((tool) => tool.name === "parley")!;
    const call = (params: Record<string, unknown>) => tool.execute("parallel-ask", params, controller.signal, undefined, harness.ctx);
    let blockingSettled = false;
    const blockingReceived = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const blocking = call({ action: "ask", to: "planner", message: "Approve the rollout?" })
      .then((result) => { blockingSettled = true; return result; });
    const [asker, approval] = await blockingReceived;

    const asyncReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const asyncReceipt = modelText(await call({ action: "ask", to: "orchestrator", message: "Are the docs ready?", blocking: false }));
    const asyncId = visibleMessageId(asyncReceipt);
    assert.equal((await asyncReceived)[1].id, asyncId);
    const failed = modelText(await call({ action: "ask", cwd: repoDir, message: "Any last concerns?", blocking: false }));
    assert.match(failed, /multiple|ambiguous/i);
    assert.ok(failed.includes(asyncId), "failure still shows the separate accepted async question");
    assert.equal(blockingSettled, false, "a failed target lookup belongs to its invocation, not the existing waiter");

    assert.equal((await orchestrator.send(asker.id, { text: "Docs are published.", replyTo: asyncId })).delivered, true);
    await waitForVisibleText(harness, "Docs are published.");
    const remainingStatus = modelText(await call({ action: "status" }));
    assert.doesNotMatch(remainingStatus, new RegExp(asyncId));
    assert.match(remainingStatus, /Approve the rollout/);
    assert.ok(remainingStatus.includes(approval.id), "the unanswered blocking question is still visible");
    assert.equal(blockingSettled, false, "answering another question must not release the blocking approval request");

    assert.equal((await planner.send(asker.id, { text: "Rollout approved.", replyTo: approval.id })).delivered, true);
    const final = modelText(await blocking);
    assert.match(final, /Rollout approved/);
    assert.doesNotMatch(final, /Failed to ask|No reply from|Cancelled/);
  } finally {
    controller.abort();
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("rename sets the canonical session name and receipts carry the send-time identity", { concurrency: false }, async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("identity-worker");

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const parleyToolFixture = harness.tools.find((tool) => tool.name === "parley")!;

    const firstDelivered = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const firstSend = await parleyToolFixture.execute("identity-send-1", {
      action: "send",
      to: "planner",
      message: "first identity check",
    }, new AbortController().signal, undefined, harness.ctx);
    await firstDelivered;
    assert.match(firstSend.content[0]?.text ?? "", /Message sent as identity-worker to planner/);

    const renameResult = await parleyToolFixture.execute("identity-rename-1", {
      action: "rename",
      name: "renamed-worker",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(renameResult.details?.error, undefined);
    assert.match(renameResult.content[0]?.text ?? "", /Session name set to "renamed-worker"\./);
    assert.equal((harness.pi as unknown as { getSessionName: () => string }).getSessionName(), "renamed-worker");

    const rosterDeadline = Date.now() + 5_000;
    let rosterName: string | undefined;
    while (Date.now() < rosterDeadline) {
      const roster = await planner.listSessions();
      rosterName = roster.find((session) => session.id === "session-child-test")?.name;
      if (rosterName === "renamed-worker") break;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    assert.equal(rosterName, "renamed-worker", "the broker roster reflects the canonical rename");

    const secondDelivered = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const secondSend = await parleyToolFixture.execute("identity-send-2", {
      action: "send",
      to: "planner",
      message: "second identity check",
    }, new AbortController().signal, undefined, harness.ctx);
    await secondDelivered;
    assert.match(secondSend.content[0]?.text ?? "", /Message sent as renamed-worker to planner/);

    const askDelivered = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const askResult = await parleyToolFixture.execute("identity-ask-1", {
      action: "ask",
      to: "planner",
      message: "identity ask check",
      blocking: false,
    }, new AbortController().signal, undefined, harness.ctx);
    await askDelivered;
    assert.match(askResult.content[0]?.text ?? "", /Ask sent as renamed-worker to planner/);

    const sentEntries = harness.entries
      .filter((entry) => entry.type === "parley_sent")
      .map((entry) => entry.data as { as?: string });
    assert.equal(sentEntries[0]?.as, "identity-worker", "historical sends keep the identity used at send time");
    assert.equal(sentEntries.at(-1)?.as, "renamed-worker");

    const reservedRename = await parleyToolFixture.execute("identity-rename-2", {
      action: "rename",
      name: "oqs1.reserved-namespace",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(reservedRename.details?.error, true);
    const emptyRename = await parleyToolFixture.execute("identity-rename-3", {
      action: "rename",
      name: "   ",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(emptyRename.details?.error, true);
  } finally {
    await harness.emitLifecycle("session_shutdown").catch(() => undefined);
    await cleanup();
  }
});

test("full ask/reply round-trip works with reply target resolved from current turn context", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replyTracker = new ReplyTracker();

  try {
    const askId = "ask-current-turn";
    const askPromise = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const replyPromise = waitForReply(planner, askId);

    const delivered = await planner.send(orchestrator.sessionId!, {
      messageId: askId,
      text: "What should I do next?",
      expectsReply: true,
    });
    assert.equal(delivered.delivered, true);

    const [from, message] = await askPromise;
    const context = replyTracker.recordIncomingMessage(from, message, Date.now());
    replyTracker.activateContexts([context]);

    const target = replyTracker.resolveReplyTarget({}, Date.now());
    const sent = await orchestrator.send(target.from.id, {
      text: "Ship it.",
      replyTo: target.message.id,
    });
    assert.equal(sent.delivered, true);
    replyTracker.markReplied(target.message.id);

    const reply = await replyPromise;
    assert.equal(reply.message.content.text, "Ship it.");
    assert.equal(reply.message.replyTo, askId);
    assert.deepEqual(replyTracker.listPending(Date.now()), []);
  } finally {
    await cleanup();
  }
});

test("pending and read recover complete questions, and reply receipts keep another colleague actionable", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("reply-target-worker");
  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "reply-target-worker");
    const releaseQuestion = "Is release safe? " + "Please review the migration constraints. ".repeat(10) + "The required rollback target is build 41.";
    assert.equal((await planner.send(worker.id, { messageId: "reply-target-release-question-long-id", text: releaseQuestion, expectsReply: true,
      attachments: [{ type: "snippet", name: "rollback.md", content: "Keep the old index until rollout completes." }],
    })).delivered, true);
    assert.equal((await orchestrator.send(worker.id, { messageId: "reply-target-docs-question-long-id", text: "Are the docs ready?", expectsReply: true })).delivered, true);
    await waitForVisibleText(harness, "Are the docs ready?");
    const tool = harness.tools.find((tool) => tool.name === "parley")!;
    const call = (params: Record<string, unknown>) => tool.execute("answer-selected", params, new AbortController().signal, undefined, harness.ctx);

    const pending = modelText(await call({ action: "pending" }));
    const releaseId = pendingMessageId(pending, "Is release safe?");
    const docsId = pendingMessageId(pending, "Are the docs ready?");
    const retained = modelText(await call({ action: "read", messageId: releaseId }));
    assert.equal(visibleMessageId(retained), releaseId);
    assert.match(retained, /From planner/);
    assert.ok(retained.includes(releaseQuestion), "read returns the full question, not the pending preview");
    assert.match(retained, /rollback.md/);
    assert.match(retained, /Keep the old index until rollout completes/);

    const docsAnswer = waitForReply(orchestrator, docsId);
    const firstReceipt = modelText(await call({ action: "reply", replyTo: docsId, message: "The docs are ready." }));
    assert.equal((await docsAnswer).message.content.text, "The docs are ready.");
    assert.match(firstReceipt, /Reply sent as reply-target-worker to orchestrator/);
    assert.equal(pendingMessageId(firstReceipt, "Is release safe?"), releaseId,
      "the receipt itself supports deciding which colleague still needs an answer");
    assert.doesNotMatch(firstReceipt, /Are the docs ready/);

    const releaseAnswer = waitForReply(planner, releaseId);
    await call({ action: "reply", replyTo: pendingMessageId(firstReceipt, "Is release safe?"), message: "Safe with rollback to build 41; keep the old index." });
    assert.equal((await releaseAnswer).message.content.text, "Safe with rollback to build 41; keep the old index.");
    assert.match(modelText(await call({ action: "pending" })), /No unresolved inbound asks/);
    assert.ok(modelText(await call({ action: "read", messageId: releaseId })).includes(releaseQuestion),
      "answering does not discard the retained original");
    assert.match(modelText(await call({ action: "read", messageId: "not-retained" })), /not retained/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("parley reply sends attachments", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("reply-attachment-worker");

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "reply-attachment-worker");

    const askId = "reply-attachment-ask";
    assert.equal((await planner.send(worker.id, { messageId: askId, text: "Send details?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const attachments = [{ type: "snippet" as const, name: "details.md", content: "attached details", language: "md" }];
    const replyReceived = waitForReply(planner, askId);
    const result = await parleyTool.execute("reply-with-attachment", {
      action: "reply",
      message: "Here are details.",
      attachments,
    }, new AbortController().signal, undefined, harness.ctx);

    assert.equal(result.details?.delivered, true);
    const reply = await replyReceived;
    assert.equal(reply.message.content.text, "Here are details.");
    assert.deepEqual(reply.message.content.attachments, attachments);

    const sentEntry = harness.entries.find((entry) => entry.type === "parley_sent");
    assert.deepEqual((sentEntry?.data as { message?: { attachments?: unknown } }).message?.attachments, attachments);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("an active inbound ask still allows consulting and notifying other colleagues", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("release-reviewer");
  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "release-reviewer");
    assert.equal((await planner.send(worker.id, {
      messageId: "release-consultation-request", text: "Is the migration safe?", expectsReply: true,
    })).delivered, true);
    const inbound = await waitForVisibleText(harness, "Is the migration safe?");
    const originalId = visibleMessageId(inbound);
    await harness.emitLifecycle("turn_start");
    const tool = harness.tools.find((tool) => tool.name === "parley")!;
    const call = (params: Record<string, unknown>) => tool.execute("collaboration", params, new AbortController().signal, undefined, harness.ctx);

    const consultationReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const consultation = call({ action: "ask", to: "orchestrator", message: "Can the index be built online?" });
    const [from, question] = await consultationReceived;
    assert.equal(question.expectsReply, true);
    assert.equal((await orchestrator.send(from.id, { text: "Yes, with CONCURRENTLY.", replyTo: question.id })).delivered, true);
    const advice = modelText(await consultation);
    assert.match(advice, /Yes, with CONCURRENTLY/);
    assert.match(advice, /Is the migration safe/);
    assert.ok(advice.includes(originalId), "the consulting receipt keeps the original colleague's question actionable");

    const notice = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const notification = await call({ action: "send", to: "orchestrator", message: "Thanks; I am preparing the release recommendation." });
    assert.equal((await notice)[1].replyTo, undefined);
    assert.match(modelText(notification), /sent as release-reviewer to orchestrator/);
    const sharedNotice = await call({ action: "send", targets: ["planner", "orchestrator"], message: "Review is still in progress." });
    assert.match(modelText(sharedNotice), /accepted for 2 of 2 targets/i);
    assert.ok(modelText(sharedNotice).includes(originalId));
    const broadcast = await call({ action: "broadcast", message: "Release freeze remains in effect." });
    assert.match(modelText(broadcast), /accepted for 2 of 2 visible sessions/i);
    assert.ok(modelText(broadcast).includes(originalId));

    const answerReceived = waitForReply(planner, originalId);
    const answered = await call({ action: "reply", replyTo: pendingMessageId(modelText(broadcast), "Is the migration safe?"), message: "Safe if we build the index with CONCURRENTLY." });
    assert.match(modelText(answered), /Reply sent as release-reviewer to planner/);
    assert.equal((await answerReceived).message.content.text, "Safe if we build the index with CONCURRENTLY.");
    assert.doesNotMatch(modelText(await call({ action: "pending" })), /awaiting your reply/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("parley reply targets one of multiple pending asks by short session ID", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("reply-short-id-worker");

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "reply-short-id-worker");

    assert.equal((await planner.send(worker.id, { messageId: "reply-short-id-1", text: "First?", expectsReply: true })).delivered, true);
    assert.equal((await orchestrator.send(worker.id, { messageId: "reply-short-id-2", text: "Second?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const replyReceived = waitForReply(planner, "reply-short-id-1");
    const result = await parleyTool.execute("reply-short-id", {
      action: "reply",
      to: planner.sessionId!.slice(0, 8),
      message: "First answer.",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(result.details?.delivered, true);
    assert.equal((await replyReceived).message.content.text, "First answer.");

    const pending = await parleyTool.execute("pending-after-short-id", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.doesNotMatch(pending.content[0]?.text ?? "", /reply-short-id-1/);
    assert.match(pending.content[0]?.text ?? "", /reply-short-id-2/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("a short-ID reply unblocks the original ask when another ask is pending", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const askerHarness = createExtensionHarness("short-id-asker", { sessionId: "asker123-session" });
  const replierHarness = createExtensionHarness("short-id-replier", { sessionId: "replier-session" });

  try {
    piParleyExtension(askerHarness.pi as never);
    piParleyExtension(replierHarness.pi as never);
    await askerHarness.emitLifecycle("session_start");
    await replierHarness.emitLifecycle("session_start");
    const asker = await waitForSessionByName(planner, "short-id-asker");
    const replier = await waitForSessionByName(planner, "short-id-replier");
    const askerTool = askerHarness.tools.find((tool) => tool.name === "parley")!;
    const replierTool = replierHarness.tools.find((tool) => tool.name === "parley")!;

    const originalAsk = askerTool.execute("ask-for-work", {
      action: "ask",
      to: replier.id,
      message: "Is any work pending?",
    }, new AbortController().signal, undefined, askerHarness.ctx);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal((await orchestrator.send(replier.id, {
      messageId: "another-pending-ask",
      text: "A separate pending question",
      expectsReply: true,
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const reply = await replierTool.execute("reply-to-work-ask", {
      action: "reply",
      to: asker.id.slice(0, 8),
      message: "No work is pending.",
    }, new AbortController().signal, undefined, replierHarness.ctx);
    assert.equal(reply.details?.delivered, true, reply.content.map((part) => part.text).join("\n"));

    const result = await originalAsk;
    assert.doesNotMatch(result.content[0]?.text ?? "", /No reply from/);
    assert.match(result.content[0]?.text ?? "", /No work is pending/);

    const pending = await replierTool.execute("remaining-pending", { action: "pending" }, new AbortController().signal, undefined, replierHarness.ctx);
    assert.match(pending.content[0]?.text ?? "", /another-pending-ask/);
  } finally {
    await askerHarness.emitLifecycle("session_shutdown");
    await replierHarness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("broker queues replies to recently disconnected named senders", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new ParleyClient();

  try {
    const originalPlannerId = planner.sessionId!;
    const receivedAsk = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await planner.send(orchestrator.sessionId!, { messageId: "ephemeral-cli-ask", text: "Can you answer later?", expectsReply: true })).delivered, true);
    await receivedAsk;
    await planner.disconnect();

    const queuedReply = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    const reply = await orchestrator.send(originalPlannerId, {
      messageId: "queued-reply-to-ephemeral",
      text: "Queued answer.",
      replyTo: "ephemeral-cli-ask",
    });
    assert.equal(reply.delivered, true);

    await replacement.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    const [from, message] = await queuedReply;
    assert.equal(from.id, orchestrator.sessionId);
    assert.equal(message.id, "queued-reply-to-ephemeral");
    assert.equal(message.replyTo, "ephemeral-cli-ask");
    assert.equal(message.content.text, "Queued answer.");
    assert.equal(typeof message.brokerReceivedAt, "number");
    assert.equal(typeof message.brokerDeliveredAt, "number");
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker rejects blocking asks to disconnected targets", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();

  try {
    const disconnectedId = planner.sessionId!;
    await planner.disconnect();
    const result = await orchestrator.send(disconnectedId, {
      messageId: "offline-broker-ask",
      text: "Do not queue this blocking request.",
      expectsReply: true,
    });
    assert.equal(result.delivered, false);
    assert.match(result.reason ?? "", /not currently connected/);
    assert.match(result.reason ?? "", /not queued/);
  } finally {
    await cleanup();
  }
});

test("broker never remaps a disconnected mailbox back to the sending session", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const sender = new ParleyClient();
  const replacement = new ParleyClient();

  try {
    const disconnectedId = planner.sessionId!;
    await planner.disconnect();
    await sender.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    const senderId = sender.sessionId!;

    const selfDeliveries: Message[] = [];
    sender.on("message", (_from: SessionInfo, message: Message) => selfDeliveries.push(message));
    const result = await sender.send(disconnectedId, {
      messageId: "no-self-mailbox-remap",
      text: "Queue this for the disconnected session.",
    });
    assert.equal(result.delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(selfDeliveries, []);

    await sender.disconnect();
    await sender.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, senderId);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(selfDeliveries, []);

    const queuedMessage = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, disconnectedId);
    const [, message] = await queuedMessage;
    assert.equal(message.id, "no-self-mailbox-remap");
  } finally {
    await sender.disconnect().catch(() => undefined);
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker does not treat runtime fallback aliases as reconnect identities", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const original = new ParleyClient();
  const unrelated = new ParleyClient();
  const replacement = new ParleyClient();
  const fallbackAlias = "session-019fe418-248e-7447";
  const originalId = "runtime-fallback-original";

  try {
    await original.connect({
      name: fallbackAlias,
      runtimeFallbackAlias: true,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, originalId);
    await original.disconnect();

    const unrelatedDeliveries: Message[] = [];
    unrelated.on("message", (_from: SessionInfo, message: Message) => unrelatedDeliveries.push(message));
    await unrelated.connect({
      name: fallbackAlias,
      runtimeFallbackAlias: true,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    assert.equal((await orchestrator.send(originalId, {
      messageId: "fallback-alias-mail",
      text: "Only the original session should receive this.",
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(unrelatedDeliveries, []);

    const queuedMessage = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: fallbackAlias,
      runtimeFallbackAlias: true,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, originalId);
    const [, message] = await queuedMessage;
    assert.equal(message.id, "fallback-alias-mail");
  } finally {
    await original.disconnect().catch(() => undefined);
    await unrelated.disconnect().catch(() => undefined);
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker does not deliver explicit mailbox mail to a matching fallback alias", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const original = new ParleyClient();
  const fallback = new ParleyClient();
  const replacement = new ParleyClient();
  const sharedName = "session-shared-worker";
  const originalId = "explicit-mailbox-original";

  try {
    await original.connect({
      name: sharedName,
      runtimeFallbackAlias: false,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, originalId);
    await original.disconnect();

    const fallbackDeliveries: Message[] = [];
    fallback.on("message", (_from: SessionInfo, message: Message) => fallbackDeliveries.push(message));
    await fallback.connect({
      name: sharedName,
      runtimeFallbackAlias: true,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    assert.equal((await orchestrator.send(originalId, {
      messageId: "explicit-mailbox-not-fallback",
      text: "Keep this message for the explicit identity.",
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(fallbackDeliveries, []);

    const queuedMessage = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: sharedName,
      runtimeFallbackAlias: false,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, originalId);
    const [, message] = await queuedMessage;
    assert.equal(message.id, "explicit-mailbox-not-fallback");
  } finally {
    await original.disconnect().catch(() => undefined);
    await fallback.disconnect().catch(() => undefined);
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker preserves mailbox reconnects for explicit subagent-chat names", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const original = new ParleyClient();
  const replacement = new ParleyClient();
  const explicitName = "subagent-chat-explicit-worker";
  const originalId = "explicit-subagent-chat-original";

  try {
    await original.connect({
      name: explicitName,
      runtimeFallbackAlias: false,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, originalId);
    await original.disconnect();

    const delivered = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: explicitName,
      runtimeFallbackAlias: false,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    assert.equal((await orchestrator.send(originalId, {
      messageId: "explicit-subagent-chat-mail",
      text: "Explicit names keep mailbox reconnect semantics.",
    })).delivered, true);
    const [, message] = await delivered;
    assert.equal(message.id, "explicit-subagent-chat-mail");
  } finally {
    await original.disconnect().catch(() => undefined);
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker delivers old-id replies to an already reconnected same-name sender", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new ParleyClient();

  try {
    const originalPlannerId = planner.sessionId!;
    const receivedAsk = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await planner.send(orchestrator.sessionId!, { messageId: "reconnected-cli-ask", text: "Can you answer after reconnect?", expectsReply: true })).delivered, true);
    await receivedAsk;
    await planner.disconnect();

    await replacement.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    const deliveredReply = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    const reply = await orchestrator.send(originalPlannerId, {
      messageId: "reply-after-cli-reconnect",
      text: "Immediate answer after reconnect.",
      replyTo: "reconnected-cli-ask",
    });
    assert.equal(reply.delivered, true);
    const [, message] = await deliveredReply;
    assert.equal(message.id, "reply-after-cli-reconnect");
    assert.equal(message.replyTo, "reconnected-cli-ask");
    assert.equal(message.content.text, "Immediate answer after reconnect.");
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker keeps queued mail away from a same-name session in another cwd", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const otherProject = new ParleyClient();

  try {
    const originalPlannerId = planner.sessionId!;
    const receivedAsk = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await planner.send(orchestrator.sessionId!, { messageId: "cross-cwd-ask", text: "Answer later?", expectsReply: true })).delivered, true);
    await receivedAsk;
    await planner.disconnect();

    assert.equal((await orchestrator.send(originalPlannerId, {
      messageId: "cross-cwd-answer",
      text: "Answer for the original project.",
      replyTo: "cross-cwd-ask",
    })).delivered, true);

    const received: Message[] = [];
    otherProject.on("message", (_from: SessionInfo, message: Message) => received.push(message));
    await otherProject.connect({
      name: "planner",
      cwd: path.join(repoDir, "other-project"),
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    await new Promise((resolve) => setTimeout(resolve, 200));

    assert.deepEqual(received, []);
  } finally {
    await otherProject.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker delivers queued mail to a relaunch reporting the same cwd differently", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new ParleyClient();

  try {
    const originalPlannerId = planner.sessionId!;
    const receivedAsk = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await planner.send(orchestrator.sessionId!, { messageId: "cwd-variant-ask", text: "Answer later?", expectsReply: true })).delivered, true);
    await receivedAsk;
    await planner.disconnect();

    assert.equal((await orchestrator.send(originalPlannerId, {
      messageId: "cwd-variant-answer",
      text: "Answer for the same project.",
      replyTo: "cwd-variant-ask",
    })).delivered, true);

    const queuedReply = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: "planner",
      // Same directory as setupClients(), spelled with a ".." segment and a
      // trailing separator. Built by concatenation because path.join would
      // collapse the ".." before the broker ever sees it.
      cwd: `${repoDir}${path.sep}ui${path.sep}..${path.sep}`,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    const [, message] = await queuedReply;
    assert.equal(message.id, "cwd-variant-answer");
    assert.equal(message.content.text, "Answer for the same project.");
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker does not reroute an id-addressed message to a same-name session in another cwd", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const otherProject = new ParleyClient();

  try {
    const originalPlannerId = planner.sessionId!;
    const receivedAsk = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await planner.send(orchestrator.sessionId!, { messageId: "cross-cwd-live-ask", text: "Answer later?", expectsReply: true })).delivered, true);
    await receivedAsk;
    await planner.disconnect();

    const received: Message[] = [];
    otherProject.on("message", (_from: SessionInfo, message: Message) => received.push(message));
    await otherProject.connect({
      name: "planner",
      cwd: path.join(repoDir, "other-project"),
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    assert.equal((await orchestrator.send(originalPlannerId, {
      messageId: "cross-cwd-live-answer",
      text: "Answer for the original project.",
      replyTo: "cross-cwd-live-ask",
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 200));

    assert.deepEqual(received, []);
  } finally {
    await otherProject.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("parley reply queues mail for a disconnected named sender", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("stale-reply-worker");
  const replacement = new ParleyClient();

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "stale-reply-worker");
    assert.equal((await planner.send(worker.id, { messageId: "stale-reply-ask", text: "Still there?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    await planner.disconnect();

    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const result = await parleyTool.execute("reply-stale", {
      action: "reply",
      message: "No sender remains.",
      replyTo: "stale-reply-ask",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(result.details?.delivered, true);

    const pending = await parleyTool.execute("pending-after-stale", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.match(pending.content[0]?.text ?? "", /No unresolved inbound asks/);

    const queuedReply = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    const [, message] = await queuedReply;
    assert.equal(message.replyTo, "stale-reply-ask");
    assert.equal(message.content.text, "No sender remains.");
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("subagent control parley events wake the current orchestrator session", async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const events = new EventEmitter();
  const sentMessages: Array<{ message: { customType?: string; content?: string }; options?: { triggerTurn?: boolean } }> = [];
  const pi = {
    getSessionName: () => "orchestrator",
    events: {
      on: (channel: string, handler: (payload: unknown) => void) => {
        events.on(channel, handler);
        return () => events.off(channel, handler);
      },
      emit: (channel: string, payload: unknown) => events.emit(channel, payload),
    },
    on: () => undefined,
    registerMessageRenderer: () => undefined,
    registerTool: () => undefined,
    registerCommand: () => undefined,
    registerShortcut: () => undefined,
    sendMessage: (message: { customType?: string; content?: string }, options?: { triggerTurn?: boolean }) => {
      sentMessages.push({ message, options });
    },
    appendEntry: () => undefined,
  };

  piParleyExtension(pi as never);
  pi.events.emit("subagent:control-parley", {
    to: "orchestrator",
    message: "subagent needs attention\n\nworker needs attention in run 78f659a3.",
  });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(sentMessages.length, 1);
  assert.equal(sentMessages[0]?.message.customType, "parley_message");
  assert.match(sentMessages[0]?.message.content ?? "", /From subagent-control/);
  assert.match(sentMessages[0]?.message.content ?? "", /worker needs attention in run 78f659a3/);
  assert.equal(sentMessages[0]?.options?.triggerTurn, true);
});

test("subagent result parley events wake the current orchestrator session", async () => {
  const { default: piParleyExtension } = await import("./index.ts");
  const events = new EventEmitter();
  const sentMessages: Array<{ message: { customType?: string; content?: string }; options?: { triggerTurn?: boolean } }> = [];
  const deliveryAcks: unknown[] = [];
  events.on("subagent:result-parley-delivery", (payload) => deliveryAcks.push(payload));
  const pi = {
    getSessionName: () => "orchestrator",
    events: {
      on: (channel: string, handler: (payload: unknown) => void) => {
        events.on(channel, handler);
        return () => events.off(channel, handler);
      },
      emit: (channel: string, payload: unknown) => events.emit(channel, payload),
    },
    on: () => undefined,
    registerMessageRenderer: () => undefined,
    registerTool: () => undefined,
    registerCommand: () => undefined,
    registerShortcut: () => undefined,
    sendMessage: (message: { customType?: string; content?: string }, options?: { triggerTurn?: boolean }) => {
      sentMessages.push({ message, options });
    },
    appendEntry: () => undefined,
  };

  piParleyExtension(pi as never);
  pi.events.emit("subagent:result-parley", {
    to: "orchestrator",
    requestId: "result-1",
    message: "subagent result\n\nRun: 78f659a3\nAgent: worker\nStatus: completed",
  });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(sentMessages.length, 1);
  assert.equal(sentMessages[0]?.message.customType, "parley_message");
  assert.match(sentMessages[0]?.message.content ?? "", /From subagent-result/);
  assert.match(sentMessages[0]?.message.content ?? "", /Status: completed/);
  assert.equal(sentMessages[0]?.options?.triggerTurn, true);
  assert.deepEqual(deliveryAcks, [{ requestId: "result-1", delivered: true }]);
});

test("async ask can be replied to later from the single pending ask fallback", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replyTracker = new ReplyTracker();

  try {
    const askId = "ask-later";
    const askPromise = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const replyPromise = waitForReply(planner, askId);

    const delivered = await planner.send(orchestrator.sessionId!, {
      messageId: askId,
      text: "Need an answer later.",
      expectsReply: true,
    });
    assert.equal(delivered.delivered, true);

    const [from, message] = await askPromise;
    replyTracker.recordIncomingMessage(from, message, Date.now());

    const target = replyTracker.resolveReplyTarget({}, Date.now());
    const sent = await orchestrator.send(target.from.id, {
      text: "Answering later worked.",
      replyTo: target.message.id,
    });
    assert.equal(sent.delivered, true);
    replyTracker.markReplied(target.message.id);

    const reply = await replyPromise;
    assert.equal(reply.message.content.text, "Answering later worked.");
    assert.equal(reply.message.replyTo, askId);
  } finally {
    await cleanup();
  }
});

test("presence carries context usage to peers, and an explicit null clears a stale value", { concurrency: false }, async () => {
  // Peers should see each other's live context-window usage without a separate
  // query, and a post-compaction null must CLEAR the value rather than leave a
  // stale-high percentage frozen in the list.
  const { planner, orchestrator, cleanup } = await setupClients();
  try {
    planner.updatePresence({ contextPct: 50, contextTokens: 100000, contextWindow: 200000 });
    // Flush barrier: a round-trip on planner's OWN socket guarantees the broker
    // processed the presence (FIFO per socket) before the peer probes.
    await planner.send(orchestrator.sessionId!, { text: "flush" });
    let sessions = await orchestrator.listSessions();
    let p = sessions.find(s => s.id === planner.sessionId);
    assert.equal(p?.contextPct, 50);
    assert.equal(p?.contextTokens, 100000);
    assert.equal(p?.contextWindow, 200000);

    // Post-compaction: null contextPct/tokens must CLEAR (not freeze the old %).
    planner.updatePresence({ contextPct: null, contextTokens: null });
    await planner.send(orchestrator.sessionId!, { text: "flush" });
    sessions = await orchestrator.listSessions();
    p = sessions.find(s => s.id === planner.sessionId);
    assert.equal(p?.contextPct, undefined, "null contextPct must CLEAR the field, not freeze the old value");
    assert.equal(p?.contextTokens, undefined);
    // contextWindow (the denominator, not nulled here) is retained.
    assert.equal(p?.contextWindow, 200000);
  } finally {
    await cleanup();
  }
});

test("SDK usage sampling clears an unknown post-compaction sample and publishes later measurements", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("usage-sampling-worker", { sessionId: "usage-sampling-session" });
  let usage: { tokens: number; contextWindow: number; percent: number } | undefined = {
    tokens: 217000, contextWindow: 272000, percent: 80,
  };
  Object.assign(harness.ctx, { getContextUsage: () => usage });
  const { default: extension } = await import("./index.ts");
  const observe = async (percent: number | undefined, tokens: number | undefined) => {
    const deadline = Date.now() + 2000;
    let seat: SessionInfo | undefined;
    do {
      seat = (await planner.listSessions()).find((peer) => peer.id === "usage-sampling-session");
      if (seat && seat.contextPct === percent && seat.contextTokens === tokens) return seat;
      await new Promise((resolve) => setTimeout(resolve, 20));
    } while (Date.now() < deadline);
    assert.equal(seat?.contextPct, percent);
    assert.equal(seat?.contextTokens, tokens);
    assert.ok(seat);
    return seat;
  };
  try {
    extension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    assert.equal((await observe(80, 217000)).contextWindow, 272000);
    usage = undefined;
    await harness.emitLifecycle("session_compact", { reason: "manual" });
    assert.equal((await observe(undefined, undefined)).contextWindow, undefined);
    usage = { tokens: 70000, contextWindow: 272000, percent: 26 };
    await harness.emitLifecycle("turn_start");
    assert.equal((await observe(26, 70000)).contextWindow, 272000);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("an ordinary notification can be replied to using only its visible conversation handle", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const sender = createExtensionHarness("docs-author", { sessionId: "docs-author-session" });
  const recipient = createExtensionHarness("docs-reviewer", { sessionId: "docs-reviewer-session" });
  try {
    piParleyExtension(sender.pi as never);
    piParleyExtension(recipient.pi as never);
    await sender.emitLifecycle("session_start");
    await recipient.emitLifecycle("session_start");
    await waitForSessionByName(planner, "docs-author");
    await waitForSessionByName(planner, "docs-reviewer");
    const senderTool = sender.tools.find((tool) => tool.name === "parley")!;
    const recipientTool = recipient.tools.find((tool) => tool.name === "parley")!;
    const sent = await senderTool.execute("notify-docs", { action: "send", to: "docs-reviewer", message: "The migration docs are published." }, new AbortController().signal, undefined, sender.ctx);
    assert.match(modelText(sent), /Message sent as docs-author to docs-reviewer/);
    const noticeId = visibleMessageId(modelText(sent));
    const incoming = await waitForVisibleText(recipient, "The migration docs are published.");
    assert.equal(visibleMessageId(incoming), noticeId);
    const pending = await recipientTool.execute("docs-pending", { action: "pending" }, new AbortController().signal, undefined, recipient.ctx);
    assert.match(modelText(pending), /No unresolved inbound asks/);

    const reply = await recipientTool.execute("acknowledge-docs", {
      action: "reply", replyTo: visibleMessageId(incoming), message: "Thanks; I linked them in the release notes.",
    }, new AbortController().signal, undefined, recipient.ctx);
    assert.match(modelText(reply), /Reply sent as docs-reviewer to docs-author/);
    const answer = await waitForVisibleText(sender, "Thanks; I linked them in the release notes.");
    assert.equal(visibleMessageId(answer), visibleMessageId(modelText(reply)));
    assert.ok(answer.includes(`Reply to: ${noticeId}`));
    const retained = await senderTool.execute("read-docs-reply", { action: "read", messageId: visibleMessageId(answer) }, new AbortController().signal, undefined, sender.ctx);
    assert.match(modelText(retained), /Thanks; I linked them in the release notes/);
    assert.ok(modelText(retained).includes(noticeId));
  } finally {
    await sender.emitLifecycle("session_shutdown");
    await recipient.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("notifications, threaded progress, and clarification questions do not complete a blocking ask; an explicit reply does", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const asker = createExtensionHarness("release-planner", { sessionId: "release-planner-session" });
  const reviewer = createExtensionHarness("release-reviewer", { sessionId: "release-reviewer-session" });
  const controller = new AbortController();
  try {
    piParleyExtension(asker.pi as never);
    piParleyExtension(reviewer.pi as never);
    await asker.emitLifecycle("session_start");
    await reviewer.emitLifecycle("session_start");
    await waitForSessionByName(planner, "release-planner");
    await waitForSessionByName(planner, "release-reviewer");
    const askerTool = asker.tools.find((tool) => tool.name === "parley")!;
    const reviewerTool = reviewer.tools.find((tool) => tool.name === "parley")!;
    const call = (params: Record<string, unknown>) => reviewerTool.execute("review", params, controller.signal, undefined, reviewer.ctx);
    let completed = false;
    const approval = askerTool.execute("approval", { action: "ask", to: "release-reviewer", message: "May I release build 42?" }, controller.signal, undefined, asker.ctx)
      .then((result) => { completed = true; return result; });
    const questionText = await waitForVisibleText(reviewer, "May I release build 42?");
    const approvalId = visibleMessageId(questionText);
    await reviewer.emitLifecycle("turn_start");

    const notice = await call({ action: "send", to: "release-planner", message: "Unrelated: the docs build finished." });
    assert.match(modelText(notice), /Message sent as release-reviewer to release-planner/);
    const deliveredNotice = await waitForVisibleText(asker, "Unrelated: the docs build finished.");
    assert.doesNotMatch(deliveredNotice, /Reply to:/);
    assert.equal(completed, false, "a notification is not the answer to the approval request");
    const pendingId = pendingMessageId(modelText(notice), "May I release build 42?");
    assert.equal(pendingId, approvalId);

    const progress = await call({ action: "send", to: "release-planner", replyTo: pendingId, message: "Still checking the migration." });
    const progressText = await waitForVisibleText(asker, "Still checking the migration.");
    assert.ok(progressText.includes(`Reply to: ${approvalId}`));
    assert.equal(completed, false, "send(replyTo) provides threaded progress without completing the waiter");
    assert.ok(modelText(progress).includes(approvalId), "progress preserves the unanswered question in its receipt");

    const clarification = await call({ action: "ask", to: "release-planner", replyTo: pendingId, message: "Which region is build 42 for?", blocking: false });
    const clarificationId = visibleMessageId(modelText(clarification));
    const clarifyText = await waitForVisibleText(asker, "Which region is build 42 for?");
    assert.equal(visibleMessageId(clarifyText), clarificationId);
    assert.equal(completed, false, "a reverse clarification question is not a final answer");
    const clarified = await askerTool.execute("clarify", { action: "reply", replyTo: visibleMessageId(clarifyText), message: "EU only." }, controller.signal, undefined, asker.ctx);
    assert.match(modelText(clarified), /Reply sent as release-planner to release-reviewer/);
    await waitForVisibleText(reviewer, "EU only.");
    const remaining = modelText(await call({ action: "pending" }));
    assert.ok(remaining.includes(approvalId));
    assert.equal(completed, false);

    const answer = await call({ action: "reply", replyTo: pendingMessageId(remaining, "May I release build 42?"), message: "Approved for EU only." });
    assert.match(modelText(answer), /Reply sent as release-reviewer to release-planner/);
    const answerId = visibleMessageId(modelText(answer));
    const approved = modelText(await approval);
    assert.match(approved, /Approved for EU only/);
    assert.equal(completed, true);
    // The answer receipt's exact handle is actionable too, not just send/ask handles.
    assert.match(modelText(await call({ action: "cancel", messageId: answerId })), new RegExp(answerId));
    assert.doesNotMatch(modelText(await call({ action: "pending" })), /awaiting your reply/);
  } finally {
    controller.abort();
    await asker.emitLifecycle("session_shutdown");
    await reviewer.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("confirmSend gates an ordinary notification; declining preserves the pending ask", { concurrency: false }, async () => {
  await withConfirmSendEnabled(async () => {
    const { planner, orchestrator, cleanup } = await setupClients();
    const { default: piParleyExtension } = await import("./index.ts");
    const confirmCalls: Array<[string, string]> = [];
    const harness = createExtensionHarness("confirm-reply-worker", {
      hasUI: true,
      ui: {
        confirm: async (title: string, text: string) => {
          confirmCalls.push([title, text]);
          return false;
        },
      },
    });

    try {
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const worker = await waitForSessionByName(orchestrator, "confirm-reply-worker");

      const askId = "confirm-reply-ask-1";
      assert.equal((await planner.send(worker.id, { messageId: askId, text: "Ready?", expectsReply: true })).delivered, true);
      await new Promise((resolve) => setTimeout(resolve, 50));

      const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
      const result = await parleyTool.execute("confirm-reply", {
        action: "send",
        to: "planner",
        message: "The docs build finished.",
      }, new AbortController().signal, undefined, harness.ctx);

      assert.equal(confirmCalls.length, 1);
      assert.equal(result.content[0]?.text, "Message cancelled by user");
      assert.equal(result.details?.delivered, undefined);

      const pending = await parleyTool.execute("pending-after-cancel", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
      assert.match(pending.content[0]?.text ?? "", /confirm-reply-ask-1/);
    } finally {
      await harness.emitLifecycle("session_shutdown");
      await cleanup();
    }
  });
});

test("contact_supervisor progress_update leaves a pending ask open for an explicit reply", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();

  try {
    await withChildOrchestratorEnv({
      orchestratorTarget: "orchestrator",
      runId: "aa11bb22",
      agent: "worker",
      index: "0",
      sessionName: "subagent-worker-aa11bb22-1",
    }, async () => {
      const { default: piParleyExtension } = await import("./index.ts");
      const harness = createExtensionHarness("subagent-worker-aa11bb22-1");
      piParleyExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const worker = await waitForSessionByName(orchestrator, "subagent-worker-aa11bb22-1");

      const askId = "boundary-ask-1";
      assert.equal((await orchestrator.send(worker.id, { messageId: askId, text: "Any blockers?", expectsReply: true })).delivered, true);
      await new Promise((resolve) => setTimeout(resolve, 50));

      const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;
      const updateReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const updateResult = await supervisorTool.execute("update-1", { reason: "progress_update", message: "Still working." }, new AbortController().signal, undefined, harness.ctx);
      const [, updateMessage] = await updateReceived;
      assert.notEqual(updateResult.details?.error, true);
      assert.equal(updateMessage.replyTo, undefined);

      const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
      const pendingAfterUpdate = await parleyTool.execute("pending-after-update", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
      assert.match(pendingAfterUpdate.content[0]?.text ?? "", /boundary-ask-1/);

      const replyReceived = waitForReply(orchestrator, askId);
      const replyId = pendingMessageId(modelText(pendingAfterUpdate), "Any blockers?");
      const sendResult = await parleyTool.execute("reply-after-update", { action: "reply", replyTo: replyId, message: "No blockers." }, new AbortController().signal, undefined, harness.ctx);
      assert.match(modelText(sendResult), /Reply sent/);
      const reply = await replyReceived;
      assert.equal(reply.message.replyTo, askId);

      await harness.emitLifecycle("session_shutdown");
    });
  } finally {
    await cleanup();
  }
});

test("parley ask fails fast when the target is not currently connected", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("offline-ask-worker");

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(orchestrator, "offline-ask-worker");
    const disconnectedId = planner.sessionId!;
    await planner.disconnect();

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 250);
    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const result = await parleyTool.execute("ask-offline", {
      action: "ask",
      to: disconnectedId,
      message: "This must not wait in the disconnected mailbox.",
    }, controller.signal, undefined, harness.ctx);
    clearTimeout(timeout);

    assert.match(result.content[0]?.text ?? "", /not currently connected/i);
    assert.equal(result.details?.error, true);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("offline send receipts allow cancellation by full ID and queued notifications never answer an ask", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("disconnected-asker-worker");

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(orchestrator, "disconnected-asker-worker");

    const askId = "disconnected-ask-1";
    const originalPlannerId = planner.sessionId!;
    assert.equal((await planner.send(worker.id, { messageId: askId, text: "Any concerns before I disconnect?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    await planner.disconnect();

    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;

    const prefixResult = await parleyTool.execute("send-prefix", {
      action: "send",
      to: originalPlannerId.slice(0, 8),
      message: "Do not guess from a bare prefix.",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(modelText(prefixResult), /queued as disconnected-asker-worker/);
    const prefixMessageId = visibleMessageId(modelText(prefixResult));
    const retracted = await parleyTool.execute("cancel-offline-notification", { action: "cancel", messageId: prefixMessageId }, new AbortController().signal, undefined, harness.ctx);
    assert.match(modelText(retracted), /removed from the offline mailbox/);
    assert.ok(modelText(retracted).includes(prefixMessageId));

    const pendingAfterPrefix = await parleyTool.execute("pending-after-prefix", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.match(pendingAfterPrefix.content[0]?.text ?? "", /disconnected-ask-1/);

    const replacement = new ParleyClient();
    const queuedReply = once(replacement, "message") as Promise<[SessionInfo, Message]>;

    const exactResult = await parleyTool.execute("send-exact-id", {
      action: "send",
      to: originalPlannerId,
      message: "Reconnect and see this.",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(modelText(exactResult), /queued as disconnected-asker-worker/i);
    assert.match(modelText(exactResult), /while this broker remains running/);
    const queuedId = visibleMessageId(modelText(exactResult));

    await replacement.connect({ name: "planner", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() });
    const [, queuedMessage] = await queuedReply;
    assert.equal(queuedMessage.id, queuedId);
    assert.equal(queuedMessage.replyTo, undefined);
    assert.equal(queuedMessage.content.text, "Reconnect and see this.");

    const pendingAfterExact = await parleyTool.execute("pending-after-exact-id", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.ok(modelText(pendingAfterExact).includes(askId), "offline notification does not answer the pending question");

    await replacement.disconnect().catch(() => undefined);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("known failed notification delivery preserves the pending ask", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const impostor = new ParleyClient();
  const { default: piParleyExtension } = await import("./index.ts");
  const harness = createExtensionHarness("delivery-failure-worker");

  try {
    piParleyExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(orchestrator, "delivery-failure-worker");

    const askId = "delivery-failure-ask-1";
    assert.equal((await planner.send(worker.id, { messageId: askId, text: "Ping before disconnect", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    await planner.disconnect();

    await impostor.connect({ name: "planner", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() });
    await impostor.disconnect();

    const parleyTool = harness.tools.find((tool) => tool.name === "parley")!;
    const result = await parleyTool.execute("send-ambiguous-disconnected", {
      action: "send",
      to: "planner",
      message: "Should not deliver.",
    }, new AbortController().signal, undefined, harness.ctx);

    assert.equal(result.details?.delivered, false);
    assert.equal(result.details?.delivery, "failed");
    assert.equal(result.details?.code, "E_AMBIGUOUS_TARGET");
    assert.equal(result.details?.retryable, false);
    assert.equal(result.details?.outcomeKnown, true);
    assert.match(modelText(result), /Message not delivered as delivery-failure-worker/);
    assert.match(modelText(result), /Multiple disconnected sessions named/);
    assert.doesNotMatch(modelText(result), /outcome unknown|may have arrived/i);
    visibleMessageId(modelText(result));

    const pending = await parleyTool.execute("pending-after-failure", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.match(pending.content[0]?.text ?? "", /delivery-failure-ask-1/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("parley command and shortcut open upstream UI contexts without a mode field", { concurrency: false }, async () => {
  const { default: extension } = await import("./index.ts");
  const { cleanup } = await setupClients();
  let pickerCalls = 0;
  const harness = createExtensionHarness("upstream-overlay", {
    hasUI: true,
    ui: {
      custom: async (_factory: unknown, options: { overlay?: boolean }) => {
        assert.equal(options.overlay, true);
        pickerCalls++;
        return undefined;
      },
      notify: () => undefined,
    },
  });
  try {
    assert.equal("mode" in harness.ctx, false);
    extension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await harness.commands.get("parley")!("", harness.ctx);
    await harness.shortcuts.get("alt+m")!(harness.ctx);
    assert.equal(pickerCalls, 2);
    await harness.commands.get("parley")!("", { ...harness.ctx, hasUI: false });
    await harness.shortcuts.get("alt+m")!({ ...harness.ctx, mode: "rpc" });
    assert.equal(pickerCalls, 2, "headless and explicitly non-TUI contexts do not open overlays");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("independent outboxes can reuse request ids without losing reply or cancellation routes", { concurrency: false }, async () => {
  const { default: extension } = await import("./index.ts");
  const { planner, orchestrator, cleanup } = await setupClients();
  const first = createExtensionHarness("outbox-first", { sessionId: "outbox-first-id" });
  const second = createExtensionHarness("outbox-second", { sessionId: "outbox-second-id" });
  const firstResults: ParleyOutboxResultV1[] = [];
  const secondResults: ParleyOutboxResultV1[] = [];
  const request = {
    version: 1, requestId: "reused-request", extensionId: "example", extensionName: "Example",
    message: "Independent notification.",
  };
  try {
    first.pi.events.on(PARLEY_OUTBOX_RESULT_EVENT, (result) => firstResults.push(result as ParleyOutboxResultV1));
    second.pi.events.on(PARLEY_OUTBOX_RESULT_EVENT, (result) => secondResults.push(result as ParleyOutboxResultV1));
    extension(first.pi as never);
    extension(second.pi as never);
    await first.emitLifecycle("session_start");
    await second.emitLifecycle("session_start");
    const firstDelivery = once(planner, "message") as Promise<[SessionInfo, Message]>;
    first.pi.events.emit(PARLEY_OUTBOX_REQUEST_EVENT, { ...request, to: "planner" });
    await waitForOutboxResults(firstResults, 1);
    const [firstSender, firstMessage] = await firstDelivery;
    const secondDelivery = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    second.pi.events.emit(PARLEY_OUTBOX_REQUEST_EVENT, { ...request, to: "orchestrator" });
    await waitForOutboxResults(secondResults, 1);
    const [secondSender, secondMessage] = await secondDelivery;
    assert.equal(firstResults[0]?.status, "sent");
    assert.equal(secondResults[0]?.status, "sent");
    assert.equal(firstMessage.id, firstResults[0]?.messageId);
    assert.equal(secondMessage.id, secondResults[0]?.messageId);
    assert.notEqual(firstMessage.id, secondMessage.id);
    assert.equal(firstMessage.provenance?.requestId, request.requestId);
    assert.equal(secondMessage.provenance?.requestId, request.requestId);
    assert.equal((await planner.send(firstSender.id, { text: "First reply.", replyTo: firstMessage.id })).delivered, true);
    assert.equal((await orchestrator.send(secondSender.id, { text: "Second reply.", replyTo: secondMessage.id })).delivered, true);
    await waitForVisibleText(first, "First reply.");
    await waitForVisibleText(second, "Second reply.");
    const controls: string[] = [];
    planner.onBrokerMessage((message) => {
      if (message.type === "message_control") controls.push(message.control.messageId);
    });
    const tool = first.tools.find((tool) => tool.name === "parley")!;
    const cancelled = await tool.execute("cancel-first", { action: "cancel", messageId: firstMessage.id }, new AbortController().signal, undefined, first.ctx);
    assert.equal(cancelled.details?.delivered, true);
    const deadline = Date.now() + 3000;
    while (!controls.includes(firstMessage.id) && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10));
    assert.deepEqual(controls, [firstMessage.id]);
  } finally {
    await first.emitLifecycle("session_shutdown");
    await second.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("never policy wakes only for outstanding completing answers, not ordinary or progress threads", { concurrency: false }, async () => {
  await withParleyConfig({ inboundTrigger: "never" }, async () => {
    const { default: extension } = await import("./index.ts");
    const { planner, cleanup } = await setupClients();
    const harness = createExtensionHarness("quiet-asker");
    try {
      extension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const tool = harness.tools.find((tool) => tool.name === "parley")!;
      const call = (params: Record<string, unknown>) => tool.execute("quiet-call", params, new AbortController().signal, undefined, harness.ctx);
      const notify = await call({ action: "send", to: "planner", message: "Ordinary notice." });
      const noticeId = visibleMessageId(modelText(notify));
      const self = await waitForSessionByName(planner, "quiet-asker");
      await planner.send(self.id, { text: "Ordinary threaded reply.", replyTo: noticeId });
      await waitForVisibleText(harness, "Ordinary threaded reply.");
      assert.equal(harness.sentMessages.at(-1)?.options?.deliverAs, "steer");
      const ask = await call({ action: "ask", to: "planner", message: "Can I release?", blocking: false });
      const askId = visibleMessageId(modelText(ask));
      await planner.send(self.id, { text: "Still investigating.", replyTo: askId, completesAsk: false });
      await waitForVisibleText(harness, "Still investigating.");
      assert.equal(harness.sentMessages.at(-1)?.options?.deliverAs, "steer");
      await planner.send(self.id, { text: "Which region?", replyTo: askId, expectsReply: true });
      await waitForVisibleText(harness, "Which region?");
      assert.equal(harness.sentMessages.at(-1)?.options?.deliverAs, "steer");
      assert.deepEqual(((await call({ action: "status" })).details?.outstandingAsks as Array<{ messageId: string }>).map((ask) => ask.messageId), [askId],
        "progress and clarification leave the question outstanding");
      await planner.send(self.id, { text: "Approved.", replyTo: askId });
      await waitForVisibleText(harness, "Approved.");
      assert.equal(harness.sentMessages.at(-1)?.options?.triggerTurn, true);
      assert.deepEqual((await call({ action: "status" })).details?.outstandingAsks, []);
      await planner.send(self.id, { text: "Additional follow-up.", replyTo: askId });
      await waitForVisibleText(harness, "Additional follow-up.");
      assert.equal(harness.sentMessages.at(-1)?.options?.deliverAs, "steer", "settled asks do not keep bypassing the policy");
    } finally {
      await harness.emitLifecycle("session_shutdown");
      await cleanup();
    }
  });
});

test("invalid configuration logs and preserves answers and control delivery without unsolicited wakeups", { concurrency: false }, async () => {
  for (const config of ["{invalid JSON", { confirmSend: true, inboundTrigger: "invalid" }]) {
    await withParleyConfig(config, async () => {
      const { default: extension } = await import("./index.ts");
      const { planner, cleanup } = await setupClients();
      const harness = createExtensionHarness("config-fallback");
      const errors: string[] = [];
      const previousError = console.error;
      console.error = (...args: unknown[]) => { errors.push(args.map(String).join(" ")); };
      try {
        extension(harness.pi as never);
        assert.equal(errors.length, 1);
        assert.match(errors[0]!, /Failed to load parley config/);
        await harness.emitLifecycle("session_start");
        const self = await waitForSessionByName(planner, "config-fallback");
        const unsolicited = await planner.send(self.id, { text: "Unsolicited notice." });
        await waitForVisibleText(harness, "Unsolicited notice.");
        assert.equal(harness.sentMessages.at(-1)?.options?.deliverAs, "steer");
        const results: ParleyOutboxResultV1[] = [];
        harness.pi.events.on(PARLEY_OUTBOX_RESULT_EVENT, (result) => results.push(result as ParleyOutboxResultV1));
        harness.pi.events.emit(PARLEY_OUTBOX_REQUEST_EVENT, {
          version: 1, requestId: "fallback-outbox", extensionId: "example", extensionName: "Example",
          to: "planner", message: "Do not bypass consent.",
        });
        await waitForOutboxResults(results, 1);
        assert.equal(results[0]?.status, "blocked");
        assert.equal(results[0]?.code, "confirmation_unavailable");
        const tool = harness.tools.find((tool) => tool.name === "parley")!;
        const ask = await tool.execute("fallback-ask", { action: "ask", to: "planner", message: "Requested answer?", blocking: false }, new AbortController().signal, undefined, harness.ctx);
        const askId = visibleMessageId(modelText(ask));
        await planner.send(self.id, { text: "Requested answer.", replyTo: askId });
        await waitForVisibleText(harness, "Requested answer.");
        assert.equal(harness.sentMessages.at(-1)?.options?.triggerTurn, true);
        assert.equal((await planner.cancelMessage(unsolicited.id)).delivered, true);
        const deadline = Date.now() + 3000;
        while (!harness.sentMessages.some((entry) => entry.message.customType === "parley_message_control") && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10));
        const control = harness.sentMessages.find((entry) => entry.message.customType === "parley_message_control");
        assert.ok(control);
        assert.equal(control.options?.triggerTurn, true);
      } finally {
        console.error = previousError;
        await harness.emitLifecycle("session_shutdown");
        await cleanup();
      }
    });
  }
});

test("invalid unrelated configuration cannot enable an explicitly disabled extension", { concurrency: false }, async () => {
  await withParleyConfig({ enabled: false, inboundTrigger: "invalid" }, async () => {
    const { default: extension } = await import("./index.ts");
    const { planner, cleanup } = await setupClients();
    const harness = createExtensionHarness("disabled-fallback");
    const errors: string[] = [];
    const previousError = console.error;
    console.error = (...args: unknown[]) => { errors.push(args.map(String).join(" ")); };
    try {
      extension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      await harness.emitLifecycle("turn_start");
      const tool = harness.tools.find((tool) => tool.name === "parley")!;
      const result = await tool.execute("disabled-list", { action: "list" }, new AbortController().signal, undefined, harness.ctx);
      assert.match(modelText(result), /Parley disabled/);
      assert.equal((await planner.listSessions()).some((session) => session.name === "disabled-fallback"), false);
      assert.equal(errors.length, 1);
    } finally {
      console.error = previousError;
      await harness.emitLifecycle("session_shutdown");
      await cleanup();
    }
  });
});
