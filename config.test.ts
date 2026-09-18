import test from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { getConfigPath, loadConfig } from "./config.ts";

async function withAgentDir<T>(agentDir: string, fn: () => T | Promise<T>): Promise<T> {
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  try {
    return await fn();
  } finally {
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
  }
}

test("getConfigPath uses the centralized parley runtime directory", () => {
  assert.equal(getConfigPath("/tmp/pi-agent/parley"), join("/tmp/pi-agent", "parley", "config.json"));
});

test("loadConfig reads config below PI_CODING_AGENT_DIR", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));

  try {
    const parleyDir = join(root, "parley");
    mkdirSync(parleyDir, { recursive: true });
    writeFileSync(join(parleyDir, "config.json"), JSON.stringify({ status: "platform-test" }));

    await withAgentDir(root, () => {
      assert.equal(loadConfig().status, "platform-test");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig defaults inboundTrigger to current auto-trigger behavior", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    await withAgentDir(root, () => {
      assert.equal(loadConfig().inboundTrigger, "always");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig accepts inboundTrigger replies policy", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    mkdirSync(join(root, "parley"), { recursive: true });
    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ inboundTrigger: "replies" }));
    await withAgentDir(root, () => {
      assert.equal(loadConfig().inboundTrigger, "replies");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig ignores obsolete toolVisibility values", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    mkdirSync(join(root, "parley"), { recursive: true });
    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ toolVisibility: "lazy", replyHint: false }));
    await withAgentDir(root, () => {
      assert.equal(loadConfig().replyHint, false);
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig accepts a restart-stable parley id", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    mkdirSync(join(root, "parley"), { recursive: true });
    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ stableId: " pinned-worker " }));
    await withAgentDir(root, () => {
      assert.equal(loadConfig().stableId, "pinned-worker");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig validates the optional default project launcher command", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    mkdirSync(join(root, "parley"), { recursive: true });
    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ projectLauncher: " tmux new-window -c \"{root}\" pi " }));
    await withAgentDir(root, () => {
      assert.equal(loadConfig().projectLauncher, "tmux new-window -c \"{root}\" pi");
    });

    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ projectLauncher: "" }));
    await withAgentDir(root, () => {
      assert.throws(() => loadConfig(), /"projectLauncher" must not be empty/);
    });

    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ projectLauncher: 42 }));
    await withAgentDir(root, () => {
      assert.throws(() => loadConfig(), /"projectLauncher" must be a string/);
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig rejects invalid inboundTrigger values", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    mkdirSync(join(root, "parley"), { recursive: true });
    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ inboundTrigger: "prompt" }));

    await withAgentDir(root, () => {
      assert.throws(
        () => loadConfig(),
        /Failed to load parley config.*"inboundTrigger" must be "always", "replies", or "never"/,
      );
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig reads a legacy intercom config before the runtime cutover moves it", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-legacy-"));
  try {
    const legacyDir = join(root, "intercom");
    mkdirSync(legacyDir, { recursive: true });
    writeFileSync(join(legacyDir, "config.json"), JSON.stringify({
      enabled: false,
      confirmSend: true,
      inboundTrigger: "never",
      stableId: "stable-legacy",
    }));

    await withAgentDir(root, () => {
      const config = loadConfig();
      assert.equal(config.enabled, false);
      assert.equal(config.confirmSend, true);
      assert.equal(config.inboundTrigger, "never");
      assert.equal(config.stableId, "stable-legacy");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
