import test, { type TestContext } from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { tryAcquireProcessLock, type ProcessLockResult, type ProcessLockOwner } from "./process-lock.ts";

// Real isolated processes, controlled over IPC. Nothing touches a broker/runtime.
const helperSource = `
  import { spawn } from 'node:child_process';
  import { tryAcquireProcessLock } from ${JSON.stringify(new URL("./process-lock.ts", import.meta.url).href)};
  let lease;
  process.on('message', ({ command, directory }) => {
    if (command === 'acquire') {
      const result = tryAcquireProcessLock(directory);
      if (result.status === 'acquired') lease = result.lease;
      process.send({ status: result.status, owner: result.status === 'acquired' ? lease.owner : result.owner });
    } else if (command === 'release') {
      lease?.release();
      lease?.release();
      process.send({ status: 'released' });
    } else if (command === 'descendant') {
      const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { detached: true, stdio: 'ignore', windowsHide: true });
      child.unref();
      process.send({ status: 'descendant', pid: child.pid });
    } else if (command === 'exit') {
      process.exit(0);
    }
  });
  process.send({ status: 'ready' });
`;

type Reply = { status: string; owner?: ProcessLockOwner | null; pid?: number };
interface Helper {
  child: ChildProcess;
  request(command: string, directory?: string): Promise<Reply>;
  exit(): Promise<void>;
}

const cleanups = new WeakMap<TestContext, Array<() => void | Promise<void>>>();
function cleanup(t: TestContext, action: () => void | Promise<void>) {
  let actions = cleanups.get(t);
  if (!actions) {
    actions = [];
    cleanups.set(t, actions);
    t.after(async () => {
      const errors: unknown[] = [];
      // Node's after hooks are FIFO; explicitly unwind resources before directories.
      for (const release of actions!.reverse()) {
        try { await release(); } catch (error) { errors.push(error); }
      }
      if (errors.length) throw new AggregateError(errors, "Test resource cleanup failed");
    });
  }
  actions.push(action);
}

function temporaryDirectory(t: TestContext): string {
  const directory = mkdtempSync(path.join(tmpdir(), "parley-process-lock-test-"));
  cleanup(t, () => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

async function helper(t: TestContext): Promise<Helper> {
  const child = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "-e", helperSource], {
    stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  let stderr = "";
  child.stderr?.on("data", (data) => { stderr += data.toString(); });
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  cleanup(t, async () => {
    if (child.exitCode === null && child.signalCode === null) {
      // Resume stopped test children before cleanup; Windows has no SIGCONT.
      if (process.platform !== "win32") child.kill("SIGCONT");
      child.kill("SIGKILL");
    }
    await exited;
  });
  function reply(): Promise<Reply> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => finish(new Error(`Helper timed out: ${stderr}`)), 15_000);
      function finish(error?: Error, value?: Reply) {
        clearTimeout(timer);
        child.off("message", onMessage);
        child.off("exit", onExit);
        child.off("error", onError);
        if (error) reject(error);
        else resolve(value!);
      }
      function onMessage(value: unknown) { finish(undefined, value as Reply); }
      function onExit() { finish(new Error(`Helper exited before reply: ${stderr}`)); }
      function onError(error: Error) { finish(error); }
      child.once("message", onMessage);
      child.once("exit", onExit);
      child.once("error", onError);
    });
  }
  assert.equal((await reply()).status, "ready");
  return {
    child,
    request(command, directory) {
      const response = reply();
      child.send({ command, directory });
      return response;
    },
    async exit() {
      child.send({ command: "exit" });
      await exited;
    },
  };
}

function acquired(result: ProcessLockResult) {
  assert.equal(result.status, "acquired");
  if (result.status !== "acquired") throw new Error("Expected acquired lease");
  return result.lease;
}

function occupied(directory: string, owner: ProcessLockOwner) {
  const result = tryAcquireProcessLock(directory);
  assert.equal(result.status, "occupied");
  if (result.status === "occupied" && result.owner !== null) assert.deepEqual(result.owner, owner);
  // Windows may deny metadata reads under LockFileEx; null is legitimate evidence.
  if (process.platform !== "win32" && result.status === "occupied") assert.deepEqual(result.owner, owner);
}

test("separate same-process attempts contend, and old release cannot affect a new lease", (t) => {
  const directory = temporaryDirectory(t);
  const first = acquired(tryAcquireProcessLock(directory));
  cleanup(t, () => first.release());
  assert.equal(first.owner.pid, process.pid);
  occupied(directory, first.owner);
  first.release();
  first.release();

  const next = acquired(tryAcquireProcessLock(directory));
  cleanup(t, () => next.release());
  assert.notEqual(next.owner.claimId, first.owner.claimId);
  first.release();
  occupied(directory, next.owner);

  // Locality: protecting one directory does not serialize unrelated directories.
  const independent = acquired(tryAcquireProcessLock(temporaryDirectory(t)));
  independent.release();
  next.release();
});

test("invalid local directories throw instead of being reported as occupied", (t) => {
  const directory = temporaryDirectory(t);
  const file = path.join(directory, "ordinary-file");
  writeFileSync(file, "not a directory");
  assert.throws(() => tryAcquireProcessLock(file));
});

test("simultaneous first starts have one winner; losing processes exiting leave it protected", async (t) => {
  const directory = temporaryDirectory(t);
  const helpers = await Promise.all(Array.from({ length: 8 }, () => helper(t)));
  const results = await Promise.all(helpers.map((peer) => peer.request("acquire", directory)));
  const winners = results.flatMap((result, index) => result.status === "acquired" ? [index] : []);
  assert.equal(winners.length, 1, JSON.stringify(results));
  for (const result of results) assert.ok(["acquired", "occupied"].includes(result.status));
  const winner = winners[0]!;
  const owner = results[winner]!.owner!;
  assert.equal(owner.pid, helpers[winner]!.child.pid);
  await Promise.all(helpers.filter((_, index) => index !== winner).map((peer) => peer.exit()));
  occupied(directory, owner);

  await helpers[winner]!.request("release");
  const replacement = acquired(tryAcquireProcessLock(directory));
  cleanup(t, () => replacement.release());
  // The former winner's second release/exit has no authority over the replacement.
  await helpers[winner]!.request("release");
  await helpers[winner]!.exit();
  occupied(directory, replacement.owner);
  replacement.release();
});

test("abrupt owner death permits racing replacements with exactly one surviving winner", async (t) => {
  const directory = temporaryDirectory(t);
  const original = await helper(t);
  assert.equal((await original.request("acquire", directory)).status, "acquired");
  const death = once(original.child, "exit");
  original.child.kill("SIGKILL");
  await death;

  const replacements = await Promise.all(Array.from({ length: 8 }, () => helper(t)));
  const results = await Promise.all(replacements.map((peer) => peer.request("acquire", directory)));
  const winners = results.flatMap((result, index) => result.status === "acquired" ? [index] : []);
  assert.equal(winners.length, 1, JSON.stringify(results));
  const winner = winners[0]!;
  await Promise.all(replacements.filter((_, index) => index !== winner).map((peer) => peer.exit()));
  occupied(directory, results[winner]!.owner!);
  // Graceful process exit without release also abandons no lasting ownership.
  await replacements[winner]!.exit();
  acquired(tryAcquireProcessLock(directory)).release();
});

test("a stopped owner retains its lease and can resume and release", { skip: process.platform === "win32" }, async (t) => {
  const directory = temporaryDirectory(t);
  const peer = await helper(t);
  const result = await peer.request("acquire", directory);
  assert.equal(result.status, "acquired");
  assert.ok(peer.child.kill("SIGSTOP"));
  occupied(directory, result.owner!);
  assert.ok(peer.child.kill("SIGCONT"));
  await peer.request("release");
  acquired(tryAcquireProcessLock(directory)).release();
});

test("detached spawned descendants do not inherit ownership after owner death", async (t) => {
  const directory = temporaryDirectory(t);
  const peer = await helper(t);
  assert.equal((await peer.request("acquire", directory)).status, "acquired");
  const descendant = await peer.request("descendant");
  assert.ok(descendant.pid);
  cleanup(t, () => {
    try { process.kill(descendant.pid!, "SIGKILL"); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
  });
  const death = once(peer.child, "exit");
  peer.child.kill("SIGKILL");
  await death;
  assert.doesNotThrow(() => process.kill(descendant.pid!, 0));
  acquired(tryAcquireProcessLock(directory)).release();
});
