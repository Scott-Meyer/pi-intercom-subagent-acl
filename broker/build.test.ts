import test from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { getBrokerBuildIdentity } from "./build.ts";

function installation(root: string): string {
  mkdirSync(join(root, "broker"), { recursive: true });
  writeFileSync(join(root, "package.json"), JSON.stringify({
    version: "1.0.0", files: ["config.ts", "broker/broker.ts", "ui/**/*.ts"],
  }));
  writeFileSync(join(root, "config.ts"), "export const enabled = true;\n");
  writeFileSync(join(root, "broker/broker.ts"), "export const broker = true;\n");
  return root;
}

test("identical installations carry the same build identity independent of path", () => {
  const scratch = mkdtempSync(join(tmpdir(), "parley-build-"));
  try {
    const left = getBrokerBuildIdentity(installation(join(scratch, "left")));
    const right = getBrokerBuildIdentity(installation(join(scratch, "right")));
    assert.deepEqual(left, right);
    assert.equal(left.packageVersion, "1.0.0");
    assert.match(left.sourceId, /^[a-f0-9]{64}$/);
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test("build identity changes with packaged source without requiring a version edit", () => {
  const scratch = mkdtempSync(join(tmpdir(), "parley-build-"));
  try {
    const root = installation(scratch);
    const captured = getBrokerBuildIdentity(root);
    writeFileSync(join(root, "config.ts"), "export const enabled = false;\n");
    const changed = getBrokerBuildIdentity(root);
    assert.equal(changed.packageVersion, captured.packageVersion);
    assert.notEqual(changed.sourceId, captured.sourceId);
    assert.equal(Object.isFrozen(captured), true);
    assert.notDeepEqual(captured, changed, "a captured identity does not follow later disk edits");
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test("UI, tests and generated files are not broker build inputs", () => {
  const scratch = mkdtempSync(join(tmpdir(), "parley-build-"));
  try {
    const root = installation(scratch);
    const before = getBrokerBuildIdentity(root);
    mkdirSync(join(root, "ui"));
    writeFileSync(join(root, "ui/card.ts"), "changed presentation");
    writeFileSync(join(root, "broker/broker.test.ts"), "changed test");
    writeFileSync(join(root, "broker/output.log"), "generated output");
    assert.deepEqual(getBrokerBuildIdentity(root), before);
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test("missing packaged source fails rather than yielding an incomplete build identity", () => {
  const scratch = mkdtempSync(join(tmpdir(), "parley-build-"));
  try {
    const root = installation(scratch);
    rmSync(join(root, "config.ts"));
    assert.throws(() => getBrokerBuildIdentity(root), { code: "ENOENT" });
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});
