import test from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  FEDERATION_ORIGIN_FILE,
  loadPersistedFederationOrigin,
  mintFederationOriginId,
  persistFederationOrigin,
} from "./federation-origin.ts";

test("canonical origins persist durably and round-trip across broker restarts", () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-parley-federation-origin-"));
  try {
    assert.equal(loadPersistedFederationOrigin(dir), undefined, "nothing is persisted before first federation use");

    const minted = mintFederationOriginId();
    assert.match(minted, /^install:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);

    persistFederationOrigin(dir, { originId: minted, mintedAt: 1789580000000 });
    assert.deepEqual(loadPersistedFederationOrigin(dir), { originId: minted, mintedAt: 1789580000000 });

    // A second broker start on the same install reads the same identity.
    persistFederationOrigin(dir, { originId: "host:adopted-machine", mintedAt: 1789580000001 });
    assert.equal(loadPersistedFederationOrigin(dir)?.originId, "host:adopted-machine");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a corrupt or hostile origin file never blocks the broker", () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-parley-federation-origin-corrupt-"));
  try {
    mkdirSync(dir, { recursive: true });
    for (const [name, content] of [
      ["not json at all", "garbage{"],
      ["array payload", "[]"],
      ["non-canonical id", JSON.stringify({ originId: "Not A Valid Origin!!", mintedAt: 1 })],
      ["missing mintedAt", JSON.stringify({ originId: "install:abc" })],
      ["negative mintedAt", JSON.stringify({ originId: "install:abc", mintedAt: -1 })],
      ["fractional mintedAt", JSON.stringify({ originId: "install:abc", mintedAt: 1.5 })],
      ["extra fields are ignored safely", JSON.stringify({ originId: "host:penguin", mintedAt: 5, injected: true })],
    ] as Array<[string, string]>) {
      writeFileSync(join(dir, FEDERATION_ORIGIN_FILE), content);
      if (name === "extra fields are ignored safely") {
        assert.deepEqual(loadPersistedFederationOrigin(dir), { originId: "host:penguin", mintedAt: 5 }, name);
      } else {
        assert.equal(loadPersistedFederationOrigin(dir), undefined, name);
      }
    }
    writeFileSync(join(dir, FEDERATION_ORIGIN_FILE), JSON.stringify({ originId: "host:penguin", mintedAt: 5 }));
    assert.equal(loadPersistedFederationOrigin(dir)?.originId, "host:penguin");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("persisted origin files carry restrictive permissions and no temp leftovers", () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-parley-federation-origin-perms-"));
  try {
    persistFederationOrigin(dir, { originId: "host:penguin", mintedAt: 1 }, "darwin");
    const raw = readFileSync(join(dir, FEDERATION_ORIGIN_FILE), "utf8");
    assert.equal(JSON.parse(raw).originId, "host:penguin");
    if (process.platform !== "win32") {
      const mode = statSync(join(dir, FEDERATION_ORIGIN_FILE)).mode & 0o777;
      assert.equal(mode, 0o600, "the origin identity file is owner-only");
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
