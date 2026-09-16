import test from "node:test";
import assert from "node:assert/strict";
import { sessionIdPrefixes } from "../index.ts";
import type { SessionInfo } from "../types.ts";

function session(id: string): SessionInfo {
  return { id, cwd: "/w", model: "m", pid: 1, startedAt: 0, lastActivity: 0 };
}

test("sessionIdPrefixes keeps stable ids with unique tails whole", () => {
  // The last segment carries the unique tail; slicing at the uniqueness
  // minimum would display "t2" for "t226".
  const prefixes = sessionIdPrefixes([
    session("mistfall-remote:game:t226"),
    session("mistfall-remote:game:t137"),
    session("mistfall-remote:game:t153"),
  ]);
  assert.deepEqual([...prefixes.values()].sort(), [
    "mistfall-remote:game:t137",
    "mistfall-remote:game:t153",
    "mistfall-remote:game:t226",
  ]);
});

test("sessionIdPrefixes stays unique, bounded, and clean for uuid-style ids", () => {
  const ids = [
    "0199f7c1a2b3c4d5-1111-4aaa-9bbb-cccccccccccc",
    "0199f7c1a2b3c4d5-2222-4aaa-9bbb-cccccccccccc",
  ];
  const prefixes = sessionIdPrefixes(ids.map(session));
  const values = [...prefixes.values()];
  assert.equal(values.length, new Set(values).size, "prefixes stay mutually unique");
  assert.ok(values.every((value) => value.length >= 8));
  assert.equal(
    values.every((value) => !value.endsWith("-")),
    true,
    "prefixes never dangle on a separator",
  );
});

test("sessionIdPrefixes disambiguates to the next separator when the minimum lands mid-segment", () => {
  const prefixes = sessionIdPrefixes([
    session("mistfall-remote:game:t226"),
    session("mistfall-remote:work:t226"),
  ]);
  // The shared prefix is "mistfall-remote:"; the minimum lands inside "game"/
  // "work", so both extend to the next ":" for a clean distinguishing prefix.
  assert.deepEqual([...prefixes.values()].sort(), [
    "mistfall-remote:game",
    "mistfall-remote:work",
  ]);
});

test("sessionIdPrefixes emits at least 8 characters for ids sharing no prefix", () => {
  const prefixes = sessionIdPrefixes([
    session("aaaaaaaa-1111"),
    session("bbbbbbbb-2222"),
  ]);
  assert.deepEqual([...prefixes.values()].sort(), ["aaaaaaaa", "bbbbbbbb"]);
});
