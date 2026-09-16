import test from "node:test";
import assert from "node:assert/strict";
import { Type } from "typebox";
import { defineTool, sessionCompactFailuresReachExtensions, sessionInfoChangesReachExtensions, StringEnum } from "./pi-compat.ts";

test("host capability detection keeps upstream and unknown launchers on the name fallback", () => {
  assert.equal(sessionInfoChangesReachExtensions("/app/node_modules/@mariozechner/pi-coding-agent/dist/cli.js"), false);
  assert.equal(sessionInfoChangesReachExtensions("C:\\app\\node_modules\\@mariozechner\\pi-coding-agent\\dist\\cli.js"), false);
  assert.equal(sessionInfoChangesReachExtensions("/opt/pi-standalone"), false);
  assert.equal(sessionInfoChangesReachExtensions("/app/node_modules/@earendil-works/pi-coding-agent/dist/cli.js"), true);
  assert.equal(sessionInfoChangesReachExtensions("C:\\app\\node_modules\\@earendil-works\\pi-coding-agent\\dist\\cli.js"), true);
});

test("compaction presence requires a host with extension failure events", () => {
  const fork = "/app/node_modules/@earendil-works/pi-coding-agent/dist/cli.js";
  assert.equal(sessionCompactFailuresReachExtensions(fork, "0.80.3"), false);
  assert.equal(sessionCompactFailuresReachExtensions(fork, "0.84.9"), false);
  assert.equal(sessionCompactFailuresReachExtensions(fork, "0.85.0"), true);
  assert.equal(sessionCompactFailuresReachExtensions(fork, "0.85.1"), true);
  assert.equal(sessionCompactFailuresReachExtensions(fork, "1.0.0"), true);
  assert.equal(sessionCompactFailuresReachExtensions("/app/node_modules/@mariozechner/pi-coding-agent/dist/cli.js", "0.85.1"), false);
  assert.equal(sessionCompactFailuresReachExtensions("/opt/pi-standalone", "0.85.1"), false);
});

test("host-neutral tool helpers preserve definitions and portable string enums", () => {
  const parameters = Type.Object({ action: StringEnum(["list", "send"] as const) });
  const tool = {
    name: "example",
    label: "Example",
    description: "Example tool",
    parameters,
    async execute() {
      return { content: [{ type: "text" as const, text: "ok" }], details: {} };
    },
  };

  assert.equal(defineTool(tool), tool);
  assert.deepEqual(parameters.properties.action.enum, ["list", "send"]);
});
