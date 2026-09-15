import test from "node:test";
import assert from "node:assert/strict";
import { Type } from "typebox";
import { defineTool, sessionInfoChangesReachExtensions, StringEnum } from "./pi-compat.ts";

test("host capability detection keeps upstream and unknown launchers on the name fallback", () => {
  assert.equal(sessionInfoChangesReachExtensions("/app/node_modules/@mariozechner/pi-coding-agent/dist/cli.js"), false);
  assert.equal(sessionInfoChangesReachExtensions("C:\\app\\node_modules\\@mariozechner\\pi-coding-agent\\dist\\cli.js"), false);
  assert.equal(sessionInfoChangesReachExtensions("/opt/pi-standalone"), false);
  assert.equal(sessionInfoChangesReachExtensions("/app/node_modules/@earendil-works/pi-coding-agent/dist/cli.js"), true);
  assert.equal(sessionInfoChangesReachExtensions("C:\\app\\node_modules\\@earendil-works\\pi-coding-agent\\dist\\cli.js"), true);
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
