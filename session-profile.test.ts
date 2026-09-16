import test from "node:test";
import assert from "node:assert/strict";
import { isSessionRegistration } from "./broker/protocol.ts";
import {
  isValidSessionDescription,
  isValidSessionName,
  normalizeSelfProfileUpdate,
  SESSION_DESCRIPTION_MAX_LENGTH,
} from "./session-profile.ts";

test("self profile normalization keeps concise one-line descriptions", () => {
  const result = normalizeSelfProfileUpdate({
    name: "  review   helper  ",
    description: "  Reviewing\npeer   discovery profile behavior today  ",
  });

  assert.deepEqual(result, {
    ok: true,
    profile: {
      name: "review helper",
      description: "Reviewing peer discovery profile behavior today",
    },
  });
  assert.equal(result.ok && isValidSessionDescription(result.profile.description), true);
  assert.deepEqual(normalizeSelfProfileUpdate({ description: null }), {
    ok: true,
    profile: { description: null },
  });
});

test("raw registrations reject unnormalized or control-bearing descriptions", () => {
  const registration = {
    name: "worker",
    description: "Reviewing raw broker profile registration behavior",
    cwd: "/test",
    model: "test",
    pid: 1,
    startedAt: 1,
    lastActivity: 1,
  };
  assert.equal(isSessionRegistration(registration), true);
  assert.equal(isSessionRegistration({ ...registration, description: "Reviewing  raw broker profile registration behavior" }), false);
  assert.equal(isSessionRegistration({ ...registration, description: "Reviewing raw\u202E broker profile registration behavior" }), false);
  assert.equal(isSessionRegistration({ ...registration, name: "worker\u001b[2J" }), false);
  assert.equal(isSessionRegistration({ ...registration, name: "worker\u202E" }), false);
  assert.equal(isSessionRegistration({ ...registration, name: "oqs1.impersonating-a-remote-row" }), false, "the origin-qualified namespace is reserved from local names");
  assert.equal(isValidSessionName("oqs1.anything"), false);
  assert.equal(isValidSessionName("ordinary worker"), true);
});

test("self profile validation rejects descriptions outside word and character bounds", () => {
  assert.deepEqual(normalizeSelfProfileUpdate({ description: "Only four words here" }), {
    ok: false,
    error: "profile.description must contain 5-9 words (received 4).",
  });
  assert.deepEqual(normalizeSelfProfileUpdate({ description: "One two three four five six seven eight nine ten" }), {
    ok: false,
    error: "profile.description must contain 5-9 words (received 10).",
  });
  const oversized = `${"a".repeat(SESSION_DESCRIPTION_MAX_LENGTH)} b c d e`;
  assert.deepEqual(normalizeSelfProfileUpdate({ description: oversized }), {
    ok: false,
    error: `profile.description must be at most ${SESSION_DESCRIPTION_MAX_LENGTH} characters.`,
  });
  assert.deepEqual(normalizeSelfProfileUpdate({ description: "Reviewing peer\u001b[2J discovery profile behavior" }), {
    ok: false,
    error: "profile.description contains unsupported control characters.",
  });
  assert.deepEqual(normalizeSelfProfileUpdate({ name: "review\u202Eworker" }), {
    ok: false,
    error: "profile.name contains unsupported control characters.",
  });
});
