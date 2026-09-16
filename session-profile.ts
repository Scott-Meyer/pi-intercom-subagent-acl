export const SESSION_DESCRIPTION_MIN_WORDS = 5;
export const SESSION_DESCRIPTION_MAX_WORDS = 9;
export const SESSION_DESCRIPTION_MAX_LENGTH = 96;
export const SESSION_PROFILE_NAME_MAX_LENGTH = 64;

export interface SelfProfileUpdate {
  name?: string;
  description?: string | null;
}

const UNSAFE_CONTROL_CHARACTERS = /[\p{Cc}\p{Cf}]/u;

function normalizeInlineText(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function containsUnsafeControlCharacters(value: string): boolean {
  return UNSAFE_CONTROL_CHARACTERS.test(value);
}

export function isValidSessionName(value: unknown): value is string {
  return typeof value === "string" && !containsUnsafeControlCharacters(value);
}

export function normalizeSessionDescription(value: string): string {
  return normalizeInlineText(value);
}

export function sessionDescriptionWordCount(value: string): number {
  const normalized = normalizeSessionDescription(value);
  return normalized ? normalized.split(" ").length : 0;
}

export function isValidSessionDescription(value: unknown): value is string {
  if (typeof value !== "string") return false;
  const normalized = normalizeSessionDescription(value);
  const words = sessionDescriptionWordCount(normalized);
  return normalized === value
    && !containsUnsafeControlCharacters(normalized)
    && normalized.length <= SESSION_DESCRIPTION_MAX_LENGTH
    && words >= SESSION_DESCRIPTION_MIN_WORDS
    && words <= SESSION_DESCRIPTION_MAX_WORDS;
}

export function normalizeSelfProfileUpdate(value: SelfProfileUpdate):
  | { ok: true; profile: SelfProfileUpdate }
  | { ok: false; error: string } {
  const profile: SelfProfileUpdate = {};

  if (value.name !== undefined) {
    const name = normalizeInlineText(value.name);
    if (!name) return { ok: false, error: "profile.name must not be empty." };
    if (containsUnsafeControlCharacters(name)) {
      return { ok: false, error: "profile.name contains unsupported control characters." };
    }
    if (name.length > SESSION_PROFILE_NAME_MAX_LENGTH) {
      return { ok: false, error: `profile.name must be at most ${SESSION_PROFILE_NAME_MAX_LENGTH} characters.` };
    }
    profile.name = name;
  }

  if (value.description === null) {
    profile.description = null;
  } else if (value.description !== undefined) {
    const description = normalizeSessionDescription(value.description);
    const words = sessionDescriptionWordCount(description);
    if (containsUnsafeControlCharacters(description)) {
      return { ok: false, error: "profile.description contains unsupported control characters." };
    }
    if (description.length > SESSION_DESCRIPTION_MAX_LENGTH) {
      return { ok: false, error: `profile.description must be at most ${SESSION_DESCRIPTION_MAX_LENGTH} characters.` };
    }
    if (words < SESSION_DESCRIPTION_MIN_WORDS || words > SESSION_DESCRIPTION_MAX_WORDS) {
      return {
        ok: false,
        error: `profile.description must contain ${SESSION_DESCRIPTION_MIN_WORDS}-${SESSION_DESCRIPTION_MAX_WORDS} words (received ${words}).`,
      };
    }
    profile.description = description;
  }

  return { ok: true, profile };
}
