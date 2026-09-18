import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export interface BrokerBuildIdentity {
  /** Package version captured from this installation at broker startup. */
  readonly packageVersion: string;
  /** SHA-256 of package metadata and packaged broker/shared TypeScript sources.
   * Identifies source on disk at startup, not dependency binaries or host code. */
  readonly sourceId: string;
}

/** Describe a broker installation independently of its running process.
 * The installed package manifest defines the broker/shared source set; UI,
 * skills, tests and generated files are not broker build inputs. Missing
 * packaged sources fail rather than reporting an incomplete fingerprint.
 * A running broker captures this value once, so later edits do not change its
 * reported identity until it is restarted. */
export function getBrokerBuildIdentity(
  extensionDir: string = join(dirname(fileURLToPath(import.meta.url)), ".."),
): BrokerBuildIdentity {
  const packageRaw = readFileSync(join(extensionDir, "package.json"));
  const manifest: unknown = JSON.parse(packageRaw.toString("utf8"));
  if (!manifest || typeof manifest !== "object" || Array.isArray(manifest)) {
    throw new Error("Invalid Parley package manifest");
  }
  const { version, files } = manifest as Record<string, unknown>;
  if (typeof version !== "string" || !version.trim() || !Array.isArray(files)) {
    throw new Error("Parley package manifest requires a version and source file list");
  }
  const inputs = [...new Set(files.filter((file): file is string =>
    typeof file === "string"
    && file.endsWith(".ts")
    && !file.includes("*")
    && !file.startsWith("/")
    && !file.includes("\\")
    && !file.split("/").includes("..")
    && (!file.includes("/") || file.startsWith("broker/"))
  ))].sort();
  if (!inputs.includes("broker/broker.ts")) {
    throw new Error("Parley package manifest must include the broker entrypoint");
  }
  const hash = createHash("sha256");
  const addInput = (name: string, bytes: Buffer) => {
    hash.update(name).update("\0").update(String(bytes.length)).update("\0").update(bytes);
  };
  addInput("package.json", packageRaw);
  for (const input of inputs) addInput(input, readFileSync(join(extensionDir, input)));
  return Object.freeze({ packageVersion: version, sourceId: hash.digest("hex") });
}
