import { randomUUID } from "node:crypto";
import {
  closeSync,
  fstatSync,
  ftruncateSync,
  mkdirSync,
  openSync,
  readSync,
  writeSync,
} from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";

// Keep the untyped native dependency behind this module's descriptor-only adapter.
interface NativeFileLocks {
  tryLock(fd: number): boolean;
  unlock(fd: number): void;
}
const nativeLocks = createRequire(import.meta.url)("fs-native-extensions") as NativeFileLocks;

export interface ProcessLockOwner {
  readonly pid: number;
  /** Identifies this acquisition, not merely a potentially reused PID. */
  readonly claimId: string;
  /** Diagnostic timestamp only; ownership never expires by age. */
  readonly acquiredAt: string;
}

export interface ProcessLockLease {
  readonly owner: ProcessLockOwner;
  /** Release only this lease. Repeated calls, including after another acquisition, do nothing. */
  release(): void;
}

export type ProcessLockResult =
  | { readonly status: "acquired"; readonly lease: ProcessLockLease }
  | {
      readonly status: "occupied";
      /**
       * Last published diagnostic identity, NOT proof of the current holder's PID.
       * May be the previous holder during handoff, or null during first publication,
       * after damaged metadata, or when Windows denies reads of the locked file.
       * The occupied status itself comes exclusively from the kernel lock attempt.
       */
      readonly owner: ProcessLockOwner | null;
    };

/**
 * Try once, without waiting, to exclusively own a local directory for this process.
 * Creates the directory if necessary. Retain the lease until all protected work ends;
 * a lease does not keep Node's event loop alive. Errors other than lock contention
 * throw (including unsupported native binaries, kernels, or filesystem locking).
 *
 * Protocol: open a permanent `.process.lock` without truncation, request an exclusive
 * whole-file kernel lock, and only then replace its diagnostic JSON. The descriptor
 * remains private and open for the lease lifetime. Release unlocks/closes only that
 * descriptor and never removes or renames the file. A losing attempt only closes its
 * own descriptor. Thus simultaneous starts have one winner, paused owners retain
 * ownership indefinitely, and process death releases ownership without PID checks,
 * age expiry, stale-file deletion, or an unlink/recreate ABA race. Node opens the
 * descriptor close-on-exec; ordinary spawned children do not inherit the lease.
 *
 * Supported by fs-native-extensions on macOS (flock), Linux >=3.15 (OFD fcntl locks),
 * and Windows (LockFileEx), with its available Node prebuilds. This is a cooperative
 * LOCAL lock, not a network-filesystem/distributed lock or a security boundary.
 * All participants must use this primitive in the same stable, trusted directory.
 * Do not unlink/rename/replace the lock file or its directory, even while apparently
 * idle: another process can already have opened it. External directory cleanup,
 * non-cooperating writers, explicit descriptor inheritance, and native fork without
 * exec are outside this contract. Unsupported locks fail rather than use a TTL.
 */
export function tryAcquireProcessLock(directory: string): ProcessLockResult {
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const fd = openSync(path.join(directory, ".process.lock"), "a+", 0o600);
  let retained = false;
  try {
    if (!fstatSync(fd).isFile()) throw new Error("Process lock must be a regular file");
    if (!nativeLocks.tryLock(fd)) {
      return Object.freeze({ status: "occupied", owner: readDiagnosticOwner(fd) });
    }

    const owner: ProcessLockOwner = Object.freeze({
      pid: process.pid,
      claimId: randomUUID(),
      acquiredAt: new Date().toISOString(),
    });
    // This descriptor is the owner. Metadata is allowed to be absent/partial after
    // a crash; no reader ever interprets JSON as authority to acquire or clean up.
    const metadata = Buffer.from(`${JSON.stringify(owner)}\n`);
    ftruncateSync(fd, 0);
    let written = 0;
    while (written < metadata.length) {
      const count = writeSync(fd, metadata, written, metadata.length - written);
      if (count === 0) throw new Error("Could not publish process lock identity");
      written += count;
    }

    let descriptor: number | null = fd;
    const lease: ProcessLockLease = Object.freeze({
      owner,
      release() {
        if (descriptor === null) return;
        const ownedDescriptor = descriptor;
        // Clear before closing: even if release throws, never operate on a reused fd.
        descriptor = null;
        try {
          nativeLocks.unlock(ownedDescriptor);
        } finally {
          closeSync(ownedDescriptor);
        }
      },
    });
    retained = true;
    return Object.freeze({ status: "acquired", lease });
  } finally {
    // Closing also releases any lock acquired before publication failed.
    if (!retained) closeSync(fd);
  }
}

function readDiagnosticOwner(fd: number): ProcessLockOwner | null {
  try {
    // Bounded, positional read through the contender's existing descriptor. No
    // pathname reopening or unbounded parsing, and unreadable metadata is harmless.
    const bytes = Buffer.alloc(4096);
    const count = readSync(fd, bytes, 0, bytes.length, 0);
    if (count === 0 || count === bytes.length) return null;
    const value: unknown = JSON.parse(bytes.subarray(0, count).toString("utf8"));
    if (typeof value !== "object" || value === null) return null;
    const owner = value as Record<string, unknown>;
    if (
      typeof owner.pid !== "number" || !Number.isSafeInteger(owner.pid) || owner.pid <= 0 ||
      typeof owner.claimId !== "string" || !/^[0-9a-f-]{36}$/.test(owner.claimId) ||
      typeof owner.acquiredAt !== "string" || !Number.isFinite(Date.parse(owner.acquiredAt))
    ) return null;
    return Object.freeze({ pid: owner.pid, claimId: owner.claimId, acquiredAt: owner.acquiredAt });
  } catch {
    return null;
  }
}
