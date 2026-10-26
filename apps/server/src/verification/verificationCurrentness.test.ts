import {
  CommandId,
  EnvironmentId,
  ProjectId,
  RunId,
  ThreadId,
  verificationMayClaimCurrentPass,
  type SourceIdentity,
  type VerificationRecord,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import {
  classifyVerificationCurrentness,
  type VerificationCurrentnessObservation,
} from "./verificationCurrentness.ts";

function fixture() {
  const source: SourceIdentity = {
    checkoutIdentity: {
      environmentId: EnvironmentId.make("test"),
      checkoutKey: "checkout",
      canonicalCwd: "/test/checkout",
      filesystemIdentity: { device: "1", file: "2" },
      gitDir: "/test/checkout/.git",
      gitCommonDir: "/test/checkout/.git",
    },
    checkpointRef: "refs/checkpoints/test",
    head: "head",
    branch: "main",
    indexDigest: "index",
    digest: "same-source-digest",
    coverage: { complete: true, policyRevision: "inputs-v1", gaps: [] },
    toolchainRevision: "tools-v1",
    configRevision: "config-v1",
  };
  const record: VerificationRecord = {
    verificationId: "verification",
    commandId: CommandId.make("command"),
    projectId: ProjectId.make("project"),
    threadId: ThreadId.make("thread"),
    sourceRunId: RunId.make("run"),
    environmentId: source.checkoutIdentity.environmentId,
    ownerHandle: null,
    actionSnapshot: {
      actionId: "test",
      revision: "action-v1",
      command: "test",
      shell: "/bin/sh",
      cwd: source.checkoutIdentity.canonicalCwd,
      timeoutMs: 1_000,
    },
    execution: "passed",
    currentness: "Current",
    requestedAt: "2026-10-25T12:00:00.000Z",
    startedAt: "2026-10-25T12:00:00.000Z",
    finishedAt: "2026-10-25T12:01:00.000Z",
    exitCode: 0,
    signal: null,
    terminationReason: null,
    startSource: source,
    endSource: source,
    startMutationGeneration: 0,
    endMutationGeneration: 0,
    observationState: "continuous",
    outputReferences: [],
    truncated: false,
  };
  const observation: VerificationCurrentnessObservation = {
    source,
    mutationGeneration: 0,
    observationState: "continuous",
    newerSourceRun: false,
    configRevision: source.configRevision,
    toolchainRevision: source.toolchainRevision,
    ownershipConflict: false,
  };
  return { source, record, observation };
}

describe("classifyVerificationCurrentness", () => {
  it("an edit-and-revert invalidates a verification despite equal endpoint digests", () => {
    const { record, observation } = fixture();
    expect(record.startSource?.digest).toBe(record.endSource?.digest);
    expect(
      classifyVerificationCurrentness(
        { ...record, endMutationGeneration: 2 },
        { ...observation, mutationGeneration: 2 },
      ),
    ).toBe("Stale");
  });

  it("watcher overflow or a monitoring gap produces Unknown", () => {
    const { record, observation } = fixture();
    for (const state of ["gap", "unavailable"] as const) {
      expect(
        classifyVerificationCurrentness(record, { ...observation, observationState: state }),
      ).toBe("Unknown");
      expect(
        classifyVerificationCurrentness({ ...record, observationState: state }, observation),
      ).toBe("Unknown");
    }
  });

  it("reverting later does not resurrect a stale pass", () => {
    const { record, observation } = fixture();
    expect(classifyVerificationCurrentness({ ...record, currentness: "Stale" }, observation)).toBe(
      "Stale",
    );
    expect(
      classifyVerificationCurrentness({ ...record, currentness: "Historical only" }, observation),
    ).toBe("Historical only");
  });

  it("only a passed, current result with complete continuous coverage claims a current pass", () => {
    const { record, observation } = fixture();
    expect(classifyVerificationCurrentness(record, observation)).toBe("Current");
    expect(verificationMayClaimCurrentPass(record)).toBe(true);
    for (const execution of ["queued", "running", "interrupted"] as const) {
      const candidate = { ...record, execution };
      expect(classifyVerificationCurrentness(candidate, observation)).toBe("Unknown");
      expect(verificationMayClaimCurrentPass(candidate)).toBe(false);
    }
    for (const execution of ["failed", "cancelled", "timed out"] as const) {
      expect(verificationMayClaimCurrentPass({ ...record, execution })).toBe(false);
    }
  });

  it("a newer source run or newer verification makes late results historical", () => {
    const { record, observation } = fixture();
    expect(classifyVerificationCurrentness(record, { ...observation, newerSourceRun: true })).toBe(
      "Historical only",
    );
    expect(
      classifyVerificationCurrentness(record, { ...observation, newerVerification: true }),
    ).toBe("Historical only");
  });

  it("invalidates changed configuration, tools and ownership independently of hashes", () => {
    const { record, observation } = fixture();
    for (const change of [
      { configRevision: "config-v2" },
      { configRevision: null },
      { toolchainRevision: "tools-v2" },
      { ownershipConflict: true },
    ]) {
      expect(classifyVerificationCurrentness(record, { ...observation, ...change })).toBe("Stale");
    }
  });

  it("invalidates source, HEAD, index, checkpoint, branch, coverage and checkout identity changes", () => {
    const { source, record, observation } = fixture();
    for (const change of [
      { digest: "changed" },
      { head: "new-head" },
      { branch: "other" },
      { indexDigest: "new-index" },
      { checkpointRef: "other-checkpoint" },
      { configRevision: "config-v2" },
      { toolchainRevision: "tools-v2" },
      { coverage: { ...source.coverage, policyRevision: "inputs-v2" } },
      {
        checkoutIdentity: {
          ...source.checkoutIdentity,
          filesystemIdentity: { device: "1", file: "replacement-directory" },
        },
      },
      { checkoutIdentity: { ...source.checkoutIdentity, checkoutKey: "other-checkout" } },
    ]) {
      const changed = { ...source, ...change };
      expect(classifyVerificationCurrentness(record, { ...observation, source: changed })).toBe(
        "Stale",
      );
      expect(classifyVerificationCurrentness({ ...record, endSource: changed }, observation)).toBe(
        "Stale",
      );
    }
  });

  it("keeps missing evidence and incomplete coverage unknown", () => {
    const { source, record, observation } = fixture();
    for (const change of [
      { startSource: null },
      { endSource: null },
      { startMutationGeneration: null },
      { endMutationGeneration: null },
      { startSource: { ...source, coverage: { ...source.coverage, complete: false } } },
      { endSource: { ...source, coverage: { ...source.coverage, gaps: ["unreadable-input"] } } },
    ]) {
      expect(classifyVerificationCurrentness({ ...record, ...change }, observation)).toBe(
        "Unknown",
      );
    }
    expect(classifyVerificationCurrentness(record, { ...observation, source: null })).toBe(
      "Unknown",
    );
    expect(
      classifyVerificationCurrentness(record, { ...observation, mutationGeneration: null }),
    ).toBe("Unknown");
    expect(
      classifyVerificationCurrentness(record, {
        ...observation,
        source: { ...source, coverage: { ...source.coverage, complete: false } },
      }),
    ).toBe("Unknown");
  });

  it("rechecks the generation after completion and rejects reset or malformed counters", () => {
    const { record, observation } = fixture();
    for (const generation of [1, -1, Number.NaN, Number.MAX_SAFE_INTEGER + 1]) {
      expect(
        classifyVerificationCurrentness(record, { ...observation, mutationGeneration: generation }),
      ).not.toBe("Current");
    }
    expect(
      classifyVerificationCurrentness(
        { ...record, startMutationGeneration: 4, endMutationGeneration: 4 },
        observation,
      ),
    ).toBe("Stale");
  });

  it("a known mutation stays stale even when another observation is missing", () => {
    const { record, observation } = fixture();
    expect(
      classifyVerificationCurrentness(
        { ...record, endSource: null, endMutationGeneration: null },
        { ...observation, mutationGeneration: 1 },
      ),
    ).toBe("Stale");
    expect(
      classifyVerificationCurrentness(
        { ...record, endMutationGeneration: 1, observationState: "gap" },
        { ...observation, mutationGeneration: null },
      ),
    ).toBe("Stale");
  });

  it("cannot promote incomplete or contradictory execution evidence into a current pass", () => {
    const { record, observation } = fixture();
    for (const change of [
      { exitCode: null },
      { exitCode: 1 },
      { signal: "SIGTERM" },
      { terminationReason: "unknown-outcome" },
    ]) {
      expect(classifyVerificationCurrentness({ ...record, ...change }, observation)).toBe(
        "Unknown",
      );
    }
    expect(
      classifyVerificationCurrentness(record, { ...observation, toolchainRevision: null }),
    ).toBe("Unknown");
  });

  it("a client-only unknown can recover from continuous authoritative server evidence", () => {
    const { record, observation } = fixture();
    expect(
      classifyVerificationCurrentness({ ...record, currentness: "Unknown" }, observation),
    ).toBe("Current");
    expect(
      classifyVerificationCurrentness(
        { ...record, currentness: "Unknown", observationState: "gap" },
        observation,
      ),
    ).toBe("Unknown");
  });
});
