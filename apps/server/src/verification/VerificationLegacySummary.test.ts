import {
  CommandId,
  EnvironmentId,
  OrchestrationV2TurnItemJson,
  ProjectId,
  ThreadId,
  type SourceIdentity,
  type VerificationRecord,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { formatVerificationLegacySummary } from "./VerificationLegacySummary.ts";

const source: SourceIdentity = {
  checkoutIdentity: {
    environmentId: EnvironmentId.make("machine"),
    checkoutKey: "checkout",
    canonicalCwd: "/repo",
    filesystemIdentity: { device: "1", file: "2" },
    gitDir: null,
    gitCommonDir: null,
  },
  checkpointRef: "checkpoint",
  head: "head",
  branch: "feature",
  indexDigest: "index",
  digest: "fingerprint",
  coverage: { complete: true, policyRevision: "policy", gaps: [] },
  toolchainRevision: "tools",
  configRevision: "config",
};
const record: VerificationRecord = {
  verificationId: "verification",
  commandId: CommandId.make("command"),
  projectId: ProjectId.make("project"),
  threadId: ThreadId.make("thread"),
  sourceRunId: null,
  environmentId: EnvironmentId.make("machine"),
  ownerHandle: null,
  actionSnapshot: {
    actionId: "tests",
    revision: "action-revision",
    command: "vp test run src/logic.test.ts",
    shell: "/bin/sh",
    cwd: "/repo",
    timeoutMs: 1000,
  },
  execution: "passed",
  currentness: "Current",
  requestedAt: "2026-10-25T00:00:00Z",
  startedAt: "2026-10-25T00:00:00Z",
  finishedAt: "2026-10-25T00:00:01Z",
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
const decodeLegacyItem = Schema.decodeUnknownSync(OrchestrationV2TurnItemJson);

describe("legacy verification summaries", () => {
  it.each(["Stale", "Unknown", "Historical only"] as const)(
    "never calls %s evidence a current pass",
    (currentness) => {
      const text = formatVerificationLegacySummary({ ...record, currentness });
      expect(text).not.toContain("Checks passed for current changes");
      expect(text).toContain(currentness);
      expect(text).toContain("Environment: machine");
      expect(text).toContain("Command: vp test run src/logic.test.ts");
    },
  );

  it("retains actual failure text and bounded output without claiming deployment", () => {
    const text = formatVerificationLegacySummary(
      { ...record, execution: "failed", exitCode: 1, truncated: true },
      "expected 4, received 5\n".repeat(1000),
    );
    expect(text).toContain("Verification: failed");
    expect(text).toContain("expected 4, received 5");
    expect(text).toContain("Output was truncated");
    expect(text.length).toBeLessThanOrEqual(4000);
    expect(text).not.toContain("Checks passed for current changes");
  });

  it("uses the established stock-client system notice shape", () => {
    const item = decodeLegacyItem({
      type: "system_notice",
      id: "notice",
      threadId: "thread",
      runId: null,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 0,
      status: "completed",
      title: "Verification evidence",
      startedAt: null,
      completedAt: null,
      updatedAt: "2026-10-25T00:00:01Z",
      message: formatVerificationLegacySummary(record),
    });
    expect(item.type).toBe("system_notice");
    if (item.type === "system_notice")
      expect(item.message).toContain("Checks passed for current changes");
  });
});
