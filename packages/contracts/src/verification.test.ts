import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  VerificationConfig,
  VerificationInputPattern,
  VerificationTimeout,
  VerificationRecord,
  type SourceIdentity,
  verificationMayClaimCurrentPass,
} from "./verification.ts";
import { CommandId, EnvironmentId, ProjectId, ThreadId } from "./baseSchemas.ts";
import {
  ServerSettings,
  ServerSettingsPatch,
  isNullableProjectSettingsOverride,
} from "./settings.ts";

const decodeSettings = Schema.decodeUnknownSync(ServerSettings);
const decodePatch = Schema.decodeUnknownSync(ServerSettingsPatch);
const decodeConfig = Schema.decodeUnknownSync(VerificationConfig);
const decodeTimeout = Schema.decodeUnknownSync(VerificationTimeout);
const decodePattern = Schema.decodeUnknownSync(VerificationInputPattern);
const decodeRecord = Schema.decodeUnknownSync(VerificationRecord);

const source: SourceIdentity = {
  checkoutIdentity: {
    environmentId: EnvironmentId.make("env"),
    checkoutKey: "checkout",
    canonicalCwd: "/repo",
    filesystemIdentity: { device: "1", file: "2" },
    gitDir: null,
    gitCommonDir: null,
  },
  checkpointRef: null,
  head: "head",
  branch: "branch",
  indexDigest: "index",
  digest: "source",
  coverage: { complete: true, policyRevision: "policy", gaps: [] },
  toolchainRevision: "toolchain",
  configRevision: "config",
};
const pass = {
  execution: "passed",
  currentness: "Current",
  observationState: "continuous",
  startSource: source,
  endSource: source,
  exitCode: 0,
  signal: null,
  startedAt: "2026-10-25T00:00:00Z",
  finishedAt: "2026-10-25T00:00:01Z",
  startMutationGeneration: 0,
  endMutationGeneration: 0,
} as const;

describe("verification contracts", () => {
  it("keeps ownership and automatic verification opt-in for legacy settings", () => {
    const settings = decodeSettings({});
    expect(settings.workspaceOwnership).toBe("off");
    expect(settings.verification).toBeNull();
    const patch = decodePatch({
      projectSettingsOverrides: { project: { workspaceOwnership: "enforce", verification: null } },
    });
    expect(patch.projectSettingsOverrides?.[ProjectId.make("project")]?.workspaceOwnership).toBe(
      "enforce",
    );
    expect(isNullableProjectSettingsOverride("verification")).toBe(true);
  });
  it("requires an explicit action and finite bounded timeout", () => {
    const decode = decodeTimeout;
    for (const timeout of [0, 999, 86_400_001, Infinity]) expect(() => decode(timeout)).toThrow();
    const config = decodeConfig({
      actionId: "test",
      inputs: { include: ["**/*"], exclude: ["dist/**"], nonGit: "fingerprint" },
    });
    expect(config.automatic).toBe(false);
    expect(config.timeoutMs).toBe(900_000);
    expect(() => decodeConfig({ ...config, actionId: "" })).toThrow();
  });

  it("rejects absolute inputs and parent traversal", () => {
    const decode = decodePattern;
    for (const path of [
      "/etc/passwd",
      "../secret",
      "src/../../secret",
      "C:\\secret",
      "\\\\host\\share",
      "{..}/secret",
      "src/{a,..}/secret",
      "src/[.][.]/secret",
    ]) {
      expect(() => decode(path)).toThrow();
    }
    expect(decode("src/**/*.ts")).toBe("src/**/*.ts");
    expect(decode(" trailing.txt ")).toBe(" trailing.txt ");
  });

  it("cannot turn a provider completion or missing coverage into a current pass", () => {
    expect(
      verificationMayClaimCurrentPass({
        ...pass,
        execution: "passed",
        currentness: "Current",
        observationState: "continuous",
        startSource: null,
        endSource: null,
      }),
    ).toBe(false);
  });

  it("withholds the current-pass claim for contradictory execution or source evidence", () => {
    expect(verificationMayClaimCurrentPass(pass)).toBe(true);
    for (const record of [
      { ...pass, exitCode: 1 },
      { ...pass, signal: "SIGTERM" },
      { ...pass, startMutationGeneration: null },
      { ...pass, endMutationGeneration: 1 },
      { ...pass, startedAt: null },
      { ...pass, finishedAt: null },
      { ...pass, endSource: { ...source, digest: "edited" } },
      { ...pass, endSource: { ...source, configRevision: "new-config" } },
      { ...pass, endSource: { ...source, toolchainRevision: "new-tool" } },
      {
        ...pass,
        endSource: { ...source, coverage: { ...source.coverage, gaps: ["missing-input"] } },
      },
    ])
      expect(verificationMayClaimCurrentPass(record)).toBe(false);
  });

  it("rejects a contradictory current pass on the wire while preserving independent failure currentness", () => {
    const record = {
      ...pass,
      verificationId: "verification",
      commandId: CommandId.make("command"),
      projectId: ProjectId.make("project"),
      threadId: ThreadId.make("thread"),
      sourceRunId: null,
      environmentId: EnvironmentId.make("env"),
      ownerHandle: null,
      actionSnapshot: {
        actionId: "test",
        revision: "revision",
        command: "vp test",
        shell: "sh",
        cwd: "/repo",
        timeoutMs: 1000,
      },
      requestedAt: "2026-10-25T00:00:00Z",
      terminationReason: null,
      outputReferences: [],
      truncated: false,
    };
    expect(() => decodeRecord({ ...record, endMutationGeneration: 1 })).toThrow();
    expect(decodeRecord({ ...record, execution: "failed", exitCode: 1 }).currentness).toBe(
      "Current",
    );
  });
});
