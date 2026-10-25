import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  VerificationConfig,
  VerificationInputPattern,
  VerificationTimeout,
  verificationMayClaimCurrentPass,
} from "./verification.ts";
import { ProjectId } from "./baseSchemas.ts";
import {
  ServerSettings,
  ServerSettingsPatch,
  isNullableProjectSettingsOverride,
} from "./settings.ts";

describe("verification contracts", () => {
  it("keeps ownership and automatic verification opt-in for legacy settings", () => {
    const settings = Schema.decodeUnknownSync(ServerSettings)({});
    expect(settings.workspaceOwnership).toBe("off");
    expect(settings.verification).toBeNull();
    const patch = Schema.decodeUnknownSync(ServerSettingsPatch)({
      projectSettingsOverrides: { project: { workspaceOwnership: "enforce", verification: null } },
    });
    expect(patch.projectSettingsOverrides?.[ProjectId.make("project")]?.workspaceOwnership).toBe(
      "enforce",
    );
    expect(isNullableProjectSettingsOverride("verification")).toBe(true);
  });
  it("requires an explicit action and finite bounded timeout", () => {
    const decode = Schema.decodeUnknownSync(VerificationTimeout);
    for (const timeout of [0, 999, 86_400_001, Infinity]) expect(() => decode(timeout)).toThrow();
    const config = Schema.decodeUnknownSync(VerificationConfig)({
      actionId: "test",
      inputs: { include: ["**/*"], exclude: ["dist/**"], nonGit: "fingerprint" },
    });
    expect(config.automatic).toBe(false);
    expect(config.timeoutMs).toBe(900_000);
    expect(() =>
      Schema.decodeUnknownSync(VerificationConfig)({ ...config, actionId: "" }),
    ).toThrow();
  });

  it("rejects absolute inputs and parent traversal", () => {
    const decode = Schema.decodeUnknownSync(VerificationInputPattern);
    for (const path of [
      "/etc/passwd",
      "../secret",
      "src/../../secret",
      "C:\\secret",
      "\\\\host\\share",
    ]) {
      expect(() => decode(path)).toThrow();
    }
    expect(decode("src/**/*.ts")).toBe("src/**/*.ts");
  });

  it("cannot turn a provider completion or missing coverage into a current pass", () => {
    expect(
      verificationMayClaimCurrentPass({
        execution: "passed",
        currentness: "Current",
        observationState: "continuous",
        startSource: null,
        endSource: null,
      }),
    ).toBe(false);
  });
});
