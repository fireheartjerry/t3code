import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  CheckoutIdentity,
  OwnershipGeneration,
  WorkspaceOwnershipPolicy,
} from "./checkoutOwnership.ts";

describe("checkout ownership contracts", () => {
  it("requires a safe positive fencing generation", () => {
    const decode = Schema.decodeUnknownSync(OwnershipGeneration);
    for (const generation of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, Infinity]) {
      expect(() => decode(generation)).toThrow();
    }
    expect(decode(Number.MAX_SAFE_INTEGER)).toBe(Number.MAX_SAFE_INTEGER);
  });

  it("preserves filesystem identities beyond JavaScript integer precision", () => {
    const identity = Schema.decodeUnknownSync(CheckoutIdentity)({
      environmentId: "env",
      checkoutKey: "checkout",
      canonicalCwd: "/repo",
      filesystemIdentity: { device: "18446744073709551615", file: "18446744073709551614" },
      gitDir: "/repo/.git",
      gitCommonDir: "/repo/.git",
    });
    expect(identity.filesystemIdentity.file).toBe("18446744073709551614");
    expect(() => Schema.decodeUnknownSync(WorkspaceOwnershipPolicy)("force")).toThrow();
  });
});
