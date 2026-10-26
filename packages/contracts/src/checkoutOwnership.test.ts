import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  CheckoutIdentity,
  OwnershipGeneration,
  WorkspaceOwnershipPolicy,
} from "./checkoutOwnership.ts";

const decodeGeneration = Schema.decodeUnknownSync(OwnershipGeneration);
const decodeIdentity = Schema.decodeUnknownSync(CheckoutIdentity);
const encodeIdentity = Schema.encodeSync(CheckoutIdentity);
const decodePolicy = Schema.decodeUnknownSync(WorkspaceOwnershipPolicy);

describe("checkout ownership contracts", () => {
  it("requires a safe positive fencing generation", () => {
    const decode = decodeGeneration;
    for (const generation of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, Infinity]) {
      expect(() => decode(generation)).toThrow();
    }
    expect(decode(Number.MAX_SAFE_INTEGER)).toBe(Number.MAX_SAFE_INTEGER);
  });

  it("preserves filesystem identities beyond JavaScript integer precision", () => {
    const identity = decodeIdentity({
      environmentId: "env",
      checkoutKey: "checkout",
      canonicalCwd: "/repo ",
      filesystemIdentity: { device: "18446744073709551615", file: "18446744073709551614" },
      gitDir: "/repo /.git ",
      gitCommonDir: "/repo /.git ",
    });
    expect(identity.filesystemIdentity.file).toBe("18446744073709551614");
    expect(encodeIdentity(identity).canonicalCwd).toBe("/repo ");
    expect(encodeIdentity(identity).gitDir).toBe("/repo /.git ");
    expect(() => decodePolicy("force")).toThrow();
  });
});
