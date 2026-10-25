import {
  CommandId,
  EnvironmentId,
  RunId,
  ThreadId,
  type AcquireCheckoutOwnershipInput,
  type CheckoutOwnershipRecord,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { decideCheckoutAcquisition } from "./CheckoutOwnershipDecider.ts";

const request: AcquireCheckoutOwnershipInput = {
  commandId: CommandId.make("claim"),
  identity: {
    environmentId: EnvironmentId.make("env"),
    checkoutKey: "physical",
    canonicalCwd: "/repo",
    filesystemIdentity: { device: "1", file: "2" },
    gitDir: null,
    gitCommonDir: null,
  },
  owner: { kind: "run", threadId: ThreadId.make("owner"), runId: RunId.make("run") },
  operation: "setup",
};
const makeInput = (current: CheckoutOwnershipRecord | null) => ({
  current,
  request,
  ownershipId: "claim",
  serverInstanceId: "server",
  now: "2026-10-25T00:00:00.000Z",
});

describe("checkout acquisition", () => {
  it.each(["held", "uncertain"] as const)(
    "never steals a %s checkout, even for the same parent",
    (state) => {
      const first = decideCheckoutAcquisition(makeInput(null));
      if (first._tag !== "Acquired") throw new Error("first owner was not admitted");
      const existing: CheckoutOwnershipRecord = {
        identity: request.identity,
        handle: first.handle,
        state,
        acquiredAt: makeInput(null).now,
        updatedAt: makeInput(null).now,
      };
      const result = decideCheckoutAcquisition({
        ...makeInput(existing),
        request: {
          ...request,
          owner: { kind: "run", threadId: ThreadId.make("child"), runId: RunId.make("child-run") },
        },
      });
      expect(result._tag).toBe("Conflict");
      if (result._tag === "Conflict") expect(result.conflict.owner.threadId).toBe("owner");
    },
  );
});
