import { WorkspaceExecutionRequestError } from "@t3tools/contracts";
import { expect, it } from "vite-plus/test";

import { workspaceExecutionError } from "./WorkspaceExecutionRpc.ts";

it.each(["not-configured", "policy", "invalid-input", "not-found", "unavailable"] as const)(
  "preserves normalized %s errors through the service and RPC boundaries",
  (reason) => {
    const normalized = new WorkspaceExecutionRequestError({ reason });
    expect(workspaceExecutionError(normalized)).toBe(normalized);
  },
);
