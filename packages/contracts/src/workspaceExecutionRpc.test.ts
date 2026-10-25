import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { clientRpcRequiredScopes } from "./clientRpcPermissions.ts";
import {
  WORKSPACE_EXECUTION_METHODS,
  WorkspaceExecutionRpcGroup,
} from "./workspaceExecutionRpc.ts";

describe("workspace execution RPC contracts", () => {
  it("requires both operation scopes to execute, but only read authority to inspect", () => {
    expect(clientRpcRequiredScopes(WORKSPACE_EXECUTION_METHODS.request, {})).toEqual([
      "orchestration:operate",
      "terminal:operate",
    ]);
    expect(clientRpcRequiredScopes(WORKSPACE_EXECUTION_METHODS.get, {})).toEqual([
      "orchestration:read",
    ]);
  });

  it("does not accept an arbitrary replacement command and bounds output pages", () => {
    const request = WorkspaceExecutionRpcGroup.requests.get(WORKSPACE_EXECUTION_METHODS.request)!;
    const decoded = Schema.decodeUnknownSync(request.payloadSchema)({
      threadId: "thread",
      commandId: "command",
      command: "arbitrary",
    });
    expect(decoded).not.toHaveProperty("command");
    const output = WorkspaceExecutionRpcGroup.requests.get(WORKSPACE_EXECUTION_METHODS.output)!;
    expect(() =>
      Schema.decodeUnknownSync(output.payloadSchema)({
        verificationId: "v",
        referenceId: "log",
        offset: 0,
        limit: 65_537,
      }),
    ).toThrow();
  });
});
