import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { clientRpcRequiredScopes } from "./clientRpcPermissions.ts";
import {
  WORKSPACE_EXECUTION_METHODS,
  WorkspaceExecutionRpcGroup,
} from "./workspaceExecutionRpc.ts";

const request = WorkspaceExecutionRpcGroup.requests.get(WORKSPACE_EXECUTION_METHODS.request)!;
const output = WorkspaceExecutionRpcGroup.requests.get(WORKSPACE_EXECUTION_METHODS.output)!;
const decodeRequest = Schema.decodeUnknownSync(request.payloadSchema);
const decodeOutput = Schema.decodeUnknownSync(output.payloadSchema);
const decodeOutputResult = Schema.decodeUnknownSync(output.successSchema);
const wait = WorkspaceExecutionRpcGroup.requests.get(WORKSPACE_EXECUTION_METHODS.wait)!;
const decodeWait = Schema.decodeUnknownSync(wait.payloadSchema);

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
    const decoded = decodeRequest({
      threadId: "thread",
      commandId: "command",
      command: "arbitrary",
    });
    expect(decoded).not.toHaveProperty("command");
    expect(() =>
      decodeOutput({
        verificationId: "v",
        referenceId: "log",
        offset: 0,
        limit: 65_537,
      }),
    ).toThrow();
    expect(() =>
      decodeOutputResult({ text: "x".repeat(65_537), truncated: false, nextOffset: null }),
    ).toThrow();
    expect(() =>
      decodeWait({
        threadId: "thread",
        commandId: "command",
        checkoutKey: "checkout",
        generation: 0,
      }),
    ).toThrow();
  });
});
