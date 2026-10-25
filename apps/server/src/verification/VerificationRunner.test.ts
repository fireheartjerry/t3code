// @effect-diagnostics preferSchemaOverJson:off -- Serialization checks that errors never expose fixture credentials.
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentId,
  ThreadId,
  type CheckoutIdentity,
  type OwnershipHandle,
  type SourceIdentity,
  type VerificationActionSnapshot,
  type VerificationConfig,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProcessRunner from "../processRunner.ts";
import * as Ownership from "../workspace/CheckoutOwnershipService.ts";
import * as Accounting from "../workspace/CheckoutProcessAccounting.ts";
import * as Fingerprint from "./VerificationFingerprint.ts";
import * as Observer from "./VerificationSourceObserver.ts";
import * as Runner from "./VerificationRunner.ts";

const identity: CheckoutIdentity = {
  environmentId: EnvironmentId.make("fixture"),
  checkoutKey: "fixture-checkout",
  canonicalCwd: "/fixture",
  filesystemIdentity: { device: "1", file: "2" },
  gitDir: null,
  gitCommonDir: null,
};
const config: VerificationConfig = {
  actionId: "check",
  automatic: false,
  timeoutMs: 1_000,
  inputs: { include: ["**/*"], exclude: ["dist/**"], nonGit: "fingerprint" },
};
const action: VerificationActionSnapshot = {
  actionId: "check",
  revision: "action-1",
  command: "check-command",
  shell: "/bin/sh",
  cwd: "/fixture",
  timeoutMs: 1_000,
};
const handle: OwnershipHandle = {
  ownershipId: "owner",
  checkoutKey: identity.checkoutKey,
  generation: 1,
  serverInstanceId: "server",
  owner: {
    kind: "verification",
    threadId: ThreadId.make("thread"),
    verificationId: "verification",
  },
  operation: "verification",
};
const source: SourceIdentity = {
  checkoutIdentity: identity,
  checkpointRef: null,
  head: "head",
  branch: "fixture",
  indexDigest: "index",
  digest: "source",
  coverage: { complete: true, gaps: [], policyRevision: "policy-1" },
  toolchainRevision: "tools-1",
  configRevision: "config-1",
};
const processOutput = (code: number | null = 0): ProcessRunner.ProcessRunOutput => ({
  stdout: "checks executed",
  stderr: "",
  code: code === null ? null : ChildProcessSpawner.ExitCode(code),
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

const fixture = Effect.fn("VerificationRunner.test.fixture")(function* (
  options: {
    readonly process?: ProcessRunner.ProcessRunner["Service"]["run"];
    readonly capture?: Fingerprint.VerificationFingerprint["Service"]["capture"];
    readonly platform?: NodeJS.Platform;
    readonly maxBytes?: number;
    readonly registerFails?: boolean;
  } = {},
) {
  const events: string[] = [];
  const invocations: ProcessRunner.ProcessRunInput[] = [];
  const registrations: Parameters<
    Accounting.CheckoutProcessAccounting["Service"]["register"]
  >[0][] = [];
  const marks: Parameters<Accounting.CheckoutProcessAccounting["Service"]["mark"]>[0][] = [];
  let observation: Observer.ObservationSnapshot = {
    mutationGeneration: 0,
    observationState: "continuous",
  };
  let closed = 0;
  let fenced = false;
  let stale = false;
  const accounting = Accounting.CheckoutProcessAccounting.of({
    register: (input) =>
      Effect.gen(function* () {
        expect(fenced).toBe(true);
        events.push("register");
        registrations.push(input);
        if (options.registerFails)
          return yield* new Accounting.CheckoutProcessAccountingError({
            reason: "revision-exhausted",
          });
      }),
    mark: (input) =>
      Effect.sync(() => {
        expect(fenced).toBe(true);
        events.push(input.state);
        marks.push(input);
      }),
    observe: () =>
      Effect.succeed({ state: "unknown", accountingRevision: 0, processIdentities: [] }),
    whenQuiescent: () =>
      Effect.succeed({ state: "unknown", accountingRevision: 0, processIdentities: [] }),
  });
  const unused = () => Effect.die("Unexpected ownership operation");
  const ownership = Ownership.CheckoutOwnershipService.of({
    serverInstanceId: "server",
    acquire: unused,
    inspect: unused,
    assertCurrent: unused,
    enqueueWait: unused,
    cancelWait: unused,
    advanceOperation: unused,
    transfer: unused,
    transferWithIntent: unused,
    releaseWhenQuiescent: unused,
    reconcileClaim: unused,
    withCurrent: (_handle, effect) =>
      Effect.gen(function* () {
        if (stale)
          return yield* new Ownership.CheckoutOwnershipServiceError({
            checkoutKey: identity.checkoutKey,
            reason: "identity-changed",
          });
        fenced = true;
        events.push("fence");
        return yield* effect.pipe(
          Effect.ensuring(
            Effect.sync(() => {
              fenced = false;
              events.push("unfence");
            }),
          ),
        );
      }),
  });
  const process = ProcessRunner.ProcessRunner.of({
    run: (input) =>
      Effect.gen(function* () {
        expect(fenced).toBe(true);
        events.push("spawn");
        invocations.push(input);
        if (options.process) return yield* options.process(input);
        yield* input.onSpawn?.(1234) ?? Effect.void;
        return processOutput();
      }),
  });
  const fingerprint = Fingerprint.VerificationFingerprint.of({
    capture: (input) =>
      Effect.gen(function* () {
        events.push("capture");
        if (events.at(-2) !== "snapshot") expect(fenced).toBe(true);
        return yield* options.capture?.(input) ?? Effect.succeed(source);
      }),
  });
  const observer = Observer.VerificationSourceObserver.of({
    start: () =>
      Effect.sync(() => {
        events.push("observe");
        return {
          snapshot: Effect.sync(() => {
            events.push("snapshot");
            return observation;
          }),
          changes: Stream.empty,
          close: Effect.sync(() => {
            closed++;
          }),
        };
      }),
  });
  const runner = yield* Runner.VerificationRunner.pipe(
    Effect.provide(Runner.layer),
    Effect.provideService(ProcessRunner.ProcessRunner, process),
    Effect.provideService(Ownership.CheckoutOwnershipService, ownership),
    Effect.provideService(Accounting.CheckoutProcessAccounting, accounting),
    Effect.provideService(Fingerprint.VerificationFingerprint, fingerprint),
    Effect.provideService(Observer.VerificationSourceObserver, observer),
    Effect.provideService(HostProcess.Platform, options.platform ?? "darwin"),
    Effect.provideService(
      Runner.VerificationRunnerOutputLimit,
      options.maxBytes ?? 8 * 1024 * 1024,
    ),
  );
  const prepare = (
    input = { identity, config, configRevision: "config-1", toolchainRevision: "tools-1" },
  ) => runner.prepare(input);
  const run = Effect.fn("VerificationRunner.test.run")(function* (
    snapshot = action,
    secrets: readonly string[] = [],
  ) {
    const prepared = yield* prepare();
    return yield* runner.run({
      verificationId: "verification",
      executionClaimId: "claim-1",
      handle,
      actionSnapshot: snapshot,
      prepared,
      secrets,
    });
  });
  return {
    runner,
    prepare,
    run,
    events,
    invocations,
    registrations,
    marks,
    closed: () => closed,
    fenced: () => fenced,
    observe: (value: Observer.ObservationSnapshot) => {
      observation = value;
    },
    stale: () => {
      stale = true;
    },
  };
});

it.effect("starts observation before fingerprinting and retains the successful lease", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const prepared = yield* f.prepare();
    expect(f.events).toEqual(["observe", "snapshot", "capture"]);
    expect(prepared.startSource).toEqual(source);
    expect(prepared.startObservation).toEqual({
      mutationGeneration: 0,
      observationState: "continuous",
    });
    yield* f.runner.run({
      verificationId: "verification",
      executionClaimId: "claim-1",
      handle,
      actionSnapshot: action,
      prepared,
      secrets: [],
    });
    expect(f.closed()).toBe(0);
    yield* prepared.lease.close;
    expect(f.closed()).toBe(1);
  }),
);

it.effect("closes a lease when preparation fails and returns a credential-free typed error", () =>
  Effect.gen(function* () {
    const f = yield* fixture({
      capture: () =>
        Effect.fail(
          new Fingerprint.VerificationFingerprintError({
            reason: "invalid-inputs",
            cause: new Error("secret-canary"),
          }),
        ),
    });
    const error = yield* f.prepare().pipe(Effect.flip);
    expect(error).toMatchObject({ _tag: "VerificationRunnerError", reason: "fingerprint-failed" });
    expect(JSON.stringify(error)).not.toContain("secret-canary");
    expect(f.closed()).toBe(1);
  }),
);

it.effect(
  "records actual PID and creation marker under the fence, retaining honest process uncertainty",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const result = yield* f.run();
      expect(f.registrations[0]).toMatchObject({
        handle,
        processRef: { pid: 1234, creationMarker: "claim-1" },
        kind: "verification",
      });
      expect(f.marks.map((mark) => mark.state)).toEqual(["live", "unknown"]);
      expect(f.events.at(-1)).toBe("unfence");
      expect(result.completion).toMatchObject({
        execution: "passed",
        currentness: "Unknown",
        exitCode: 0,
        signal: null,
        terminationReason: "exited-descendants-unconfirmed",
        endSource: source,
      });
      expect(result.startedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
      expect(result.completion.finishedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
      expect(f.invocations[0]).toMatchObject({
        command: "/bin/sh",
        args: ["-lc", "check-command"],
        cwd: "/fixture",
        timeout: 1_000,
        outputMode: "truncate",
        timeoutBehavior: "timedOutResult",
      });
      expect(f.invocations[0]?.env).toBeUndefined();
    }),
);

it.effect("rejects known and recognizable command secrets before invoking ProcessRunner", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    for (const command of [
      "echo secret-canary",
      "TOKEN=unlisted-credential check-command",
      "curl 'https://example.invalid/?sig=canary'",
    ]) {
      const error = yield* f.run({ ...action, command }, ["secret-canary"]).pipe(Effect.flip);
      expect(error.reason).toBe("secret-bearing-command");
      expect(JSON.stringify(error)).not.toContain(command);
    }
    expect(f.invocations).toHaveLength(0);
  }),
);

it.effect("checks the current ownership fence before spawning", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    f.stale();
    const error = yield* f.run().pipe(Effect.flip);
    expect(error.reason).toBe("ownership-lost");
    expect(f.invocations).toHaveLength(0);
  }),
);

it.effect("rejects mismatched verification ownership and action snapshots", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const prepared = yield* f.prepare();
    const error = yield* f.runner
      .run({
        verificationId: "other",
        executionClaimId: "claim",
        handle,
        prepared,
        actionSnapshot: action,
        secrets: [],
      })
      .pipe(Effect.flip);
    expect(error.reason).toBe("identity-mismatch");
    expect((yield* f.run({ ...action, actionId: "other-action" }).pipe(Effect.flip)).reason).toBe(
      "action-mismatch",
    );
    expect(f.invocations).toHaveLength(0);
  }),
);

it.effect("does not promote zero exit without the actual spawn callback", () =>
  Effect.gen(function* () {
    const f = yield* fixture({ process: () => Effect.succeed(processOutput()) });
    const result = yield* f.run();
    expect(result.startedAt).toBeNull();
    expect(result.completion).toMatchObject({
      execution: "interrupted",
      currentness: "Unknown",
      terminationReason: "missing-spawn-evidence",
    });
  }),
);

it.effect("retains nonzero check text and sanitizes an aggregate bounded output", () =>
  Effect.gen(function* () {
    const f = yield* fixture({
      maxBytes: 100,
      process: (input) =>
        Effect.gen(function* () {
          yield* input.onSpawn!(42);
          return {
            ...processOutput(7),
            stdout: "canary-",
            stderr: "credential\nFAIL: expected 2, received 3\n" + "x".repeat(100),
          };
        }),
    });
    const result = yield* f.run(action, ["canary-credential"]);
    expect(result.completion).toMatchObject({
      execution: "failed",
      exitCode: 7,
      terminationReason: "nonzero-exit",
      truncated: true,
    });
    expect(result.stderr).toContain("FAIL: expected 2, received 3");
    expect(result.stdout + result.stderr + result.diagnostic).not.toContain("canary-credential");
    expect(
      new TextEncoder().encode(result.stdout + result.stderr + result.diagnostic).length,
    ).toBeLessThanOrEqual(100);
  }),
);

it.effect("distinguishes timeout, missing executable, process failure and unknown exit", () =>
  Effect.gen(function* () {
    const cases = [
      {
        output: { ...processOutput(null), timedOut: true },
        execution: "timed out",
        reason: "timeout",
      },
      { output: processOutput(null), execution: "interrupted", reason: "exit-status-unavailable" },
    ];
    for (const item of cases) {
      const f = yield* fixture({
        process: (input) =>
          Effect.gen(function* () {
            yield* input.onSpawn!(42);
            return item.output;
          }),
      });
      const result = yield* f.run();
      expect(result.completion).toMatchObject({
        execution: item.execution,
        terminationReason: item.reason,
        signal: null,
        currentness: "Unknown",
      });
    }
    const missing = yield* fixture({
      process: () =>
        Effect.fail(
          new ProcessRunner.ProcessSpawnError({
            command: "secret-canary",
            argumentCount: 0,
            cause: { code: "ENOENT" },
          }),
        ),
    });
    const result = yield* missing.run();
    expect(result.completion).toMatchObject({
      execution: "failed",
      terminationReason: "missing-executable",
      exitCode: null,
    });
    expect(result.startedAt).toBeNull();
    expect(JSON.stringify(result)).not.toContain("secret-canary");
    const broken = yield* fixture({
      process: (input) =>
        Effect.gen(function* () {
          yield* input.onSpawn!(42);
          return yield* new ProcessRunner.ProcessReadError({
            command: "secret-canary",
            argumentCount: 0,
            stream: "stdout",
            cause: "credential",
          });
        }),
    });
    expect((yield* broken.run()).completion.terminationReason).toBe("process-read-failed");
  }),
);

it.effect("does not invent signal details when ProcessRunner cannot obtain an exit status", () =>
  Effect.gen(function* () {
    const f = yield* fixture({
      process: (input) =>
        Effect.gen(function* () {
          yield* input.onSpawn!(42);
          return yield* new ProcessRunner.ProcessReadError({
            command: "shell",
            argumentCount: 2,
            stream: "exitCode",
            cause: new Error("raw signal detail and credential"),
          });
        }),
    });
    const result = yield* f.run();
    expect(result.completion).toMatchObject({
      execution: "interrupted",
      signal: null,
      exitCode: null,
      terminationReason: "exit-status-unavailable",
    });
    expect(JSON.stringify(result)).not.toContain("raw signal");
  }),
);

it.effect("closes preparation on interruption without transferring or leaking a lease", () =>
  Effect.gen(function* () {
    const capturing = yield* Deferred.make<void>();
    const f = yield* fixture({
      capture: () => Deferred.succeed(capturing, undefined).pipe(Effect.andThen(Effect.never)),
    });
    const preparing = yield* f.prepare().pipe(Effect.forkChild);
    yield* Deferred.await(capturing);
    yield* Fiber.interrupt(preparing);
    const exit = yield* Fiber.await(preparing);
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
    expect(f.closed()).toBe(1);
  }),
);

it.effect("captures preparation-time mutations and requires fresh observation after capture", () =>
  Effect.gen(function* () {
    let captures = 0;
    const f = yield* fixture({
      capture: () =>
        Effect.sync(() => {
          captures++;
          f.observe({ mutationGeneration: captures, observationState: "continuous" });
          return source;
        }),
    });
    const result = yield* f.run();
    expect(result.startMutationGeneration).toBe(0);
    expect(result.diagnostic).toContain("Covered source changed during verification.");
    expect(result.completion.endMutationGeneration).toBe(2);
    expect(result.completion.currentness).toBe("Unknown");
    expect(f.events.slice(-4)).toEqual(["unknown", "capture", "snapshot", "unfence"]);
  }),
);

it.effect("does not hide accounting registration failure or mark the process stopped", () =>
  Effect.gen(function* () {
    const f = yield* fixture({ registerFails: true });
    const result = yield* f.run();
    expect(result.completion).toMatchObject({
      execution: "interrupted",
      currentness: "Unknown",
      terminationReason: "process-accounting-failed",
    });
    expect(f.marks.some((mark) => mark.state === "stopped")).toBe(false);
  }),
);

it.effect("retains edit-and-revert generations even with an equal endpoint fingerprint", () =>
  Effect.gen(function* () {
    const f = yield* fixture({
      process: (input) =>
        Effect.gen(function* () {
          yield* input.onSpawn!(42);
          f.observe({ mutationGeneration: 2, observationState: "continuous" });
          return processOutput();
        }),
    });
    const result = yield* f.run();
    expect(result.completion).toMatchObject({
      execution: "passed",
      currentness: "Unknown",
      endMutationGeneration: 2,
      endSource: source,
    });
    expect(result.startMutationGeneration).toBe(0);
  }),
);

it.effect("observation gaps and failed final fingerprinting cannot be a passing observation", () =>
  Effect.gen(function* () {
    for (const state of ["gap", "unavailable"] as const) {
      const f = yield* fixture({
        process: (input) =>
          Effect.gen(function* () {
            yield* input.onSpawn!(42);
            f.observe({ mutationGeneration: 0, observationState: state });
            return processOutput();
          }),
      });
      expect((yield* f.run()).completion).toMatchObject({
        execution: "failed",
        currentness: "Unknown",
        terminationReason: "observation-failed",
        observationState: state,
      });
    }
    let captures = 0;
    const f = yield* fixture({
      capture: () =>
        ++captures === 1
          ? Effect.succeed(source)
          : Effect.fail(
              new Fingerprint.VerificationFingerprintError({ reason: "git-command-failed" }),
            ),
    });
    expect((yield* f.run()).completion).toMatchObject({
      execution: "failed",
      terminationReason: "observation-failed",
      endSource: null,
    });
  }),
);

it.effect(
  "awaits interrupted process cleanup before accounting uncertainty and fence release",
  () =>
    Effect.gen(function* () {
      const spawned = yield* Deferred.make<void>();
      const cleaning = yield* Deferred.make<void>();
      const cleaned = yield* Deferred.make<void>();
      const f = yield* fixture({
        process: (input) =>
          Effect.gen(function* () {
            yield* input.onSpawn!(42);
            yield* Deferred.succeed(spawned, undefined);
            return yield* Effect.never.pipe(
              Effect.ensuring(
                Effect.gen(function* () {
                  yield* Deferred.succeed(cleaning, undefined);
                  yield* Deferred.await(cleaned);
                  f.events.push("cleaned");
                }),
              ),
            );
          }),
      });
      const running = yield* f.run().pipe(Effect.forkChild);
      yield* Deferred.await(spawned);
      const interrupting = yield* Fiber.interrupt(running).pipe(Effect.forkChild);
      yield* Deferred.await(cleaning);
      expect(f.fenced()).toBe(true);
      expect(f.marks.map((mark) => mark.state)).toEqual(["live"]);
      yield* Deferred.succeed(cleaned, undefined);
      yield* Fiber.join(interrupting);
      const exit = yield* Fiber.await(running);
      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
      expect(f.events.slice(-3)).toEqual(["cleaned", "unknown", "unfence"]);
    }),
);

it.effect("chooses arguments for each supported shell on its owning platform", () =>
  Effect.gen(function* () {
    for (const [platform, shell, args] of [
      ["darwin", "/bin/zsh", ["-lc", "check-command"]],
      ["linux", "/usr/bin/bash", ["-lc", "check-command"]],
      ["win32", "C:\\Windows\\System32\\cmd.exe", ["/d", "/s", "/c", "check-command"]],
      ["win32", "powershell.exe", ["-NoLogo", "-NoProfile", "-Command", "check-command"]],
      ["win32", "pwsh.exe", ["-NoLogo", "-NoProfile", "-Command", "check-command"]],
      ["win32", "C:\\Tools\\bash.exe", ["-lc", "check-command"]],
    ] as const) {
      const f = yield* fixture({ platform });
      yield* f.run({ ...action, shell });
      expect(f.invocations[0]?.args).toEqual(args);
    }
    const f = yield* fixture();
    expect((yield* f.run({ ...action, shell: "cmd.exe" }).pipe(Effect.flip)).reason).toBe(
      "unsupported-shell",
    );
  }),
);

it.effect("retains source changes and incomplete coverage without claiming a current pass", () =>
  Effect.gen(function* () {
    for (const change of [
      { ...source, digest: "changed" },
      { ...source, indexDigest: "changed-index" },
      { ...source, head: "changed-head" },
      { ...source, configRevision: "config-2" },
      { ...source, toolchainRevision: "tools-2" },
    ]) {
      let captures = 0;
      const f = yield* fixture({
        capture: () => Effect.succeed(++captures === 1 ? source : change),
      });
      const result = yield* f.run();
      expect(result.completion).toMatchObject({
        execution: "passed",
        currentness: "Unknown",
        endSource: change,
      });
      expect(result.diagnostic).toContain("Covered source changed during verification.");
    }
    const incomplete = {
      ...source,
      coverage: { ...source.coverage, complete: false, gaps: ["unreadable-input"] },
    };
    const f = yield* fixture({ capture: () => Effect.succeed(incomplete) });
    const result = yield* f.run();
    expect(result.completion.currentness).toBe("Unknown");
    expect(result.diagnostic).toContain("Source coverage is incomplete.");
  }),
);

it.effect("uses the prepared owning-platform action and retains the checkpoint input", () =>
  Effect.gen(function* () {
    const f = yield* fixture({ platform: "win32" });
    const prepared = yield* f.runner.prepare({
      identity,
      config: { ...config, platformActions: { windows: "windows-check" } },
      configRevision: "config-1",
      toolchainRevision: "tools-1",
      checkpointRef: "refs/checkpoint",
    });
    expect(prepared.checkpointRef).toBe("refs/checkpoint");
    const input = {
      verificationId: "verification",
      executionClaimId: "claim-1",
      handle,
      prepared,
      actionSnapshot: { ...action, actionId: "windows-check", shell: "cmd.exe" },
      secrets: [],
    };
    expect((yield* f.runner.run(input)).completion.execution).toBe("passed");
    expect(
      (yield* f.runner
        .run({ ...input, actionSnapshot: { ...input.actionSnapshot, actionId: "check" } })
        .pipe(Effect.flip)).reason,
    ).toBe("action-mismatch");
  }),
);

it.live(
  "executes a real shell only in a temporary fixture with sanitized failed-check evidence",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-verification-runner-" });
      const process = yield* ProcessRunner.ProcessRunner;
      const f = yield* fixture({ process: process.run });
      const result = yield* f.run({
        ...action,
        cwd,
        command: "printf 'FAIL: real fixture check\\n' >&2; exit 9",
      });
      expect(result.completion).toMatchObject({
        execution: "failed",
        exitCode: 9,
        signal: null,
        currentness: "Unknown",
      });
      expect(result.stderr).toBe("FAIL: real fixture check\n");
      expect(f.registrations[0]?.processRef.pid).toBeGreaterThan(0);
      const passed = yield* f.run({ ...action, cwd, command: "printf 'fixture checks passed\\n'" });
      expect(passed.completion).toMatchObject({
        execution: "passed",
        exitCode: 0,
        currentness: "Unknown",
      });
      expect(passed.stdout).toBe("fixture checks passed\n");
      const missing = yield* f.run({ ...action, cwd, shell: `${cwd}/missing/sh` });
      expect(missing.completion).toMatchObject({
        execution: "failed",
        terminationReason: "missing-executable",
        exitCode: null,
      });
      expect(missing.startedAt).toBeNull();
    }).pipe(
      Effect.scoped,
      Effect.provide(ProcessRunner.layer.pipe(Layer.provideMerge(NodeServices.layer))),
    ),
);
