// @effect-diagnostics preferSchemaOverJson:off -- Serialization asserts that public evidence contains no fixture secrets.
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentId,
  SourceIdentity,
  type CheckoutIdentity,
  type VerificationConfig,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as HostProcess from "@t3tools/shared/HostProcess";

import * as ProcessRunner from "../processRunner.ts";
import * as VerificationFingerprint from "./VerificationFingerprint.ts";
import { VerificationInputLimits } from "./VerificationInputs.ts";

const isSourceIdentity = Schema.is(SourceIdentity);
const layerTest = VerificationFingerprint.layer.pipe(
  Layer.provideMerge(ProcessRunner.layer),
  Layer.provideMerge(NodeServices.layer),
);
const git = Effect.fn("VerificationFingerprint.test.git")(function* (
  cwd: string,
  args: ReadonlyArray<string>,
) {
  const runner = yield* ProcessRunner.ProcessRunner;
  const result = yield* runner.run({
    command: "git",
    args: ["-c", "core.hooksPath=/dev/null", "-C", cwd, ...args],
  });
  expect(result.code, result.stderr).toBe(0);
  return result.stdout.trim();
});
const fixture = Effect.fn("VerificationFingerprint.test.fixture")(function* (withGit = true) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const temporary = yield* fs.makeTempDirectoryScoped({ prefix: "t3-verification-fingerprint-" });
  const cwd = yield* fs.realPath(temporary);
  const write = Effect.fn("VerificationFingerprint.test.write")(function* (
    name: string,
    content: string,
  ) {
    const absolute = path.join(cwd, name);
    yield* fs.makeDirectory(path.dirname(absolute), { recursive: true });
    yield* fs.writeFileString(absolute, content);
  });
  yield* write("source.ts", "before");
  if (withGit) {
    yield* git(cwd, ["init", "--initial-branch=fixture"]);
    yield* git(cwd, ["config", "user.name", "Fixture"]);
    yield* git(cwd, ["config", "user.email", "fixture@example.invalid"]);
    yield* git(cwd, ["add", "."]);
    yield* git(cwd, ["commit", "-m", "fixture"]);
  }
  const identity: CheckoutIdentity = {
    environmentId: EnvironmentId.make("test"),
    checkoutKey: "fixture",
    canonicalCwd: cwd,
    filesystemIdentity: { device: "fixture", file: "fixture" },
    gitDir: withGit ? path.join(cwd, ".git") : null,
    gitCommonDir: withGit ? path.join(cwd, ".git") : null,
  };
  const config: VerificationConfig = {
    actionId: "test",
    automatic: false,
    timeoutMs: 1_000,
    inputs: { include: ["**/*"], exclude: ["**/dist/**"], nonGit: "fingerprint" },
  };
  const input = { identity, config, configRevision: "config-1", toolchainRevision: "tools-1" };
  const fingerprint = yield* VerificationFingerprint.VerificationFingerprint;
  const capture = Effect.fn("VerificationFingerprint.test.capture")(function* () {
    return yield* fingerprint.capture(input);
  });
  return { fs, path, cwd, identity, config, input, fingerprint, capture, write };
});

it.live("HEAD equality cannot hide tracked or covered untracked edits", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* f.write(".gitignore", "ignored/\n");
    const before = yield* f.capture();
    yield* f.write("source.ts", "edited");
    const tracked = yield* f.capture();
    expect(tracked.head).toBe(before.head);
    expect(tracked.digest).not.toBe(before.digest);
    yield* f.write("untracked.ts", "untracked source");
    const untracked = yield* f.capture();
    expect(untracked.head).toBe(before.head);
    expect(untracked.digest).not.toBe(tracked.digest);
    yield* f.write("ignored/config.json", "ignored input");
    const ignored = yield* f.capture();
    expect(ignored.digest).not.toBe(untracked.digest);
    yield* f.write("ignored/config.json", "changed input");
    const changedIgnored = yield* f.capture();
    expect(changedIgnored.digest).not.toBe(ignored.digest);
    expect(changedIgnored.coverage.complete).toBe(true);
    expect(isSourceIdentity(changedIgnored)).toBe(true);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live("includes index, modes, symlink identity and submodule state", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const before = yield* f.capture();
    yield* f.write("source.ts", "staged source");
    yield* git(f.cwd, ["add", "source.ts"]);
    yield* f.write("source.ts", "before");
    const staged = yield* f.capture();
    expect(staged.head).toBe(before.head);
    expect(staged.indexDigest).not.toBe(before.indexDigest);
    expect(staged.digest).not.toBe(before.digest);
    yield* f.fs.chmod(f.path.join(f.cwd, "source.ts"), 0o755);
    const mode = yield* f.capture();
    expect(mode.digest).not.toBe(staged.digest);
    yield* f.write("same.ts", "before");
    yield* f.fs.chmod(f.path.join(f.cwd, "same.ts"), 0o755);
    yield* f.fs.symlink("source.ts", f.path.join(f.cwd, "link.ts"));
    const linked = yield* f.capture();
    yield* f.fs.remove(f.path.join(f.cwd, "link.ts"));
    yield* f.fs.symlink("same.ts", f.path.join(f.cwd, "link.ts"));
    const retargeted = yield* f.capture();
    expect(retargeted.digest).not.toBe(linked.digest);
    expect(retargeted.coverage.complete).toBe(true);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live(
  "fingerprints available submodule HEAD, index and worktree; missing submodules are gaps",
  () =>
    Effect.gen(function* () {
      const remote = yield* fixture();
      const f = yield* fixture();
      yield* git(f.cwd, [
        "-c",
        "protocol.file.allow=always",
        "submodule",
        "add",
        remote.cwd,
        "module",
      ]);
      yield* git(f.cwd, ["commit", "-am", "submodule fixture"]);
      const before = yield* f.capture();
      expect(before.coverage.complete).toBe(true);
      yield* f.write("module/source.ts", "submodule edit");
      const edited = yield* f.capture();
      expect(edited.digest).not.toBe(before.digest);
      const subRoot = f.path.join(f.cwd, "module");
      yield* git(subRoot, ["add", "source.ts"]);
      const staged = yield* f.capture();
      expect(staged.indexDigest).not.toBe(edited.indexDigest);
      yield* git(subRoot, [
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "-m",
        "submodule change",
      ]);
      const committed = yield* f.capture();
      expect(committed.digest).not.toBe(staged.digest);
      expect(committed.head).toBe(before.head);
      yield* f.fs.remove(f.path.join(subRoot, ".git"));
      const missing = yield* f.capture();
      expect(missing.coverage.complete).toBe(false);
      expect(missing.coverage.gaps).toContain("unreadable-submodule");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live(
  "excluded build output alone leaves source identity unchanged, including staged output",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.write("dist/bundle.js", "initial build");
      yield* f.write("nested/dist/bundle.js", "initial nested build");
      yield* git(f.cwd, ["add", "."]);
      yield* git(f.cwd, ["commit", "-m", "output fixture"]);
      const before = yield* f.capture();
      yield* f.write("dist/bundle.js", "new build");
      yield* f.write("nested/dist/bundle.js", "new nested build");
      yield* f.write("dist/new.js", "new generated output");
      yield* git(f.cwd, ["add", "."]);
      const after = yield* f.capture();
      expect(after.digest).toBe(before.digest);
      expect(after.indexDigest).toBe(before.indexDigest);
      expect(after.coverage).toEqual(before.coverage);
      expect(after.coverage.complete).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live("binds HEAD, branch, checkpoint, action and opaque configuration/toolchain revisions", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const before = yield* f.capture();
    expect((yield* f.capture()).digest).toBe(before.digest);
    for (const override of [
      { checkpointRef: "refs/t3/checkpoint-1" },
      { configRevision: "config-2" },
      { toolchainRevision: "tools-2" },
      { config: { ...f.config, actionId: "other-action" } },
    ]) {
      expect((yield* f.fingerprint.capture({ ...f.input, ...override })).digest).not.toBe(
        before.digest,
      );
    }
    yield* git(f.cwd, ["checkout", "-b", "another-branch"]);
    const branched = yield* f.capture();
    expect(branched.head).toBe(before.head);
    expect(branched.branch).toBe("another-branch");
    expect(branched.digest).not.toBe(before.digest);
    yield* git(f.cwd, ["commit", "--allow-empty", "-m", "new head"]);
    const committed = yield* f.capture();
    expect(committed.head).not.toBe(branched.head);
    expect(committed.digest).not.toBe(branched.digest);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live("supports explicit non-Git fingerprinting without inheriting an ancestor repository", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* f.write("nested/source.ts", "nested source");
    const input = {
      ...f.input,
      identity: {
        ...f.identity,
        canonicalCwd: f.path.join(f.cwd, "nested"),
        gitDir: null,
        gitCommonDir: null,
      },
    };
    const before = yield* f.fingerprint.capture(input);
    expect(before.head).toBeNull();
    expect(before.branch).toBeNull();
    expect(before.indexDigest).toBeNull();
    expect(before.coverage.complete).toBe(true);
    yield* f.write("nested/source.ts", "changed source");
    expect((yield* f.fingerprint.capture(input)).digest).not.toBe(before.digest);
    const unsupported = yield* f.fingerprint
      .capture({
        ...input,
        config: {
          ...f.config,
          inputs: { ...f.config.inputs, nonGit: "unsupported" },
        },
      })
      .pipe(Effect.flip);
    expect(unsupported._tag).toBe("VerificationFingerprintError");
    expect(unsupported.reason).toBe("non-git-unsupported");
    expect(unsupported.cause).toMatchObject({ reason: "non-git-unsupported" });
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live("marks escaped symlinks, missing inputs and unreadable file contents as gaps", () =>
  Effect.gen(function* () {
    const f = yield* fixture(false);
    const outside = yield* f.fs.makeTempFileScoped({ prefix: "t3-verification-external-" });
    yield* f.fs.writeFileString(outside, "external canary");
    yield* f.fs.symlink(outside, f.path.join(f.cwd, "escape"));
    const result = yield* f.fingerprint.capture({
      ...f.input,
      config: {
        ...f.config,
        inputs: { ...f.config.inputs, include: ["**/*", "missing.json"] },
      },
    });
    expect(result.coverage.complete).toBe(false);
    expect(result.coverage.gaps).toEqual(
      expect.arrayContaining(["escaped-symlink", "missing-input"]),
    );
    const unreadableFS = FileSystem.FileSystem.of({
      ...f.fs,
      open: (name, options) =>
        f.fs.open(
          name === f.path.join(f.cwd, "source.ts") ? f.path.join(f.cwd, "absent") : name,
          options,
        ),
    });
    const unreadable = yield* Effect.gen(function* () {
      const service = yield* VerificationFingerprint.VerificationFingerprint;
      return yield* service.capture(f.input);
    }).pipe(
      Effect.provide(Layer.fresh(VerificationFingerprint.layer)),
      Effect.provideService(FileSystem.FileSystem, unreadableFS),
    );
    expect(unreadable.coverage.complete).toBe(false);
    expect(unreadable.coverage.gaps).toContain("unreadable-input");
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live("streams bounded chunks and never exports plaintext file or environment contents", () =>
  Effect.gen(function* () {
    const f = yield* fixture(false);
    const canary = "credential-canary-DO-NOT-EXPORT";
    yield* f.write("source.ts", canary.repeat(8_000));
    const reads: number[] = [];
    const streamingFS = FileSystem.FileSystem.of({
      ...f.fs,
      readFile: () => f.fs.readFile(f.path.join(f.cwd, "absent")),
      readFileString: () => f.fs.readFileString(f.path.join(f.cwd, "absent")),
      open: (name, options) =>
        f.fs.open(name, options).pipe(
          Effect.map(
            (file) =>
              new Proxy(file, {
                get(target, property) {
                  if (property === "readAlloc")
                    return (size: number) => {
                      reads.push(size);
                      return target.readAlloc(size);
                    };
                  return Reflect.get(target, property, target);
                },
              }),
          ),
        ),
    });
    const result = yield* Effect.gen(function* () {
      const service = yield* VerificationFingerprint.VerificationFingerprint;
      return yield* service.capture(f.input);
    }).pipe(
      Effect.provide(Layer.fresh(VerificationFingerprint.layer)),
      Effect.provideService(FileSystem.FileSystem, streamingFS),
    );
    expect(result.coverage.complete).toBe(true);
    expect(reads.length).toBeGreaterThan(3);
    expect(Math.max(...reads)).toBeLessThanOrEqual(65_536);
    expect(JSON.stringify(result)).not.toContain(canary);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live(
  "bounds files, streamed bytes and Git metadata with incomplete coverage or typed failures",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture(false);
      yield* f.write("other.ts", "other source");
      const limits = yield* VerificationInputLimits;
      for (const [override, gap] of [
        [{ maxBytes: 1 }, "byte-limit"],
        [{ maxFiles: 1 }, "file-limit"],
      ] as const) {
        const result = yield* f
          .capture()
          .pipe(Effect.provideService(VerificationInputLimits, { ...limits, ...override }));
        expect(result.coverage.complete).toBe(false);
        expect(result.coverage.gaps).toContain(gap);
      }
      const repository = yield* fixture();
      const limited = yield* repository
        .capture()
        .pipe(
          Effect.provideService(VerificationInputLimits, { ...limits, maxGitOutputBytes: 1 }),
          Effect.flip,
        );
      expect(limited.reason).toBe("git-output-limit");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live("ignores timestamps and identity/configuration object property order", () =>
  Effect.gen(function* () {
    const f = yield* fixture(false);
    const before = yield* f.capture();
    yield* f.fs.utimes(f.path.join(f.cwd, "source.ts"), 0, 0);
    const reordered = yield* f.fingerprint.capture({
      ...f.input,
      identity: {
        gitCommonDir: null,
        gitDir: null,
        filesystemIdentity: { file: "fixture", device: "fixture" },
        canonicalCwd: f.cwd,
        checkoutKey: "fixture",
        environmentId: f.identity.environmentId,
      },
    });
    expect(reordered.digest).toBe(before.digest);
    yield* f.write("source.ts", "edited");
    yield* f.fs.utimes(f.path.join(f.cwd, "source.ts"), 0, 0);
    const edited = yield* f.capture();
    expect(edited.digest).not.toBe(before.digest);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live("ignores inherited Git overrides and keeps environment secrets out of evidence", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const other = yield* fixture();
    yield* git(other.cwd, ["commit", "--allow-empty", "-m", "different fixture"]);
    const baseline = yield* f.capture();
    const canary = "environment-secret-canary-DO-NOT-EXPORT";
    const environment = {
      ...(yield* HostProcess.Environment),
      CLOUD_CREDENTIAL: canary,
      GIT_DIR: other.identity.gitDir!,
      GIT_WORK_TREE: other.cwd,
      GIT_INDEX_FILE: other.path.join(other.cwd, "absent-index"),
      GIT_CONFIG_COUNT: "500",
    };
    const captured = yield* Effect.gen(function* () {
      const service = yield* VerificationFingerprint.VerificationFingerprint;
      return yield* service.capture(f.input);
    }).pipe(
      Effect.provide(Layer.fresh(VerificationFingerprint.layer)),
      Effect.provideService(HostProcess.Environment, environment),
    );
    expect(captured.digest).toBe(baseline.digest);
    expect(captured.coverage.complete).toBe(true);
    expect(JSON.stringify(captured)).not.toContain(canary);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live("uses a linked worktree's own index and HEAD without hashing shared Git internals", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const directory = yield* f.fs.makeTempDirectoryScoped({ prefix: "t3-verification-worktree-" });
    const cwd = yield* f.fs.realPath(directory);
    yield* git(f.cwd, ["worktree", "add", "--detach", cwd, "HEAD"]);
    const gitDir = yield* git(cwd, ["rev-parse", "--absolute-git-dir"]);
    const input = {
      ...f.input,
      identity: { ...f.identity, checkoutKey: "worktree", canonicalCwd: cwd, gitDir },
    };
    const before = yield* f.fingerprint.capture(input);
    expect(before.head).toBe((yield* f.capture()).head);
    expect(before.branch).toBeNull();
    expect(before.coverage.complete).toBe(true);
    yield* f.write("source.ts", "parent checkout edit");
    yield* git(f.cwd, ["add", "source.ts"]);
    expect((yield* f.fingerprint.capture(input)).digest).toBe(before.digest);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live("does not claim complete coverage when an input or symlink changes during capture", () =>
  Effect.gen(function* () {
    const f = yield* fixture(false);
    yield* f.write("other.ts", "other content");
    yield* f.fs.symlink("source.ts", f.path.join(f.cwd, "link.ts"));
    const replacedFS = FileSystem.FileSystem.of({
      ...f.fs,
      open: (name, options) =>
        f.fs.open(
          name === f.path.join(f.cwd, "source.ts") ? f.path.join(f.cwd, "other.ts") : name,
          options,
        ),
    });
    const replaced = yield* Effect.gen(function* () {
      const service = yield* VerificationFingerprint.VerificationFingerprint;
      return yield* service.capture(f.input);
    }).pipe(
      Effect.provide(Layer.fresh(VerificationFingerprint.layer)),
      Effect.provideService(FileSystem.FileSystem, replacedFS),
    );
    expect(replaced.coverage.complete).toBe(false);
    expect(replaced.coverage.gaps).toContain("input-changed-during-capture");
    let linkReads = 0;
    const changedLinkFS = FileSystem.FileSystem.of({
      ...f.fs,
      readLink: (name) => {
        const read = f.fs.readLink(name);
        if (name !== f.path.join(f.cwd, "link.ts") || ++linkReads > 1) return read;
        return read.pipe(
          Effect.tap(() => f.fs.remove(name).pipe(Effect.andThen(f.fs.symlink("other.ts", name)))),
        );
      },
    });
    const changedLink = yield* Effect.gen(function* () {
      const service = yield* VerificationFingerprint.VerificationFingerprint;
      return yield* service.capture(f.input);
    }).pipe(
      Effect.provide(Layer.fresh(VerificationFingerprint.layer)),
      Effect.provideService(FileSystem.FileSystem, changedLinkFS),
    );
    expect(changedLink.coverage.complete).toBe(false);
    expect(changedLink.coverage.gaps).toContain("input-changed-during-capture");
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.live(
  "rejects stale Git-directory identity rather than pairing another repository's index with this checkout",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const other = yield* fixture();
      const changed = yield* f.fingerprint
        .capture({
          ...f.input,
          identity: {
            ...f.identity,
            gitDir: other.identity.gitDir,
            gitCommonDir: other.identity.gitCommonDir,
          },
        })
        .pipe(Effect.flip);
      expect(changed.reason).toBe("checkout-changed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
);
