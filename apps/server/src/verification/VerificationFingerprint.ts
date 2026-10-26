// @effect-diagnostics preferSchemaOverJson:off -- JSON only frames hash inputs; it is not a decoder or persisted plaintext.
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect Crypto has no incremental hash API.
import * as NodeCrypto from "node:crypto";
import type { CheckoutIdentity, SourceIdentity, VerificationConfig } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { resolveVerificationInputs, VerificationInputLimits } from "./VerificationInputs.ts";

export class VerificationFingerprintError extends Schema.TaggedError<VerificationFingerprintError>()(
  "VerificationFingerprintError",
  {
    reason: Schema.Literals([
      "invalid-inputs",
      "git-unavailable",
      "git-command-failed",
      "git-output-limit",
      "git-timeout",
      "invalid-git-metadata",
      "checkout-changed",
      "non-git-unsupported",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Cannot capture verification source identity (${this.reason}).`;
  }
}

export class VerificationFingerprint extends Context.Service<
  VerificationFingerprint,
  {
    readonly capture: (input: {
      readonly identity: CheckoutIdentity;
      readonly config: VerificationConfig;
      readonly configRevision: string;
      readonly toolchainRevision: string;
      readonly checkpointRef?: string | null;
    }) => Effect.Effect<SourceIdentity, VerificationFingerprintError>;
  }
>()("t3/verification/VerificationFingerprint") {}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const hostEnvironment = yield* HostProcess.Environment;
  const platform = yield* HostProcess.Platform;
  // No inherited Git overrides, credentials, hooks or lazy fetching. These reads must stay local.
  const gitEnvironment = Object.fromEntries(
    Object.entries(hostEnvironment).filter(([key]) =>
      ["PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TMP", "TEMP", "TMPDIR"].includes(
        key.toUpperCase(),
      ),
    ),
  );
  Object.assign(gitEnvironment, {
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: platform === "win32" ? "NUL" : "/dev/null",
    GIT_OPTIONAL_LOCKS: "0",
    GIT_NO_LAZY_FETCH: "1",
    GIT_TERMINAL_PROMPT: "0",
  });

  const git = Effect.fn("VerificationFingerprint.git")(
    function* (cwd: string, gitDir: string | null, args: ReadonlyArray<string>) {
      const limits = yield* VerificationInputLimits;
      const child = yield* spawner
        .spawn(
          ChildProcess.make(
            "git",
            [
              "--no-optional-locks",
              "-c",
              "core.fsmonitor=false",
              ...(gitDir === null ? [] : [`--git-dir=${gitDir}`, `--work-tree=${cwd}`]),
              ...args,
            ],
            { cwd, env: gitEnvironment, extendEnv: false, stdin: "ignore", stderr: "ignore" },
          ),
        )
        .pipe(
          Effect.mapError(
            (cause) => new VerificationFingerprintError({ reason: "git-unavailable", cause }),
          ),
        );
      const chunks: Uint8Array[] = [];
      let bytes = 0;
      yield* child.stdout.pipe(
        Stream.runForEach((chunk) =>
          Effect.gen(function* () {
            bytes += chunk.byteLength;
            if (bytes > limits.maxGitOutputBytes) {
              return yield* new VerificationFingerprintError({ reason: "git-output-limit" });
            }
            chunks.push(chunk);
          }),
        ),
        Effect.mapError((cause) =>
          cause._tag === "VerificationFingerprintError"
            ? cause
            : new VerificationFingerprintError({ reason: "git-command-failed", cause }),
        ),
      );
      const code = yield* child.exitCode.pipe(
        Effect.mapError(
          (cause) => new VerificationFingerprintError({ reason: "git-command-failed", cause }),
        ),
      );
      const output = yield* Effect.try({
        try: () => {
          const buffer = new Uint8Array(bytes);
          let offset = 0;
          for (const chunk of chunks) {
            buffer.set(chunk, offset);
            offset += chunk.byteLength;
          }
          return new TextDecoder("utf-8", { fatal: true }).decode(buffer);
        },
        catch: () => new VerificationFingerprintError({ reason: "invalid-git-metadata" }),
      });
      return { code: Number(code), output };
    },
    Effect.scoped,
    Effect.timeoutOrElse({
      duration: "30 seconds",
      orElse: () => Effect.fail(new VerificationFingerprintError({ reason: "git-timeout" })),
    }),
  );

  const requiredGit = Effect.fn("VerificationFingerprint.requiredGit")(function* (
    cwd: string,
    gitDir: string | null,
    args: ReadonlyArray<string>,
  ) {
    const result = yield* git(cwd, gitDir, args);
    if (result.code !== 0)
      return yield* new VerificationFingerprintError({ reason: "git-command-failed" });
    return result.output;
  });

  const capture: VerificationFingerprint["Service"]["capture"] = Effect.fn(
    "VerificationFingerprint.capture",
  )(function* (input) {
    const limits = yield* VerificationInputLimits;
    const inputs = yield* resolveVerificationInputs(input).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.mapError(
        (cause) =>
          new VerificationFingerprintError({
            reason:
              cause.reason === "non-git-unsupported" ? "non-git-unsupported" : "invalid-inputs",
            cause,
          }),
      ),
    );
    const gaps = new Set(inputs.coverage.gaps);
    const hash = NodeCrypto.createHash("sha256");
    // JSON records provide unambiguous framing; raw contents and target strings never escape this operation.
    const record = (value: unknown) => {
      hash.update(JSON.stringify(value));
      hash.update("\n");
    };
    record([
      "verification-source-v1",
      [
        input.identity.environmentId,
        input.identity.checkoutKey,
        input.identity.canonicalCwd,
        input.identity.filesystemIdentity.device,
        input.identity.filesystemIdentity.file,
        input.identity.gitDir,
        input.identity.gitCommonDir,
      ],
      inputs.policy.revision,
      input.config.actionId,
      [
        input.config.platformActions?.darwin ?? null,
        input.config.platformActions?.linux ?? null,
        input.config.platformActions?.windows ?? null,
      ],
      input.configRevision,
      input.toolchainRevision,
      input.checkpointRef ?? null,
    ]);
    let head: string | null = null;
    let branch: string | null = null;
    let indexDigest: string | null = null;
    let bytes = 0;
    let indexedFiles = 0;
    const indexHash = NodeCrypto.createHash("sha256");
    const root = input.identity.canonicalCwd;
    const coveredPaths = new Set(inputs.paths);

    const repository = Effect.fn("VerificationFingerprint.repository")(function* (
      cwd: string,
      gitDir: string,
      prefix: string,
      depth: number,
    ): Effect.fn.Return<void, VerificationFingerprintError> {
      if (depth > limits.maxDepth) {
        gaps.add("submodule-depth-limit");
        return;
      }
      const top = (yield* requiredGit(cwd, gitDir, ["rev-parse", "--show-toplevel"])).replace(
        /\r?\n$/,
        "",
      );
      const canonicalTop = yield* fs
        .realPath(top)
        .pipe(
          Effect.mapError(
            (cause) => new VerificationFingerprintError({ reason: "checkout-changed", cause }),
          ),
        );
      if (canonicalTop !== cwd)
        return yield* new VerificationFingerprintError({ reason: "checkout-changed" });
      const headResult = yield* git(cwd, gitDir, ["rev-parse", "--verify", "HEAD"]);
      const branchResult = yield* git(cwd, gitDir, ["symbolic-ref", "--quiet", "--short", "HEAD"]);
      if (
        (headResult.code !== 0 && headResult.code !== 128 && headResult.code !== 1) ||
        (branchResult.code !== 0 && branchResult.code !== 1)
      ) {
        return yield* new VerificationFingerprintError({ reason: "invalid-git-metadata" });
      }
      const repositoryHead = headResult.code === 0 ? headResult.output.trim() : null;
      const repositoryBranch = branchResult.code === 0 ? branchResult.output.trim() : null;
      if (repositoryHead === null && repositoryBranch === null) gaps.add("unresolved-head");
      if (prefix === "") {
        head = repositoryHead;
        branch = repositoryBranch;
      }
      record(["repository", prefix, repositoryHead, repositoryBranch]);
      const staged = yield* requiredGit(cwd, gitDir, ["ls-files", "--stage", "-z"]);
      const submodules: string[] = [];
      for (const entry of staged.split("\0")) {
        if (!entry) continue;
        const separator = entry.indexOf("\t");
        const metadata = entry.slice(0, separator).split(" ");
        const relative = entry.slice(separator + 1);
        const full = prefix ? `${prefix}/${relative}` : relative;
        if (
          separator === -1 ||
          metadata.length !== 3 ||
          !/^[0-7]{6}$/.test(metadata[0]!) ||
          !/^[a-f0-9]{40,64}$/.test(metadata[1]!) ||
          !/^[0-3]$/.test(metadata[2]!) ||
          relative.startsWith("/") ||
          relative.split("/").includes("..")
        ) {
          return yield* new VerificationFingerprintError({ reason: "invalid-git-metadata" });
        }
        const submodule = metadata[0] === "160000";
        if (!inputs.policy.includes(full) && !(submodule && inputs.policy.mayContain(full)))
          continue;
        indexedFiles++;
        if (indexedFiles > limits.maxFiles) {
          gaps.add("file-limit");
          break;
        }
        const indexRecord = JSON.stringify([full, ...metadata]);
        indexHash.update(indexRecord).update("\n");
        if (submodule) submodules.push(relative);
        else if (!coveredPaths.has(full)) gaps.add("missing-index-input");
      }
      // Capture index flags as well as staged object IDs, without timestamp/cache noise from raw index bytes.
      const flags = yield* requiredGit(cwd, gitDir, ["ls-files", "-v", "-z"]);
      for (const flag of flags.split("\0")) {
        const relative = flag.slice(2);
        const full = prefix ? `${prefix}/${relative}` : relative;
        if (flag && inputs.policy.includes(full))
          indexHash.update(JSON.stringify([full, flag[0]])).update("\n");
      }
      for (const relative of [...new Set(submodules)].sort()) {
        const full = prefix ? `${prefix}/${relative}` : relative;
        const subRoot = path.join(cwd, relative);
        yield* Effect.gen(function* () {
          // A missing submodule must not discover its parent's repository and masquerade as available.
          const marker = yield* fs.exists(path.join(subRoot, ".git"));
          if (!marker) {
            gaps.add("unreadable-submodule");
            return;
          }
          const realSubRoot = yield* fs.realPath(subRoot);
          if (realSubRoot !== subRoot) {
            gaps.add("escaped-submodule");
            return;
          }
          const subGitDir = (yield* requiredGit(subRoot, null, [
            "rev-parse",
            "--absolute-git-dir",
          ])).replace(/\r?\n$/, "");
          yield* repository(subRoot, subGitDir, full, depth + 1);
        }).pipe(
          Effect.catch(() => {
            gaps.add("unreadable-submodule");
            return Effect.void;
          }),
        );
      }
    });

    if (input.identity.gitDir !== null) {
      // Discover through the checkout before pinning --git-dir. A stale supplied path can still
      // exist after a .git pointer changes, and --work-tree would otherwise hide that mismatch.
      const discovered = yield* requiredGit(root, null, [
        "rev-parse",
        "--path-format=absolute",
        "--absolute-git-dir",
        "--git-common-dir",
      ]).pipe(
        Effect.mapError((cause) =>
          cause.reason === "git-command-failed"
            ? new VerificationFingerprintError({ reason: "checkout-changed", cause })
            : cause,
        ),
      );
      const directories = discovered.replace(/\r?\n$/, "").split(/\r?\n/);
      if (
        directories.length !== 2 ||
        directories.some((directory) => !path.isAbsolute(directory))
      ) {
        return yield* new VerificationFingerprintError({ reason: "invalid-git-metadata" });
      }
      const actualDirectories = yield* Effect.forEach(directories, (directory) =>
        fs
          .realPath(directory)
          .pipe(
            Effect.mapError(
              (cause) => new VerificationFingerprintError({ reason: "checkout-changed", cause }),
            ),
          ),
      );
      const actualGitDir = actualDirectories[0]!;
      if (
        actualGitDir !== input.identity.gitDir ||
        actualDirectories[1] !== input.identity.gitCommonDir
      )
        return yield* new VerificationFingerprintError({ reason: "checkout-changed" });
      yield* repository(root, actualGitDir, "", 0);
      indexDigest = indexHash.digest("hex");
    } else if (
      yield* fs
        .exists(path.join(root, ".git"))
        .pipe(
          Effect.mapError(
            (cause) => new VerificationFingerprintError({ reason: "checkout-changed", cause }),
          ),
        )
    ) {
      return yield* new VerificationFingerprintError({ reason: "checkout-changed" });
    }
    record(["index", indexDigest]);
    for (const entry of inputs.entries) {
      if (entry.linkTarget !== null) {
        const currentTarget = yield* fs
          .readLink(entry.absolutePath)
          .pipe(Effect.orElseSucceed(() => null));
        const currentInfo = yield* fs
          .stat(entry.absolutePath)
          .pipe(Effect.orElseSucceed(() => null));
        if (
          currentTarget !== entry.linkTarget ||
          currentInfo === null ||
          !sameFile(entry.info, currentInfo)
        ) {
          gaps.add("input-changed-during-capture");
          continue;
        }
        const currentReal = yield* fs
          .realPath(entry.absolutePath)
          .pipe(Effect.orElseSucceed(() => null));
        if (currentReal === null) {
          gaps.add("unreadable-symlink");
          continue;
        }
        const relative = path.relative(root, currentReal).replaceAll("\\", "/");
        if (relative === ".." || relative.startsWith("../") || path.isAbsolute(relative)) {
          gaps.add("escaped-symlink");
          continue;
        }
        if (!inputs.policy.includes(relative)) {
          gaps.add("symlink-target-outside-coverage");
          continue;
        }
        bytes += new TextEncoder().encode(entry.linkTarget).byteLength;
        if (bytes > limits.maxBytes) {
          gaps.add("byte-limit");
          break;
        }
        record([
          "symlink",
          entry.relativePath,
          entry.info.mode,
          NodeCrypto.createHash("sha256").update(entry.linkTarget).digest("hex"),
        ]);
        continue;
      }
      if (BigInt(bytes) + entry.info.size > BigInt(limits.maxBytes)) {
        gaps.add("byte-limit");
        break;
      }
      const contentHash = NodeCrypto.createHash("sha256");
      const result = yield* Effect.gen(function* () {
        const real = yield* fs.realPath(entry.absolutePath);
        if (real !== entry.absolutePath) {
          gaps.add("input-changed-during-capture");
          return false;
        }
        const file = yield* fs.open(entry.absolutePath, {
          flag: "r",
          ...(platform === "win32" ? {} : { noFollow: true }),
        });
        const opened = yield* file.stat;
        if (!sameFile(entry.info, opened) || opened.type !== "File") {
          gaps.add("input-changed-during-capture");
          return false;
        }
        let fileBytes = 0;
        for (;;) {
          const chunk = yield* file.readAlloc(Math.min(65_536, limits.maxBytes - bytes + 1));
          if (Option.isNone(chunk)) break;
          bytes += chunk.value.byteLength;
          fileBytes += chunk.value.byteLength;
          if (bytes > limits.maxBytes) {
            gaps.add("byte-limit");
            return false;
          }
          contentHash.update(chunk.value);
        }
        if (BigInt(fileBytes) !== entry.info.size || !sameFile(opened, yield* file.stat)) {
          gaps.add("input-changed-during-capture");
          return false;
        }
        return true;
      }).pipe(
        Effect.scoped,
        Effect.catch(() => {
          gaps.add("unreadable-input");
          return Effect.succeed(false);
        }),
      );
      if (result) record(["file", entry.relativePath, entry.info.mode, contentHash.digest("hex")]);
      if (bytes > limits.maxBytes) break;
    }
    const coverage = {
      complete: gaps.size === 0,
      policyRevision: inputs.policy.revision,
      gaps: [...gaps].sort(),
    };
    record(["coverage", coverage]);
    return {
      checkoutIdentity: input.identity,
      checkpointRef: input.checkpointRef ?? null,
      head,
      branch,
      indexDigest,
      digest: hash.digest("hex"),
      coverage,
      toolchainRevision: input.toolchainRevision,
      configRevision: input.configRevision,
    };
  });
  return VerificationFingerprint.of({ capture });
});

function sameFile(left: FileSystem.File.Info, right: FileSystem.File.Info): boolean {
  return (
    left.dev === right.dev &&
    Option.getOrNull(left.ino) === Option.getOrNull(right.ino) &&
    left.type === right.type &&
    left.mode === right.mode &&
    left.size === right.size
  );
}

export const layer = Layer.effect(VerificationFingerprint, make);
