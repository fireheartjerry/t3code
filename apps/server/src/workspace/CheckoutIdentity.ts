// @effect-diagnostics nodeBuiltinImport:off - FileSystem lacks lstat and lossless bigint IDs; crypto hashes only identity.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";

import { type CheckoutIdentity, type EnvironmentId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import * as ProcessRunner from "../processRunner.ts";

export class CheckoutIdentityError extends Schema.TaggedError<CheckoutIdentityError>()(
  "CheckoutIdentityError",
  {
    cwd: Schema.String,
    path: Schema.String,
    stage: Schema.Literals(["canonicalize", "stat", "git-discovery", "stability"]),
    reason: Schema.Literals([
      "unresolvable",
      "not-directory",
      "unavailable-identity",
      "git-failed",
      "invalid-git-output",
      "unstable",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to resolve checkout identity for '${this.cwd}' during ${this.stage}: ${this.reason}.`;
  }
}

const sameDirectory = Schema.toEquivalence(
  Schema.Struct({
    canonicalPath: Schema.String,
    device: Schema.String,
    file: Schema.String,
  }),
);
const encodeKeyParts = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

export class CheckoutIdentityResolver extends Context.Service<
  CheckoutIdentityResolver,
  {
    readonly resolve: (input: {
      readonly environmentId: EnvironmentId;
      readonly cwd: string;
    }) => Effect.Effect<CheckoutIdentity, CheckoutIdentityError>;
  }
>()("t3/workspace/CheckoutIdentity/CheckoutIdentityResolver") {}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const runner = yield* ProcessRunner.ProcessRunner;
  const environment = yield* HostProcess.Environment;

  const statDirectory = Effect.fn("CheckoutIdentityResolver.statDirectory")(function* (
    cwd: string,
    directory: string,
  ) {
    const info = yield* Effect.tryPromise({
      try: () => NodeFSP.stat(directory, { bigint: true }),
      catch: (cause) =>
        new CheckoutIdentityError({
          cwd,
          path: directory,
          stage: "stat",
          reason: "unresolvable",
          cause,
        }),
    });
    if (!info.isDirectory()) {
      return yield* new CheckoutIdentityError({
        cwd,
        path: directory,
        stage: "stat",
        reason: "not-directory",
      });
    }
    // An unavailable file ID cannot safely fall back to a path-based lock.
    if (
      typeof info.dev !== "bigint" ||
      typeof info.ino !== "bigint" ||
      info.dev < 0n ||
      info.ino <= 0n
    ) {
      return yield* new CheckoutIdentityError({
        cwd,
        path: directory,
        stage: "stat",
        reason: "unavailable-identity",
      });
    }
    return { device: info.dev.toString(), file: info.ino.toString() };
  });

  const inspectDirectory = Effect.fn("CheckoutIdentityResolver.inspectDirectory")(function* (
    cwd: string,
    directory: string,
  ) {
    const canonicalPath = yield* fs.realPath(directory).pipe(
      Effect.mapError(
        (cause) =>
          new CheckoutIdentityError({
            cwd,
            path: directory,
            stage: "canonicalize",
            reason: "unresolvable",
            cause,
          }),
      ),
    );
    const identity = yield* statDirectory(cwd, canonicalPath);
    const original = yield* statDirectory(cwd, directory);
    if (identity.device !== original.device || identity.file !== original.file) {
      return yield* new CheckoutIdentityError({
        cwd,
        path: directory,
        stage: "stability",
        reason: "unstable",
      });
    }
    return { canonicalPath, ...identity };
  });

  const markerExists = Effect.fn("CheckoutIdentityResolver.markerExists")(function* (
    cwd: string,
    marker: string,
  ) {
    return yield* Effect.tryPromise({
      try: () => NodeFSP.lstat(marker),
      catch: (cause) =>
        new CheckoutIdentityError({
          cwd,
          path: marker,
          stage: "git-discovery",
          reason: "unresolvable",
          cause,
        }),
    }).pipe(
      Effect.as(true),
      Effect.catchIf(
        (error) => Predicate.hasProperty(error.cause, "code") && error.cause.code === "ENOENT",
        () => Effect.succeed(false),
      ),
    );
  });

  // A damaged .git directory can produce Git's ordinary "not a repository" diagnostic.
  // lstat detects dangling markers without listing potentially enormous directories.
  const hasGitMetadata = Effect.fn("CheckoutIdentityResolver.hasGitMetadata")(function* (
    cwd: string,
    directory: string,
  ) {
    let current = directory;
    while (true) {
      if (yield* markerExists(cwd, path.join(current, ".git"))) return true;
      if (
        (yield* markerExists(cwd, path.join(current, "objects"))) &&
        ((yield* markerExists(cwd, path.join(current, "HEAD"))) ||
          (yield* markerExists(cwd, path.join(current, "refs"))))
      ) {
        return true;
      }
      const parent = path.dirname(current);
      if (parent === current) return false;
      current = parent;
    }
  });

  const discoverGit = Effect.fn("CheckoutIdentityResolver.discoverGit")(function* (
    cwd: string,
    canonicalCwd: string,
  ) {
    // ProcessRunner extends the process environment. Undefined entries explicitly erase
    // inherited Git overrides, rather than redirecting discovery into an unrelated repo.
    const env: NodeJS.ProcessEnv = {
      ...Object.fromEntries(
        Object.keys(environment)
          .filter((key) => key.toUpperCase().startsWith("GIT_"))
          .map((key) => [key, undefined]),
      ),
      GIT_CONFIG_COUNT: "0",
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_CONFIG_GLOBAL: "",
      GIT_OPTIONAL_LOCKS: "0",
      GIT_TERMINAL_PROMPT: "0",
      GIT_DISCOVERY_ACROSS_FILESYSTEM: "1",
      LC_ALL: "C",
      LANG: "C",
    };
    const output = yield* runner
      .run({
        command: "git",
        args: [
          "-c",
          "core.fsmonitor=false",
          "rev-parse",
          "--path-format=absolute",
          "--absolute-git-dir",
          "--git-common-dir",
        ],
        cwd: canonicalCwd,
        env,
        timeout: "10 seconds",
        maxOutputBytes: 64 * 1024,
        outputMode: "error",
      })
      .pipe(
        Effect.mapError(
          (cause) =>
            new CheckoutIdentityError({
              cwd,
              path: canonicalCwd,
              stage: "git-discovery",
              reason: "git-failed",
              cause,
            }),
        ),
      );
    if (
      output.timedOut ||
      output.stdoutTruncated ||
      output.stderrTruncated ||
      output.stdoutInvalidUtf8 ||
      output.stderrInvalidUtf8
    ) {
      return yield* new CheckoutIdentityError({
        cwd,
        path: canonicalCwd,
        stage: "git-discovery",
        reason: "invalid-git-output",
        cause: output,
      });
    }
    if (output.code !== 0) {
      if (
        output.code === 128 &&
        output.stdout === "" &&
        /^fatal: not a git repository \(or any of the parent directories\): \.git\r?\n?$/.test(
          output.stderr,
        ) &&
        !(yield* hasGitMetadata(cwd, canonicalCwd))
      )
        return null;
      return yield* new CheckoutIdentityError({
        cwd,
        path: canonicalCwd,
        stage: "git-discovery",
        reason: "git-failed",
        cause: output,
      });
    }
    // Remove only the record terminator. Whitespace can be part of a real directory name.
    const directories = output.stdout.replace(/\r?\n$/, "").split(/\r?\n/);
    if (
      directories.length !== 2 ||
      directories.some((directory) => !path.isAbsolute(directory) || directory.includes("\0"))
    ) {
      return yield* new CheckoutIdentityError({
        cwd,
        path: canonicalCwd,
        stage: "git-discovery",
        reason: "invalid-git-output",
        cause: output,
      });
    }
    const gitDir = yield* inspectDirectory(cwd, directories[0]!);
    const gitCommonDir = yield* inspectDirectory(cwd, directories[1]!);
    return { gitDir, gitCommonDir };
  });

  const resolve: CheckoutIdentityResolver["Service"]["resolve"] = Effect.fn(
    "CheckoutIdentityResolver.resolve",
  )(function* ({ environmentId, cwd }) {
    const directory = yield* inspectDirectory(cwd, cwd);
    const git = yield* discoverGit(cwd, directory.canonicalPath);
    const afterDirectory = yield* inspectDirectory(cwd, cwd);
    const afterGit = yield* discoverGit(cwd, afterDirectory.canonicalPath);
    const finalDirectory = yield* inspectDirectory(cwd, cwd);
    const gitStable =
      git === null
        ? afterGit === null
        : afterGit !== null &&
          sameDirectory(git.gitDir, afterGit.gitDir) &&
          sameDirectory(git.gitCommonDir, afterGit.gitCommonDir);
    if (
      !sameDirectory(directory, afterDirectory) ||
      !sameDirectory(directory, finalDirectory) ||
      !gitStable
    ) {
      return yield* new CheckoutIdentityError({
        cwd,
        path: directory.canonicalPath,
        stage: "stability",
        reason: "unstable",
      });
    }
    // Paths are display evidence. Physical IDs collapse case, short-name and symlink
    // aliases without lowercasing paths. The common dir never merges separate worktrees.
    const tuple = [
      environmentId,
      directory.device,
      directory.file,
      ...(git === null ? ["non-git"] : ["git", git.gitDir.device, git.gitDir.file]),
    ];
    const checkoutKey = `checkout-v1:${NodeCrypto.createHash("sha256").update(encodeKeyParts(tuple)).digest("hex")}`;
    return {
      environmentId,
      canonicalCwd: directory.canonicalPath,
      checkoutKey,
      filesystemIdentity: { device: directory.device, file: directory.file },
      gitDir: git?.gitDir.canonicalPath ?? null,
      gitCommonDir: git?.gitCommonDir.canonicalPath ?? null,
    };
  });
  return CheckoutIdentityResolver.of({ resolve });
});

export const layer = Layer.effect(CheckoutIdentityResolver, make);
