// @effect-diagnostics nodeBuiltinImport:off - inject lossless native stat identities and deterministic filesystem races.
import * as NodeFSP from "node:fs/promises";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import { ExitCode } from "effect/process/ChildProcessSpawner";
import { vi } from "vite-plus/test";

import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ProcessRunner from "../processRunner.ts";
import * as CheckoutIdentity from "./CheckoutIdentity.ts";

vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeFSP>();
  return { ...actual, stat: vi.fn(actual.stat) };
});

const environmentId = EnvironmentId.make("identity-test-environment");
const layerDependencies = ProcessRunner.layer.pipe(Layer.provideMerge(NodeServices.layer));
const layerTest = CheckoutIdentity.layer.pipe(Layer.provideMerge(layerDependencies));

const git = Effect.fn("CheckoutIdentity.test.git")(function* (
  cwd: string,
  args: ReadonlyArray<string>,
) {
  const runner = yield* ProcessRunner.ProcessRunner;
  const environment = yield* HostProcess.Environment;
  const output = yield* runner.run({
    command: "git",
    args: [
      "-c",
      "core.fsmonitor=false",
      "-c",
      "core.hooksPath=",
      "-c",
      "init.templateDir=",
      ...args,
    ],
    cwd,
    env: {
      ...Object.fromEntries(
        Object.keys(environment)
          .filter((key) => key.toUpperCase().startsWith("GIT_"))
          .map((key) => [key, undefined]),
      ),
      GIT_CONFIG_COUNT: "0",
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_CONFIG_GLOBAL: "",
    },
  });
  expect(output.code, output.stderr).toBe(0);
  return output.stdout;
});

const makeRepo = Effect.fn("CheckoutIdentity.test.makeRepo")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-repo-" });
  const cwd = path.join(root, "repository");
  yield* fs.makeDirectory(cwd);
  yield* git(cwd, ["init", "--initial-branch=main"]);
  yield* git(cwd, [
    "-c",
    "user.name=Identity Fixture",
    "-c",
    "user.email=identity@example.invalid",
    "-c",
    "commit.gpgsign=false",
    "commit",
    "--allow-empty",
    "-m",
    "fixture",
  ]);
  return { root, cwd };
});

const nativeStat = Effect.fn("CheckoutIdentity.test.nativeStat")(function* () {
  const native = yield* Effect.promise(() => vi.importActual<typeof NodeFSP>("node:fs/promises"));
  const mocked = vi.mocked(NodeFSP.stat);
  mocked.mockReset().mockImplementation(native.stat);
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => mocked.mockReset().mockImplementation(native.stat)),
  );
  return { native, mocked };
});

const resolverWithRunner = (runner: ProcessRunner.ProcessRunner["Service"]) =>
  Effect.service(CheckoutIdentity.CheckoutIdentityResolver).pipe(
    Effect.provide(Layer.fresh(CheckoutIdentity.layer)),
    Effect.provideService(ProcessRunner.ProcessRunner, runner),
  );

describe("CheckoutIdentityResolver", () => {
  it.live("resolves a stable identity for a non-Git directory", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-identity-" });
      const first = yield* resolver.resolve({ environmentId, cwd });
      const second = yield* resolver.resolve({ environmentId, cwd });

      expect(first).toEqual(second);
      expect(first.canonicalCwd).toBe(yield* fs.realPath(cwd));
      expect(first.gitDir).toBeNull();
      expect(first.gitCommonDir).toBeNull();
      expect(first.filesystemIdentity.file).toMatch(/^\d+$/);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("separates the same physical directory in different environments", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-environments-" });
      const first = yield* resolver.resolve({ environmentId, cwd });
      const second = yield* resolver.resolve({
        environmentId: EnvironmentId.make("another-environment"),
        cwd,
      });

      expect(first.filesystemIdentity).toEqual(second.filesystemIdentity);
      expect(first.checkoutKey).not.toBe(second.checkoutKey);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("accepts a plain directory containing a file named HEAD", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-plain-head-" });
      yield* fs.writeFileString(path.join(cwd, "HEAD"), "ordinary project file");
      const identity = yield* resolver.resolve({ environmentId, cwd });
      expect(identity.gitDir).toBeNull();
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("collapses real symlink and redundant path aliases", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-aliases-" });
      const cwd = path.join(root, "checkout");
      const alias = path.join(root, "alias");
      yield* fs.makeDirectory(cwd);
      yield* fs.symlink(cwd, alias);
      const first = yield* resolver.resolve({ environmentId, cwd });
      const second = yield* resolver.resolve({ environmentId, cwd: path.join(alias, ".") });

      expect(second).toEqual(first);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("records canonical Git metadata and keeps worktrees independent", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const { root, cwd } = yield* makeRepo();
      const worktree = path.join(root, "worktree");
      yield* git(cwd, ["worktree", "add", "--detach", worktree, "HEAD"]);
      const first = yield* resolver.resolve({ environmentId, cwd });
      const second = yield* resolver.resolve({ environmentId, cwd: worktree });
      const alias = path.join(root, "worktree-alias");
      yield* fs.symlink(worktree, alias);
      const aliased = yield* resolver.resolve({ environmentId, cwd: alias });

      expect(first.gitDir).toBe(yield* fs.realPath(path.join(cwd, ".git")));
      expect(first.gitCommonDir).toBe(first.gitDir);
      expect(second.gitCommonDir).toBe(first.gitCommonDir);
      expect(second.gitDir).not.toBe(first.gitDir);
      expect(second.checkoutKey).not.toBe(first.checkoutKey);
      expect(aliased).toEqual(second);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("ignores branch names and identical remote URLs when deriving keys", () =>
    Effect.gen(function* () {
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const firstRepo = yield* makeRepo();
      const secondRepo = yield* makeRepo();
      for (const repo of [firstRepo, secondRepo]) {
        yield* git(repo.cwd, ["remote", "add", "origin", "https://example.invalid/same.git"]);
      }
      const first = yield* resolver.resolve({ environmentId, cwd: firstRepo.cwd });
      const second = yield* resolver.resolve({ environmentId, cwd: secondRepo.cwd });
      yield* git(firstRepo.cwd, ["checkout", "-b", "renamed"]);
      const renamed = yield* resolver.resolve({ environmentId, cwd: firstRepo.cwd });

      expect(first.checkoutKey).not.toBe(second.checkoutKey);
      expect(renamed.checkoutKey).toBe(first.checkoutKey);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("supports bare and separate Git directories", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-layouts-" });
      const bare = path.join(root, "bare.git");
      yield* git(root, ["init", "--bare", bare]);
      const bareIdentity = yield* resolver.resolve({ environmentId, cwd: bare });
      expect(bareIdentity.gitDir).toBe(yield* fs.realPath(bare));
      expect(bareIdentity.gitCommonDir).toBe(bareIdentity.gitDir);

      const cwd = path.join(root, "checkout with spaces ");
      const metadata = path.join(root, "metadata with spaces ");
      yield* fs.makeDirectory(cwd);
      yield* git(cwd, ["init", `--separate-git-dir=${metadata}`]);
      const separateIdentity = yield* resolver.resolve({ environmentId, cwd });
      expect(separateIdentity.gitDir).toBe(yield* fs.realPath(metadata));
      expect(separateIdentity.canonicalCwd).toBe(yield* fs.realPath(cwd));
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("preserves device and inode values above Number.MAX_SAFE_INTEGER", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-bigint-" });
      const { native, mocked } = yield* nativeStat();
      const info = yield* Effect.promise(() => native.stat(cwd, { bigint: true }));
      mocked.mockResolvedValue(
        Object.assign(info, { dev: 9007199254740993n, ino: 18446744073709551615n }),
      );
      const identity = yield* resolver.resolve({ environmentId, cwd });

      expect(identity.filesystemIdentity).toEqual({
        device: "9007199254740993",
        file: "18446744073709551615",
      });
      mocked.mockResolvedValue(Object.assign(info, { ino: 18446744073709551614n }));
      const nextInode = yield* resolver.resolve({ environmentId, cwd });
      expect(nextInode.checkoutKey).not.toBe(identity.checkoutKey);
      mocked.mockResolvedValue(Object.assign(info, { dev: 9007199254740994n }));
      const nextDevice = yield* resolver.resolve({ environmentId, cwd });
      expect(nextDevice.checkoutKey).not.toBe(nextInode.checkoutKey);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live(
    "uses physical IDs for differently spelled canonical aliases without merging other directories",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-physical-alias-" });
        const first = path.join(root, "first-spelling");
        const alias = path.join(root, "another-spelling");
        yield* fs.makeDirectory(first);
        yield* fs.makeDirectory(alias);
        const { native, mocked } = yield* nativeStat();
        const info = yield* Effect.promise(() => native.stat(first, { bigint: true }));
        // Model a case/short-name/bind-mount alias that realPath does not unify.
        mocked.mockResolvedValue(info);
        const identity = yield* resolver.resolve({ environmentId, cwd: first });
        const aliasIdentity = yield* resolver.resolve({ environmentId, cwd: alias });
        expect(identity.canonicalCwd).not.toBe(aliasIdentity.canonicalCwd);
        expect(identity.checkoutKey).toBe(aliasIdentity.checkoutKey);

        mocked.mockResolvedValue(Object.assign(info, { ino: info.ino + 1n }));
        const different = yield* resolver.resolve({ environmentId, cwd: alias });
        expect(different.checkoutKey).not.toBe(identity.checkoutKey);
      }).pipe(Effect.provide(layerTest)),
  );

  it.live("preserves a native stat failure as a typed unresolvable identity", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-stat-failure-" });
      const { mocked } = yield* nativeStat();
      const cause = Object.assign(new Error("fixture denied"), { code: "EACCES" });
      mocked.mockRejectedValue(cause);
      const error = yield* resolver.resolve({ environmentId, cwd }).pipe(Effect.flip);
      expect(error.reason).toBe("unresolvable");
      expect(error.stage).toBe("stat");
      expect(error.cause).toBe(cause);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("rejects unavailable filesystem identities", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-no-inode-" });
      const { native, mocked } = yield* nativeStat();
      const info = yield* Effect.promise(() => native.stat(cwd, { bigint: true }));
      mocked.mockResolvedValue(Object.assign(info, { ino: 0n }));
      const error = yield* resolver.resolve({ environmentId, cwd }).pipe(Effect.flip);

      expect(error.reason).toBe("unavailable-identity");
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("rejects missing directories and regular files with typed errors", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-invalid-" });
      const file = path.join(root, "file");
      yield* fs.writeFileString(file, "fixture");
      const missing = yield* resolver
        .resolve({ environmentId, cwd: path.join(root, "missing") })
        .pipe(Effect.flip);
      const notDirectory = yield* resolver.resolve({ environmentId, cwd: file }).pipe(Effect.flip);

      expect(missing._tag).toBe("CheckoutIdentityError");
      expect(missing.reason).toBe("unresolvable");
      expect(notDirectory.reason).toBe("not-directory");
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("does not reclassify broken Git directories or gitfiles as non-Git", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const { cwd } = yield* makeRepo();
      yield* fs.remove(path.join(cwd, ".git", "HEAD"));
      const broken = yield* resolver.resolve({ environmentId, cwd }).pipe(Effect.flip);
      expect(broken.reason).toBe("git-failed");

      yield* fs.remove(path.join(cwd, ".git"), { recursive: true });
      yield* fs.writeFileString(path.join(cwd, ".git"), "gitdir: absent\n");
      const gitfile = yield* resolver.resolve({ environmentId, cwd }).pipe(Effect.flip);
      expect(gitfile.reason).toBe("git-failed");

      yield* fs.remove(path.join(cwd, ".git"));
      yield* fs.symlink(path.join(cwd, "absent"), path.join(cwd, ".git"));
      const dangling = yield* resolver.resolve({ environmentId, cwd }).pipe(Effect.flip);
      expect(dangling.reason).toBe("git-failed");
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("does not reclassify a broken bare repository as non-Git", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-broken-bare-" });
      yield* git(cwd, ["init", "--bare"]);
      yield* fs.remove(path.join(cwd, "HEAD"));
      const error = yield* resolver.resolve({ environmentId, cwd }).pipe(Effect.flip);
      expect(error.reason).toBe("git-failed");
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("rejects arbitrary Git failures and preserves process errors as causes", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-git-failure-" });
      const cause = new ProcessRunner.ProcessSpawnError({
        command: "git",
        argumentCount: 0,
        cause: new Error("fixture"),
      });
      const resolver = yield* resolverWithRunner({ run: () => Effect.fail(cause) });
      const error = yield* resolver.resolve({ environmentId, cwd }).pipe(Effect.flip);
      expect(error.reason).toBe("git-failed");
      expect(error.cause).toBe(cause);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("rejects malformed or truncated successful Git output", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const runner = yield* ProcessRunner.ProcessRunner;
      const { cwd } = yield* makeRepo();
      for (const override of [
        { stdout: "relative/path\nrelative/common\n" },
        { stdoutTruncated: true },
        { stdoutInvalidUtf8: true },
        { stderrTruncated: true },
        { stderrInvalidUtf8: true },
        { timedOut: true },
        { stdout: "" },
      ]) {
        const resolver = yield* resolverWithRunner({
          run: (input) =>
            runner.run(input).pipe(Effect.map((output) => ({ ...output, ...override }))),
        });
        const error = yield* resolver.resolve({ environmentId, cwd }).pipe(Effect.flip);
        expect(error.reason).toBe("invalid-git-output");
      }
      expect(yield* fs.exists(cwd)).toBe(true);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("does not trust a non-repository diagnostic when metadata exists in an ancestor", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const runner = yield* ProcessRunner.ProcessRunner;
      const { cwd } = yield* makeRepo();
      const child = path.join(cwd, "nested");
      yield* fs.makeDirectory(child);
      yield* fs.remove(path.join(cwd, ".git", "HEAD"));
      const resolver = yield* resolverWithRunner({
        run: (input) =>
          runner.run(input).pipe(
            Effect.map((output) => ({
              ...output,
              code: ExitCode(128),
              stdout: "",
              stderr: "fatal: not a git repository (or any of the parent directories): .git\n",
            })),
          ),
      });
      const error = yield* resolver.resolve({ environmentId, cwd: child }).pipe(Effect.flip);
      expect(error.reason).toBe("git-failed");
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("ignores ambient Git discovery and configuration overrides", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const { cwd } = yield* makeRepo();
      const plain = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-ambient-" });
      const ambient = yield* HostProcess.Environment;
      const runner = yield* ProcessRunner.ProcessRunner;
      const inherited = {
        GIT_DIR: path.join(cwd, ".git"),
        GIT_WORK_TREE: cwd,
        GIT_COMMON_DIR: path.join(cwd, ".git"),
        GIT_CONFIG_COUNT: "1",
        GIT_CONFIG_KEY_0: "core.worktree",
        GIT_CONFIG_VALUE_0: cwd,
      };
      const redirected = yield* runner.run({
        command: "git",
        args: ["rev-parse", "--absolute-git-dir"],
        cwd: plain,
        env: inherited,
      });
      expect(redirected.code).toBe(0);
      expect(redirected.stdout).toContain(".git");
      const resolver = yield* Effect.service(CheckoutIdentity.CheckoutIdentityResolver).pipe(
        Effect.provide(Layer.fresh(CheckoutIdentity.layer)),
        Effect.provideService(HostProcess.Environment, {
          ...ambient,
          ...inherited,
        }),
        Effect.provideService(ProcessRunner.ProcessRunner, {
          run: (input) => runner.run({ ...input, env: { ...inherited, ...input.env } }),
        }),
      );
      const identity = yield* resolver.resolve({ environmentId, cwd: plain });
      expect(identity.gitDir).toBeNull();
      expect(identity.gitCommonDir).toBeNull();
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("fails closed when the physical root changes during Git discovery", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const runner = yield* ProcessRunner.ProcessRunner;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-replaced-" });
      const cwd = path.join(root, "checkout");
      yield* fs.makeDirectory(cwd);
      let changed = false;
      const resolver = yield* resolverWithRunner({
        run: Effect.fn(function* (input) {
          const result = yield* runner.run(input);
          if (!changed) {
            changed = true;
            yield* fs.rename(cwd, path.join(root, "original")).pipe(Effect.orDie);
            yield* fs.makeDirectory(cwd).pipe(Effect.orDie);
          }
          return result;
        }),
      });
      const error = yield* resolver.resolve({ environmentId, cwd }).pipe(Effect.flip);
      expect(error.reason).toBe("unstable");
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("fails closed when an input symlink is retargeted during discovery", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const runner = yield* ProcessRunner.ProcessRunner;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkout-retargeted-" });
      const first = path.join(root, "first");
      const second = path.join(root, "second");
      const alias = path.join(root, "alias");
      yield* fs.makeDirectory(first);
      yield* fs.makeDirectory(second);
      yield* fs.symlink(first, alias);
      let changed = false;
      const resolver = yield* resolverWithRunner({
        run: Effect.fn(function* (input) {
          const result = yield* runner.run(input);
          if (!changed) {
            changed = true;
            yield* fs.remove(alias).pipe(Effect.orDie);
            yield* fs.symlink(second, alias).pipe(Effect.orDie);
          }
          return result;
        }),
      });
      const error = yield* resolver.resolve({ environmentId, cwd: alias }).pipe(Effect.flip);
      expect(error.reason).toBe("unstable");
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("keys include physical Git-directory identity and detect its replacement", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const runner = yield* ProcessRunner.ProcessRunner;
      const resolver = yield* CheckoutIdentity.CheckoutIdentityResolver;
      const { cwd } = yield* makeRepo();
      const first = yield* resolver.resolve({ environmentId, cwd });
      yield* fs.rename(path.join(cwd, ".git"), path.join(cwd, "old-metadata"));
      yield* git(cwd, ["init"]);
      const second = yield* resolver.resolve({ environmentId, cwd });
      expect(second.filesystemIdentity).toEqual(first.filesystemIdentity);
      expect(second.checkoutKey).not.toBe(first.checkoutKey);

      let changed = false;
      const unstable = yield* resolverWithRunner({
        run: Effect.fn(function* (input) {
          const result = yield* runner.run(input);
          // Change after the first snapshot has read Git directory identities.
          if (!changed && input.args.includes("rev-parse")) {
            changed = true;
            return result;
          }
          yield* fs
            .rename(path.join(cwd, ".git"), path.join(cwd, "replaced-metadata"))
            .pipe(Effect.orDie);
          yield* git(cwd, ["init"]).pipe(
            Effect.provideService(ProcessRunner.ProcessRunner, runner),
            Effect.orDie,
          );
          return result;
        }),
      });
      const error = yield* unstable.resolve({ environmentId, cwd }).pipe(Effect.flip);
      expect(error.reason).toBe("unstable");
    }).pipe(Effect.provide(layerTest)),
  );
});
