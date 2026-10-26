import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId, type CheckoutIdentity, type VerificationConfig } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  compileVerificationInputPolicy,
  resolveVerificationInputs,
  VerificationInputLimits,
} from "./VerificationInputs.ts";

const fixture = Effect.fn("VerificationInputs.test.fixture")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const temporary = yield* fs.makeTempDirectoryScoped({ prefix: "t3-verification-inputs-" });
  const cwd = yield* fs.realPath(temporary);
  const identity: CheckoutIdentity = {
    environmentId: EnvironmentId.make("test"),
    checkoutKey: "fixture",
    canonicalCwd: cwd,
    filesystemIdentity: { device: "fixture", file: "fixture" },
    gitDir: null,
    gitCommonDir: null,
  };
  const config: VerificationConfig = {
    actionId: "test",
    automatic: false,
    timeoutMs: 1_000,
    inputs: { include: ["**/*"], exclude: ["**/dist/**"], nonGit: "fingerprint" },
  };
  const write = Effect.fn("VerificationInputs.test.write")(function* (name: string) {
    yield* fs.makeDirectory(path.dirname(path.join(cwd, name)), { recursive: true });
    yield* fs.writeFileString(path.join(cwd, name), "fixture");
  });
  return { fs, path, cwd, identity, config, write };
});

it.layer(NodeServices.layer)("verification inputs", (it) => {
  it.effect("resolves relative source patterns and excludes generated output", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      for (const file of [
        "src/main.ts",
        "src/nested/helper.ts",
        "src/main.js",
        "src/dist/bundle.ts",
        "other.ts",
      ]) {
        yield* f.write(file);
      }
      const inputs = yield* resolveVerificationInputs({
        ...f,
        config: { ...f.config, inputs: { ...f.config.inputs, include: ["src/**/*.ts"] } },
      });
      expect(inputs.paths).toEqual(["src/main.ts", "src/nested/helper.ts"]);
      expect(inputs.coverage.complete).toBe(true);
    }),
  );

  it.effect("matches anchored globstars and single-segment wildcards", () =>
    Effect.gen(function* () {
      const policy = yield* compileVerificationInputPolicy({
        include: ["src/**/file?.ts", "package.json"],
        exclude: ["**/dist/**"],
        nonGit: "fingerprint",
      });
      expect(policy.includes("src/file3.ts")).toBe(true);
      expect(policy.includes("src/deep/fileA.ts")).toBe(true);
      expect(policy.includes("src/file.ts")).toBe(false);
      expect(policy.includes("src/file3xx.ts")).toBe(false);
      expect(policy.includes("nested/package.json")).toBe(false);
      expect(policy.includes("src/dist/file3x.ts")).toBe(false);
      expect(policy.excluded("src/dist")).toBe(true);
      const single = yield* compileVerificationInputPolicy({
        include: ["src/*.ts"],
        exclude: [],
        nonGit: "fingerprint",
      });
      expect(single.includes("src/main.ts")).toBe(true);
      expect(single.includes("src/nested/main.ts")).toBe(false);
    }),
  );

  it.effect("rejects traversal, absolute paths and malformed patterns in either list", () =>
    Effect.gen(function* () {
      for (const pattern of [
        "../outside",
        "src/../../secret",
        "src\\..\\secret",
        "/tmp/file",
        "C:\\secret",
        "x\0y",
        "[",
        "foo**bar",
        "{a}",
      ]) {
        for (const field of ["include", "exclude"] as const) {
          const error = yield* compileVerificationInputPolicy({
            include: ["**/*"],
            exclude: [],
            nonGit: "fingerprint",
            [field]: [pattern],
          }).pipe(Effect.flip);
          expect(error.reason).toBe("invalid-pattern");
        }
      }
    }),
  );

  it.effect("marks escaped symlinks, missing inputs and uncovered targets as gaps", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const outside = yield* f.fs.makeTempFileScoped({ prefix: "t3-outside-input-" });
      yield* f.fs.writeFileString(outside, "external contents");
      yield* f.fs.symlink(outside, f.path.join(f.cwd, "escape"));
      yield* f.write("dist/generated");
      yield* f.fs.symlink("dist/generated", f.path.join(f.cwd, "generated-link"));
      yield* f.fs.symlink("missing", f.path.join(f.cwd, "broken-link"));
      const inputs = yield* resolveVerificationInputs({
        ...f,
        config: {
          ...f.config,
          inputs: { ...f.config.inputs, include: ["**/*", "required.json"] },
        },
      });
      expect(inputs.coverage.complete).toBe(false);
      expect(inputs.coverage.gaps).toEqual(
        expect.arrayContaining([
          "escaped-symlink",
          "missing-input",
          "symlink-target-outside-coverage",
          "unreadable-symlink",
        ]),
      );
      expect(inputs.paths).not.toContain("escape");
    }),
  );

  it.effect("skips Git internals and prunes excluded directories before reading them", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      for (const file of [
        "source.ts",
        ".git/objects/object",
        "nested/.git/config",
        "nested/dist/huge-output",
      ])
        yield* f.write(file);
      const read: string[] = [];
      const guarded = FileSystem.FileSystem.of({
        ...f.fs,
        readDirectory: (directory, options) => {
          read.push(directory);
          return f.fs.readDirectory(directory, options);
        },
      });
      const inputs = yield* resolveVerificationInputs(f).pipe(
        Effect.provideService(FileSystem.FileSystem, guarded),
      );
      expect(inputs.paths).toEqual(["source.ts"]);
      expect(inputs.coverage.complete).toBe(true);
      expect(read).not.toContain(f.path.join(f.cwd, "nested/dist"));
      expect(read.some((directory) => directory.includes("/.git"))).toBe(false);
    }),
  );

  it.effect(
    "bounds covered files, discovery entries and directory depth without claiming complete coverage",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.write("a.ts");
        yield* f.write("b.ts");
        yield* f.write("nested/deep/source.ts");
        const defaults = yield* VerificationInputLimits;
        for (const [override, gap] of [
          [{ maxFiles: 1 }, "file-limit"],
          [{ maxEntries: 1 }, "entry-limit"],
          [{ maxDepth: 1 }, "directory-depth-limit"],
        ] as const) {
          const inputs = yield* resolveVerificationInputs(f).pipe(
            Effect.provideService(VerificationInputLimits, { ...defaults, ...override }),
          );
          expect(inputs.coverage.complete).toBe(false);
          expect(inputs.coverage.gaps).toContain(gap);
        }
      }),
  );

  it.effect(
    "reports unreadable discovery and empty declarations instead of complete coverage",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.write("src/source.ts");
        const guarded = FileSystem.FileSystem.of({
          ...f.fs,
          readDirectory: (directory, options) =>
            f.fs.readDirectory(
              directory.endsWith("/src") ? f.path.join(f.cwd, "absent") : directory,
              options,
            ),
        });
        const unreadable = yield* resolveVerificationInputs(f).pipe(
          Effect.provideService(FileSystem.FileSystem, guarded),
        );
        expect(unreadable.coverage.complete).toBe(false);
        expect(unreadable.coverage.gaps).toContain("unreadable-directory");
        const empty = yield* resolveVerificationInputs({
          ...f,
          config: {
            ...f.config,
            inputs: { ...f.config.inputs, include: [] },
          },
        });
        expect(empty.coverage.gaps).toContain("no-inputs-declared");
        const defaults = yield* VerificationInputLimits;
        const invalid = yield* resolveVerificationInputs(f).pipe(
          Effect.provideService(VerificationInputLimits, { ...defaults, maxFiles: 0 }),
          Effect.flip,
        );
        expect(invalid.reason).toBe("invalid-limits");
      }),
  );

  it.effect("returns explicit non-Git unsupported and rejects a noncanonical checkout", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const unsupported = yield* resolveVerificationInputs({
        ...f,
        config: {
          ...f.config,
          inputs: { ...f.config.inputs, nonGit: "unsupported" },
        },
      }).pipe(Effect.flip);
      expect(unsupported.reason).toBe("non-git-unsupported");
      const invalid = yield* resolveVerificationInputs({
        ...f,
        identity: {
          ...f.identity,
          canonicalCwd: `${f.cwd}/.`,
        },
      }).pipe(Effect.flip);
      expect(invalid.reason).toBe("invalid-root");
    }),
  );

  it.effect(
    "matches dotfiles, newline filenames and literal directories with bounded wildcard work",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        for (const file of [".env", "src/line\nbreak.ts", "src/emoji😀.ts"]) yield* f.write(file);
        const all = yield* resolveVerificationInputs({
          ...f,
          config: {
            ...f.config,
            inputs: { ...f.config.inputs, include: ["**"] },
          },
        });
        expect(all.paths).toEqual([".env", "src/emoji😀.ts", "src/line\nbreak.ts"]);
        expect(all.coverage.complete).toBe(true);
        const literal = yield* resolveVerificationInputs({
          ...f,
          config: {
            ...f.config,
            inputs: { ...f.config.inputs, include: ["src"] },
          },
        });
        expect(literal.paths).toEqual(["src/emoji😀.ts", "src/line\nbreak.ts"]);
        const repeated = yield* compileVerificationInputPolicy({
          include: [`${"*a".repeat(40)}b`],
          exclude: [],
          nonGit: "fingerprint",
        });
        expect(repeated.includes(`${"a".repeat(80)}c`)).toBe(false);
        const unicode = yield* compileVerificationInputPolicy({
          include: ["src/emoji?.ts"],
          exclude: [],
          nonGit: "fingerprint",
        });
        expect(unicode.includes("src/emoji😀.ts")).toBe(true);
      }),
  );

  it.effect("excluded outputs do not consume discovery limits or create coverage gaps", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.write("source.ts");
      const limits = { ...(yield* VerificationInputLimits), maxEntries: 1 };
      const before = yield* resolveVerificationInputs(f).pipe(
        Effect.provideService(VerificationInputLimits, limits),
      );
      yield* f.write("dist/build.ts");
      const after = yield* resolveVerificationInputs(f).pipe(
        Effect.provideService(VerificationInputLimits, limits),
      );
      expect(after.coverage).toEqual(before.coverage);
      expect(after.coverage.complete).toBe(true);
      expect(after.paths).toEqual(before.paths);
    }),
  );

  it.effect("normalizes policy order and bounds pattern counts and lengths", () =>
    Effect.gen(function* () {
      const config = {
        include: ["src", "package.json"],
        exclude: ["dist", "coverage"],
        nonGit: "fingerprint",
      } as const;
      const first = yield* compileVerificationInputPolicy(config);
      const reordered = yield* compileVerificationInputPolicy({
        ...config,
        include: ["package.json", "src", "src"],
        exclude: ["coverage", "dist"],
      });
      expect(first.revision).toBe(reordered.revision);
      const limits = yield* VerificationInputLimits;
      for (const override of [{ maxPatterns: 1 }, { maxPatternLength: 1 }]) {
        const error = yield* compileVerificationInputPolicy(config).pipe(
          Effect.provideService(VerificationInputLimits, { ...limits, ...override }),
          Effect.flip,
        );
        expect(error.reason).toBe("invalid-pattern");
      }
    }),
  );

  it.effect(
    "only requires symlink contents and directory scans that can match the declared inputs",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.write("src/main.ts");
        const outside = yield* f.fs.makeTempFileScoped({ prefix: "t3-uncovered-input-" });
        yield* f.fs.symlink(outside, f.path.join(f.cwd, "src/irrelevant.txt"));
        const config = { ...f.config, inputs: { ...f.config.inputs, include: ["src/*.ts"] } };
        const resolved = yield* resolveVerificationInputs({ ...f, config });
        expect(resolved.paths).toEqual(["src/main.ts"]);
        expect(resolved.coverage.complete).toBe(true);
        const recursive = yield* resolveVerificationInputs({
          ...f,
          config: {
            ...config,
            inputs: { ...config.inputs, include: ["src/**/*.ts"] },
          },
        });
        expect(recursive.coverage.complete).toBe(true);
      }),
  );
});
