import { describe, expect, it } from "@effect/vitest";

import {
  sanitizeVerificationOutput,
  verificationCommandContainsSecrets,
} from "./VerificationOutput.ts";

describe("verification output", () => {
  it("redacts known values across chunks, UTF-8 byte chunks and output strings", () => {
    const secret = "canary-🔒-credential";
    const bytes = new TextEncoder().encode(secret);
    const output = sanitizeVerificationOutput({
      stdout: [bytes.slice(0, 9), bytes.slice(9, 13), bytes.slice(13), "\nFAIL: "],
      stderr: "expected true; canary-🔒-",
      diagnostic: "credential",
      secrets: [secret],
    });
    expect(output.stdout).toBe("[REDACTED]\nFAIL: ");
    expect(output.stderr).toBe("expected true; [REDACTED]");
    expect(output.diagnostic).toBe("");
    expect(output.stdout + output.stderr + output.diagnostic).not.toContain(secret);
    expect(output.truncated).toBe(false);
  });

  it("redacts overlapping known secrets without revealing a suffix", () => {
    const output = sanitizeVerificationOutput({
      stdout: "abcdefg",
      stderr: "",
      diagnostic: "",
      secrets: ["abcde", "cdefg"],
    });
    expect(output.stdout).toBe("[REDACTED]");
  });

  it.each([
    "Authorization: Bearer credential-canary",
    "Authorization: Basic dXNlcjpwYXNz",
    'API_KEY="credential-canary"',
    '{"client_secret": "credential-canary"}',
    "https://user:credential-canary@example.invalid/path",
    "https://example.invalid/?X-Amz-Signature=credential-canary&ok=true",
    "https://example.invalid/?sig=credential-canary&sv=2026",
    "https://example.invalid/?X-Goog-Credential=credential-canary",
    "github_pat_credentialcanary0123456789",
    "-----BEGIN PRIVATE KEY-----\ncredential-canary\n-----END PRIVATE KEY-----",
  ])("redacts common credentials: %s", (value) => {
    const output = sanitizeVerificationOutput({
      stdout: value,
      stderr: "",
      diagnostic: "",
      secrets: [],
    });
    expect(output.stdout).toContain("[REDACTED]");
    expect(output.stdout).not.toContain("credential-canary");
    expect(verificationCommandContainsSecrets(value, [])).toBe(true);
  });

  it("redacts URL-encoded known secrets and ignores empty secrets", () => {
    const secret = "canary/space +?";
    const output = sanitizeVerificationOutput({
      stdout: encodeURIComponent(secret),
      stderr: "ordinary failure",
      diagnostic: "",
      secrets: ["", secret],
    });
    expect(output.stdout).toBe("[REDACTED]");
    expect(output.stderr).toBe("ordinary failure");
    expect(verificationCommandContainsSecrets("printf 'ordinary failure'", ["", secret])).toBe(
      false,
    );
  });

  it("sanitizes before bounding and never leaks a secret prefix at an upstream truncation", () => {
    const output = sanitizeVerificationOutput({
      stdout: "FAIL canary-se",
      stderr: "canary-sec",
      diagnostic: "failed check",
      stdoutTruncated: true,
      stderrTruncated: true,
      secrets: ["canary-secret-value"],
      maxBytes: 50,
    });
    expect(output.stdout).toBe("FAIL [REDACTED]");
    expect(output.stderr).toBe("");
    expect(output.diagnostic).toBe("failed check");
    expect(output.truncated).toBe(true);
  });

  it("hides a retained secret prefix when upstream truncation splits a UTF-8 code point", () => {
    const output = sanitizeVerificationOutput({
      stdout: "FAIL: canary-�",
      stderr: "",
      diagnostic: "",
      secrets: ["canary-🔒-secret"],
      stdoutTruncated: true,
    });
    expect(output.stdout).toBe("FAIL: [REDACTED]");
    expect(output.truncated).toBe(true);
  });

  it("redacts generic credentials across string boundaries and supports an in-memory secret matching the marker", () => {
    const output = sanitizeVerificationOutput({
      stdout: "Authorization: Bear",
      stderr: "er credential-canary",
      diagnostic: "",
      secrets: [],
    });
    expect(output.stdout + output.stderr).toBe("[REDACTED]");
    const marker = sanitizeVerificationOutput({
      stdout: "[REDACTED]",
      stderr: "",
      diagnostic: "",
      secrets: ["REDACTED"],
    });
    expect(marker.stdout).not.toContain("REDACTED");
  });

  it("bounds the aggregate, including diagnostics, without splitting a Unicode code point", () => {
    const output = sanitizeVerificationOutput({
      stdout: "🔒🔒",
      stderr: "error",
      diagnostic: "reason",
      secrets: [],
      maxBytes: 10,
    });
    expect(output.stdout).toBe("🔒🔒");
    expect(output.stderr).toBe("er");
    expect(output.diagnostic).toBe("");
    expect(output.truncated).toBe(true);
    expect(
      new TextEncoder().encode(output.stdout + output.stderr + output.diagnostic).byteLength,
    ).toBeLessThanOrEqual(10);
    const tiny = sanitizeVerificationOutput({
      stdout: "🔒",
      stderr: "",
      diagnostic: "",
      secrets: [],
      maxBytes: 3,
    });
    expect(tiny.stdout).toBe("");
  });

  it("preserves an exact byte limit and clamps a larger requested limit to 8 MiB", () => {
    expect(
      sanitizeVerificationOutput({
        stdout: "exact",
        stderr: "",
        diagnostic: "",
        secrets: [],
        maxBytes: 5,
      }).truncated,
    ).toBe(false);
    const output = sanitizeVerificationOutput({
      stdout: "x".repeat(8 * 1024 * 1024 + 1),
      stderr: "err",
      diagnostic: "diag",
      secrets: [],
      maxBytes: 32 * 1024 * 1024,
    });
    expect(output.stdout.length + output.stderr.length + output.diagnostic.length).toBe(
      8 * 1024 * 1024,
    );
    expect(output.truncated).toBe(true);
  });

  it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY, 1.5])(
    "rejects invalid byte budgets (%s) with a fixed error",
    (maxBytes) => {
      expect(() =>
        sanitizeVerificationOutput({
          stdout: "secret",
          stderr: "",
          diagnostic: "",
          secrets: [],
          maxBytes,
        }),
      ).toThrow("Invalid verification output byte limit.");
    },
  );
});
