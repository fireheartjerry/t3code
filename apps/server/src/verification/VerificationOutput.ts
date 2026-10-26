import { VERIFICATION_MAX_OUTPUT_BYTES } from "@t3tools/contracts";

type OutputText = string | ReadonlyArray<string | Uint8Array>;

interface Span {
  start: number;
  end: number;
}

/** Best-effort credential patterns supplement exact in-memory secrets; this is not a generic secret detector. */
function credentialSpans(text: string, secrets: readonly string[]): Span[] {
  const spans: Span[] = [];
  for (const secret of secrets) {
    let offset = 0;
    for (;;) {
      const start = text.indexOf(secret, offset);
      if (start === -1) break;
      spans.push({ start, end: start + secret.length });
      offset = start + 1;
    }
  }
  for (const pattern of [
    /\b(?:authorization\s*:\s*)?(?:Bearer|Basic)\s+[^\s"'<>;,]+/gi,
    /\b(?:[A-Z][A-Z0-9_]*_)?(?:API[_-]?KEY|ACCESS[_-]?TOKEN|REFRESH[_-]?TOKEN|TOKEN|PASSWORD|PASSWD|SECRET|CLIENT[_-]?SECRET|SECRET[_-]?ACCESS[_-]?KEY|ACCESS[_-]?KEY[_-]?ID|SESSION[_-]?TOKEN|AUTHORIZATION)["']?\s*[:=]\s*(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s,;&<>]+)/gi,
    /\b[a-z][a-z0-9+.-]*:\/\/[^\s/@]+:[^\s/@]*@/gi,
    /[?&](?:x-amz-[a-z0-9-]+|x-goog-[a-z0-9-]+|AWSAccessKeyId|GoogleAccessId|signature|sig|access_token|refresh_token|token|api[_-]?key|credential|key)=[^&#\s"'<>]*/gi,
    /\b(?:github_pat_|gh[pousr]_|sk-(?:proj-|ant-)?)[A-Za-z0-9_-]+/g,
    /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/g,
    /\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g,
    /-----BEGIN (?:[A-Z]+ )?PRIVATE KEY-----[\s\S]*?(?:-----END (?:[A-Z]+ )?PRIVATE KEY-----|$)/g,
  ]) {
    for (const match of text.matchAll(pattern)) {
      spans.push({ start: match.index, end: match.index + match[0].length });
    }
  }
  return spans;
}

function secretSpellings(secrets: readonly string[]): string[] {
  return [
    ...new Set(
      secrets
        .filter((secret) => secret.length > 0)
        .flatMap((secret) => {
          // Malformed surrogate strings still need exact redaction, even when URI encoding rejects them.
          try {
            return [secret, encodeURIComponent(secret), encodeURI(secret)];
          } catch {
            return [secret];
          }
        }),
    ),
  ];
}

function decode(text: OutputText): string {
  if (typeof text === "string") return text;
  const decoder = new TextDecoder();
  let result = "";
  for (const chunk of text) {
    result +=
      typeof chunk === "string"
        ? decoder.decode() + chunk
        : decoder.decode(chunk, { stream: true });
  }
  return result + decoder.decode();
}

export function verificationCommandContainsSecrets(
  command: string,
  secrets: readonly string[],
): boolean {
  return credentialSpans(command, secretSpellings(secrets)).length > 0;
}

/** Sanitize before truncating, including matches split across chunks or adjacent output fields. */
export function sanitizeVerificationOutput(input: {
  readonly stdout: OutputText;
  readonly stderr: OutputText;
  readonly diagnostic: OutputText;
  readonly secrets: readonly string[];
  readonly stdoutTruncated?: boolean;
  readonly stderrTruncated?: boolean;
  readonly maxBytes?: number;
}): {
  readonly stdout: string;
  readonly stderr: string;
  readonly diagnostic: string;
  readonly truncated: boolean;
} {
  const requested = input.maxBytes ?? VERIFICATION_MAX_OUTPUT_BYTES;
  if (!Number.isSafeInteger(requested) || requested < 1)
    throw new RangeError("Invalid verification output byte limit.");
  let remaining = Math.min(requested, VERIFICATION_MAX_OUTPUT_BYTES);
  const fields = [decode(input.stdout), decode(input.stderr), decode(input.diagnostic)];
  const text = fields.join("");
  const secrets = secretSpellings(input.secrets);
  const spans = credentialSpans(text, secrets);
  const upstreamTruncated = [input.stdoutTruncated ?? false, input.stderrTruncated ?? false, false];
  let offset = 0;
  for (const [index, field] of fields.entries()) {
    if (upstreamTruncated[index]) {
      // Buffer decoding replaces an incomplete UTF-8 suffix with U+FFFD; it is part of the cut.
      const retained = field.replace(/\uFFFD+$/u, "");
      for (const secret of secrets) {
        // ProcessRunner can cut through a secret before this helper sees it. Hide the retained prefix too.
        for (let length = Math.min(secret.length, retained.length); length > 0; length--) {
          if (retained.endsWith(secret.slice(0, length))) {
            spans.push({ start: offset + retained.length - length, end: offset + field.length });
            break;
          }
        }
      }
    }
    offset += field.length;
  }
  spans.sort((left, right) => left.start - right.start || right.end - left.end);
  const merged: Span[] = [];
  for (const span of spans) {
    const previous = merged.at(-1);
    if (previous && span.start <= previous.end) previous.end = Math.max(previous.end, span.end);
    else merged.push({ ...span });
  }
  const marker = ["[REDACTED]", "[hidden]", ""].find(
    (value) => !secrets.some((secret) => value.includes(secret)),
  )!;
  let truncated = upstreamTruncated.some(Boolean);
  offset = 0;
  const sanitized = fields.map((field) => {
    const start = offset;
    const end = start + field.length;
    offset = end;
    let cursor = start;
    const parts: string[] = [];
    for (const span of merged) {
      if (span.end <= start) continue;
      if (span.start >= end) break;
      parts.push(text.slice(cursor, Math.max(cursor, span.start)));
      if (span.start >= start) parts.push(marker);
      cursor = Math.min(end, span.end);
    }
    parts.push(text.slice(cursor, end));
    const bytes = new TextEncoder().encode(parts.join(""));
    let length = Math.min(remaining, bytes.byteLength);
    if (length < bytes.byteLength) {
      truncated = true;
      // A UTF-8 continuation at the cut means the preceding code point did not fit.
      while (length > 0 && (bytes[length]! & 0xc0) === 0x80) length--;
    }
    remaining -= length;
    return new TextDecoder().decode(bytes.subarray(0, length));
  });
  return { stdout: sanitized[0]!, stderr: sanitized[1]!, diagnostic: sanitized[2]!, truncated };
}
