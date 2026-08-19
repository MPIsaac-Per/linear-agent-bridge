import { createHmac, timingSafeEqual } from "node:crypto";

/** Reject deliveries whose `webhookTimestamp` is further than this from now. */
const STALE_THRESHOLD_MS = 60_000;

/**
 * Verify a Linear webhook delivery.
 *
 * Per Linear's docs (linear.app/developers/webhooks):
 * - The `linear-signature` header holds a hex-encoded HMAC-SHA256 signature
 *   of the raw request body, keyed with the webhook's signing secret. Use
 *   the raw body, not a re-stringified parse of it, or the signature won't
 *   match.
 * - Replay protection: the JSON body carries a `webhookTimestamp` field
 *   (Unix time in milliseconds). Reject deliveries not within 60s of now.
 */
export function verifyWebhook(
  rawBody: Buffer,
  signatureHeader: string | undefined,
  secret: string,
  now: number = Date.now(),
): boolean {
  if (!verifyWebhookSignature(rawBody, signatureHeader, secret)) {
    return false;
  }

  let payload: unknown;
  try {
    payload = JSON.parse(rawBody.toString("utf8"));
  } catch {
    return false;
  }

  return hasFreshWebhookTimestamp(payload, now);
}

/** Verify only the HMAC so callers can emit a static parse diagnostic next. */
export function verifyWebhookSignature(
  rawBody: Buffer,
  signatureHeader: string | undefined,
  secret: string,
): boolean {
  if (signatureHeader === undefined) {
    return false;
  }
  const expectedHex = createHmac("sha256", secret).update(rawBody).digest("hex");
  const expected = Buffer.from(expectedHex, "utf8");
  const actual = Buffer.from(signatureHeader, "utf8");
  return expected.length === actual.length && timingSafeEqual(expected, actual);
}

/** Validate the parsed replay-protection timestamp without inspecting content. */
export function hasFreshWebhookTimestamp(
  payload: unknown,
  now: number = Date.now(),
): boolean {
  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
    return false;
  }
  const webhookTimestamp = (payload as Record<string, unknown>).webhookTimestamp;
  return (
    typeof webhookTimestamp === "number" &&
    Math.abs(now - webhookTimestamp) <= STALE_THRESHOLD_MS
  );
}
