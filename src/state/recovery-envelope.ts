import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
} from "node:crypto";

const ENVELOPE_VERSION = 1;
const KEY_BYTES = 32;
const NONCE_BYTES = 12;
const TAG_BYTES = 16;
const MAX_PROMPT_BYTES = 256 * 1024;
const MAX_SIGNAL_LENGTH = 128;
const MAX_ISSUE_IDENTIFIER_LENGTH = 256;
const MAX_CIPHERTEXT_BYTES = MAX_PROMPT_BYTES + 2 * 1024;
const KEY_ID_BYTES = 16;

export interface IngressRecoveryIdentity {
  webhookId: string;
  executionId: string;
  linearSessionId: string;
  action: "created" | "prompted";
}

interface RecoveryPayloadBase {
  prompt: string;
  occurredAt: string;
}

export interface CreatedIngressRecoveryPayload extends RecoveryPayloadBase {
  action: "created";
  issueIdentifier?: string | undefined;
}

export interface PromptedIngressRecoveryPayload extends RecoveryPayloadBase {
  action: "prompted";
  signal?: string | undefined;
  stop: boolean;
}

export type IngressRecoveryPayload =
  | CreatedIngressRecoveryPayload
  | PromptedIngressRecoveryPayload;

export interface SealedIngressRecoveryEnvelope {
  v: 1;
  keyId: string;
  nonce: string;
  ciphertext: string;
  tag: string;
}

interface RecoveryKey {
  id: string;
  bytes: Buffer;
}

export interface IngressRecoveryKeyring {
  primary: RecoveryKey;
  keysById: ReadonlyMap<string, RecoveryKey>;
}

export class IngressRecoveryEnvelopeError extends Error {
  constructor() {
    super("Ingress recovery envelope is unavailable");
    this.name = "IngressRecoveryEnvelopeError";
  }
}

export function parseCanonicalRecoveryKey(value: string): Buffer | undefined {
  if (!/^[A-Za-z0-9_-]{43}$/.test(value)) {
    return undefined;
  }
  const decoded = Buffer.from(value, "base64url");
  return decoded.length === KEY_BYTES && decoded.toString("base64url") === value
    ? decoded
    : undefined;
}

export function createIngressRecoveryKeyring(
  primaryValue: string,
  previousValues: readonly string[] = [],
): IngressRecoveryKeyring {
  const values = [primaryValue, ...previousValues];
  const keys = values.map((value) => {
    const bytes = parseCanonicalRecoveryKey(value);
    if (bytes === undefined) {
      throw new Error("Ingress recovery key must be canonical 32-byte base64url");
    }
    return {
      id: createHash("sha256")
        .update(bytes)
        .digest()
        .subarray(0, KEY_ID_BYTES)
        .toString("base64url"),
      bytes,
    };
  });
  const keysById = new Map<string, RecoveryKey>();
  for (const key of keys) {
    if (keysById.has(key.id)) {
      throw new Error("Ingress recovery keys must be unique");
    }
    keysById.set(key.id, key);
  }
  return { primary: keys[0]!, keysById };
}

export function sealIngressRecoveryPayload(
  keyring: IngressRecoveryKeyring,
  identity: IngressRecoveryIdentity,
  sequence: number,
  payload: IngressRecoveryPayload,
): SealedIngressRecoveryEnvelope {
  validateSequence(sequence);
  validateRecoveryPayload(identity, payload);
  const plaintext = Buffer.from(JSON.stringify(payload), "utf8");
  if (plaintext.length > MAX_CIPHERTEXT_BYTES) {
    throw new IngressRecoveryEnvelopeError();
  }
  const nonce = randomBytes(NONCE_BYTES);
  const cipher = createCipheriv("aes-256-gcm", keyring.primary.bytes, nonce, {
    authTagLength: TAG_BYTES,
  });
  cipher.setAAD(canonicalAad(identity, sequence, keyring.primary.id), {
    plaintextLength: plaintext.length,
  });
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  return {
    v: ENVELOPE_VERSION,
    keyId: keyring.primary.id,
    nonce: nonce.toString("base64url"),
    ciphertext: ciphertext.toString("base64url"),
    tag: cipher.getAuthTag().toString("base64url"),
  };
}

export function openIngressRecoveryPayload(
  keyring: IngressRecoveryKeyring,
  identity: IngressRecoveryIdentity,
  sequence: number,
  value: unknown,
): IngressRecoveryPayload {
  validateSequence(sequence);
  const envelope = parseEnvelope(value);
  const key = keyring.keysById.get(envelope.keyId);
  if (key === undefined) {
    throw new IngressRecoveryEnvelopeError();
  }
  try {
    const nonce = decodeCanonicalBase64url(envelope.nonce, NONCE_BYTES);
    const ciphertext = decodeCanonicalBase64url(
      envelope.ciphertext,
      undefined,
      MAX_CIPHERTEXT_BYTES,
    );
    const tag = decodeCanonicalBase64url(envelope.tag, TAG_BYTES);
    const decipher = createDecipheriv("aes-256-gcm", key.bytes, nonce, {
      authTagLength: TAG_BYTES,
    });
    decipher.setAAD(canonicalAad(identity, sequence, envelope.keyId), {
      plaintextLength: ciphertext.length,
    });
    decipher.setAuthTag(tag);
    const plaintext = Buffer.concat([
      decipher.update(ciphertext),
      decipher.final(),
    ]);
    if (plaintext.length > MAX_CIPHERTEXT_BYTES) {
      throw new IngressRecoveryEnvelopeError();
    }
    const payload = JSON.parse(plaintext.toString("utf8")) as unknown;
    validateRecoveryPayload(identity, payload);
    return payload;
  } catch {
    throw new IngressRecoveryEnvelopeError();
  }
}

function canonicalAad(
  identity: IngressRecoveryIdentity,
  sequence: number,
  keyId: string,
): Buffer {
  return Buffer.from(
    JSON.stringify([
      ENVELOPE_VERSION,
      keyId,
      identity.webhookId,
      identity.executionId,
      identity.linearSessionId,
      identity.action,
      sequence,
    ]),
    "utf8",
  );
}

function parseEnvelope(value: unknown): SealedIngressRecoveryEnvelope {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new IngressRecoveryEnvelopeError();
  }
  const record = value as Record<string, unknown>;
  if (
    Object.keys(record).length !== 5 ||
    record.v !== ENVELOPE_VERSION ||
    typeof record.keyId !== "string" ||
    !/^[A-Za-z0-9_-]{22}$/.test(record.keyId) ||
    typeof record.nonce !== "string" ||
    typeof record.ciphertext !== "string" ||
    typeof record.tag !== "string"
  ) {
    throw new IngressRecoveryEnvelopeError();
  }
  return record as unknown as SealedIngressRecoveryEnvelope;
}

function decodeCanonicalBase64url(
  value: string,
  exactLength?: number,
  maximumLength?: number,
): Buffer {
  const exactEncodedLength =
    exactLength === undefined ? undefined : Math.ceil((exactLength * 4) / 3);
  const maximumEncodedLength =
    maximumLength === undefined
      ? undefined
      : Math.ceil((maximumLength * 4) / 3);
  if (
    !/^[A-Za-z0-9_-]+$/.test(value) ||
    (exactEncodedLength !== undefined && value.length !== exactEncodedLength) ||
    (maximumEncodedLength !== undefined && value.length > maximumEncodedLength)
  ) {
    throw new IngressRecoveryEnvelopeError();
  }
  const decoded = Buffer.from(value, "base64url");
  if (
    decoded.toString("base64url") !== value ||
    (exactLength !== undefined && decoded.length !== exactLength) ||
    (maximumLength !== undefined && decoded.length > maximumLength)
  ) {
    throw new IngressRecoveryEnvelopeError();
  }
  return decoded;
}

function validateSequence(value: number): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new IngressRecoveryEnvelopeError();
  }
}

function validateRecoveryPayload(
  identity: IngressRecoveryIdentity,
  value: unknown,
): asserts value is IngressRecoveryPayload {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new IngressRecoveryEnvelopeError();
  }
  const record = value as Record<string, unknown>;
  if (
    record.action !== identity.action ||
    typeof record.prompt !== "string" ||
    Buffer.byteLength(record.prompt, "utf8") > MAX_PROMPT_BYTES ||
    typeof record.occurredAt !== "string"
  ) {
    throw new IngressRecoveryEnvelopeError();
  }
  validateOccurredAt(record.occurredAt);
  if (identity.action === "created") {
    if (
      !hasExactKeys(record, ["action", "prompt", "occurredAt"], [
        "issueIdentifier",
      ]) ||
      (record.issueIdentifier !== undefined &&
        (typeof record.issueIdentifier !== "string" ||
          record.issueIdentifier.length === 0 ||
          record.issueIdentifier.length > MAX_ISSUE_IDENTIFIER_LENGTH))
    ) {
      throw new IngressRecoveryEnvelopeError();
    }
    return;
  }
  if (
    !hasExactKeys(record, ["action", "prompt", "occurredAt", "stop"], [
      "signal",
    ]) ||
    typeof record.stop !== "boolean" ||
    (record.signal !== undefined &&
      (typeof record.signal !== "string" ||
        record.signal.length === 0 ||
        record.signal.length > MAX_SIGNAL_LENGTH))
  ) {
    throw new IngressRecoveryEnvelopeError();
  }
  const signal = typeof record.signal === "string" ? record.signal : undefined;
  if (record.stop !== isStopPrompt(record.prompt, signal)) {
    throw new IngressRecoveryEnvelopeError();
  }
}

export function isStopPrompt(prompt: string, signal?: string): boolean {
  return signal === "stop" || /^stop[.!]?$/i.test(prompt.trim());
}

function validateOccurredAt(value: string): void {
  if (!isCanonicalRecoveryTimestamp(value)) {
    throw new IngressRecoveryEnvelopeError();
  }
}

export function isCanonicalRecoveryTimestamp(
  value: unknown,
): value is string {
  if (typeof value !== "string" || value.length !== 24) {
    return false;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) && new Date(parsed).toISOString() === value;
}

function hasExactKeys(
  record: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
): boolean {
  const allowed = new Set([...required, ...optional]);
  return (
    required.every((key) => Object.hasOwn(record, key)) &&
    Object.keys(record).every((key) => allowed.has(key))
  );
}
