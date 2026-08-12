import { promises as fs } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { JsonSessionStore, type SessionRecord } from "../src/sessions/store.js";

let tmpDir: string;

beforeEach(async () => {
  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "session-store-test-"));
});

afterEach(async () => {
  await fs.rm(tmpDir, { recursive: true, force: true });
});

function record(overrides: Partial<SessionRecord> = {}): SessionRecord {
  return {
    linearSessionId: "linear-1",
    runtimeSessionId: "runtime-1",
    runtime: "claude",
    issueIdentifier: "ENG-42",
    updatedAt: "2026-08-12T00:00:00.000Z",
    ...overrides,
  };
}

describe("JsonSessionStore", () => {
  it("returns undefined for a missing id", async () => {
    const store = new JsonSessionStore(path.join(tmpDir, "sessions.json"));
    await expect(store.get("nope")).resolves.toBeUndefined();
  });

  it("round-trips a put record via get", async () => {
    const store = new JsonSessionStore(path.join(tmpDir, "sessions.json"));
    const rec = record();

    await store.put(rec);

    await expect(store.get(rec.linearSessionId)).resolves.toEqual(rec);
  });

  it("overwrites the existing record on a second put for the same id", async () => {
    const storePath = path.join(tmpDir, "sessions.json");
    const store = new JsonSessionStore(storePath);
    const first = record({ runtimeSessionId: "runtime-1", updatedAt: "2026-08-12T00:00:00.000Z" });
    const second = record({ runtimeSessionId: "runtime-2", updatedAt: "2026-08-12T01:00:00.000Z" });

    await store.put(first);
    await store.put(second);

    await expect(store.get(first.linearSessionId)).resolves.toEqual(second);

    // Confirm the upsert didn't leave a duplicate entry behind.
    const raw = JSON.parse(await fs.readFile(storePath, "utf8")) as Record<string, unknown>;
    expect(Object.keys(raw)).toHaveLength(1);
  });

  it("persists across a fresh instance pointed at the same path", async () => {
    const storePath = path.join(tmpDir, "sessions.json");
    const writer = new JsonSessionStore(storePath);
    const rec = record();
    await writer.put(rec);

    const reader = new JsonSessionStore(storePath);
    await expect(reader.get(rec.linearSessionId)).resolves.toEqual(rec);
  });

  it("creates the parent directory on first write", async () => {
    const storePath = path.join(tmpDir, "nested", "deeper", "sessions.json");
    const store = new JsonSessionStore(storePath);
    const rec = record();

    await store.put(rec);

    const stat = await fs.stat(storePath);
    expect(stat.isFile()).toBe(true);
  });

  it("writes atomically: no leftover temp files, target file present after put", async () => {
    const storePath = path.join(tmpDir, "sessions.json");
    const store = new JsonSessionStore(storePath);

    await store.put(record());

    const entries = await fs.readdir(tmpDir);
    expect(entries).toEqual(["sessions.json"]);
  });

  it("treats a missing file as an empty store", async () => {
    const store = new JsonSessionStore(path.join(tmpDir, "does-not-exist.json"));
    await expect(store.get("anything")).resolves.toBeUndefined();
  });

  it("tolerates corrupt (non-JSON) file content, reading it as empty", async () => {
    const storePath = path.join(tmpDir, "sessions.json");
    await fs.writeFile(storePath, "{not valid json!!!", "utf8");
    const store = new JsonSessionStore(storePath);

    await expect(store.get("anything")).resolves.toBeUndefined();

    // And it should still be writable afterward, replacing the garbage.
    const rec = record();
    await store.put(rec);
    await expect(store.get(rec.linearSessionId)).resolves.toEqual(rec);
  });
});
