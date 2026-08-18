import { promises as fs } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
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

function rootToLeaf(directory: string): string[] {
  const { root } = path.parse(directory);
  const directories = [root];
  let current = root;
  for (const segment of path.relative(root, directory).split(path.sep)) {
    if (segment.length > 0) {
      current = path.join(current, segment);
      directories.push(current);
    }
  }
  return directories;
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
    const first = record({
      runtimeSessionId: "runtime-1",
      updatedAt: "2026-08-12T00:00:00.000Z",
    });
    const second = record({
      runtimeSessionId: "runtime-2",
      updatedAt: "2026-08-12T01:00:00.000Z",
    });

    await store.put(first);
    await store.put(second);

    await expect(store.get(first.linearSessionId)).resolves.toEqual(second);

    // Confirm the upsert didn't leave a duplicate entry behind.
    const raw = JSON.parse(await fs.readFile(storePath, "utf8")) as Record<
      string,
      unknown
    >;
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

  it("fails closed on corrupt persisted content instead of replacing it", async () => {
    const storePath = path.join(tmpDir, "sessions.json");
    const corrupt = "{not valid json!!!";
    await fs.writeFile(storePath, corrupt, "utf8");
    const store = new JsonSessionStore(storePath);

    await expect(store.get("anything")).rejects.toThrow(
      "Invalid session store JSON",
    );
    await expect(store.put(record())).rejects.toThrow(
      "Invalid session store JSON",
    );
    await expect(fs.readFile(storePath, "utf8")).resolves.toBe(corrupt);
  });

  it.each(["EIO", "EACCES"])(
    "propagates a %s read failure instead of treating the store as empty",
    async (code) => {
      const storePath = path.join(tmpDir, "sessions.json");
      const failure = Object.assign(new Error("synthetic session read failure"), {
        code,
      });
      const readSpy = vi.spyOn(fs, "readFile").mockRejectedValueOnce(failure);

      try {
        await expect(
          new JsonSessionStore(storePath).get("linear-1"),
        ).rejects.toBe(failure);
      } finally {
        readSpy.mockRestore();
      }
    },
  );

  it.each([
    ["an array", []],
    ["a scalar", "sessions"],
    [
      "a mismatched map key",
      { "linear-2": record({ linearSessionId: "linear-1" }) },
    ],
    [
      "a missing required field",
      {
        "linear-1": {
          linearSessionId: "linear-1",
          runtimeSessionId: "runtime-1",
          updatedAt: "2026-08-12T00:00:00.000Z",
        },
      },
    ],
    [
      "an unexpected field",
      { "linear-1": { ...record(), unexpected: true } },
    ],
    [
      "a noncanonical timestamp",
      { "linear-1": record({ updatedAt: "August 12, 2026" }) },
    ],
    [
      "an empty required identifier",
      { "": record({ linearSessionId: "" }) },
    ],
    [
      "a non-string optional issue identifier",
      { "linear-1": { ...record(), issueIdentifier: null } },
    ],
  ])("rejects persisted state containing %s", async (_label, invalidState) => {
    const storePath = path.join(tmpDir, "sessions.json");
    await fs.writeFile(storePath, JSON.stringify(invalidState), "utf8");

    await expect(
      new JsonSessionStore(storePath).get("linear-1"),
    ).rejects.toThrow("Invalid session store structure");
  });

  it("syncs the full directory chain before a secured temp file and atomic rename", async () => {
    const directory = path.join(tmpDir, "sessions", "nested");
    const storePath = path.join(directory, "sessions.json");
    const directoryPrefixes = rootToLeaf(directory);
    const directorySet = new Set(directoryPrefixes);
    const operations: string[] = [];
    const originalOpen = fs.open.bind(fs);
    const originalRename = fs.rename.bind(fs);
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const openedPath = String(args[0]);
      if (openedPath.endsWith(".tmp")) {
        operations.push(
          `temp-open:${String(args[1])}:${Number(args[2]).toString(8)}`,
        );
      }
      const handle = await originalOpen(...args);
      if (openedPath.endsWith(".tmp")) {
        const originalWriteFile = handle.writeFile.bind(handle);
        const originalChmod = handle.chmod.bind(handle);
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "writeFile").mockImplementation(async (...args) => {
          operations.push("temp-write");
          await originalWriteFile(...args);
        });
        vi.spyOn(handle, "chmod").mockImplementation(async (mode) => {
          operations.push(`temp-chmod:${mode.toString(8)}`);
          await originalChmod(mode);
        });
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          operations.push("temp-sync");
          await originalSync();
        });
      } else if (directorySet.has(openedPath)) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          operations.push(`directory-sync:${openedPath}`);
          await originalSync();
        });
      }
      return handle;
    });
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(
      async (from, to) => {
        await originalRename(from, to);
        operations.push("rename");
      },
    );

    try {
      await new JsonSessionStore(storePath).put(record());
      expect(operations).toEqual([
        ...directoryPrefixes.map((entry) => `directory-sync:${entry}`),
        "temp-open:wx:600",
        "temp-write",
        "temp-chmod:600",
        "temp-sync",
        "rename",
        `directory-sync:${directory}`,
      ]);
      expect((await fs.stat(storePath)).mode & 0o777).toBe(0o600);
    } finally {
      openSpy.mockRestore();
      renameSpy.mockRestore();
    }
  });

  it("caches a successful directory chain sync per store and repeats it for a fresh store", async () => {
    const directory = path.join(tmpDir, "sessions");
    const storePath = path.join(directory, "sessions.json");
    const directoryPrefixes = rootToLeaf(directory);
    const directorySet = new Set(directoryPrefixes);
    const syncs: string[] = [];
    const originalOpen = fs.open.bind(fs);
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      const openedPath = String(args[0]);
      if (directorySet.has(openedPath)) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          syncs.push(openedPath);
          await originalSync();
        });
      }
      return handle;
    });

    try {
      const store = new JsonSessionStore(storePath);
      await store.put(record());
      expect(syncs).toEqual([...directoryPrefixes, directory]);

      syncs.length = 0;
      await store.put(
        record({
          runtimeSessionId: "runtime-2",
          updatedAt: "2026-08-12T01:00:00.000Z",
        }),
      );
      expect(syncs).toEqual([directory]);

      syncs.length = 0;
      await new JsonSessionStore(storePath).put(
        record({
          runtimeSessionId: "runtime-3",
          updatedAt: "2026-08-12T02:00:00.000Z",
        }),
      );
      expect(syncs).toEqual([...directoryPrefixes, directory]);
    } finally {
      openSpy.mockRestore();
    }
  });

  it("retries the full directory chain after an ancestor sync failure", async () => {
    const directory = path.join(tmpDir, "sessions");
    const storePath = path.join(directory, "sessions.json");
    const failedDirectory = path.dirname(tmpDir);
    const failure = Object.assign(new Error("synthetic ancestor sync failure"), {
      code: "EIO",
    });
    let failedDirectorySyncs = 0;
    const originalOpen = fs.open.bind(fs);
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]) === failedDirectory) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          failedDirectorySyncs += 1;
          if (failedDirectorySyncs === 1) {
            throw failure;
          }
          await originalSync();
        });
      }
      return handle;
    });

    try {
      const store = new JsonSessionStore(storePath);
      await expect(store.put(record())).rejects.toBe(failure);
      await store.put(record());
      await new JsonSessionStore(storePath).put(
        record({
          runtimeSessionId: "runtime-2",
          updatedAt: "2026-08-12T01:00:00.000Z",
        }),
      );
      expect(failedDirectorySyncs).toBe(3);
    } finally {
      openSpy.mockRestore();
    }
  });

  it("rejects a temp-file fsync failure before rename and removes the temp file", async () => {
    const storePath = path.join(tmpDir, "sessions.json");
    const failure = Object.assign(new Error("synthetic temp fsync failure"), {
      code: "EIO",
    });
    const originalOpen = fs.open.bind(fs);
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]).endsWith(".tmp")) {
        vi.spyOn(handle, "sync").mockRejectedValueOnce(failure);
      }
      return handle;
    });
    const renameSpy = vi.spyOn(fs, "rename");

    try {
      await expect(new JsonSessionStore(storePath).put(record())).rejects.toBe(
        failure,
      );
      expect(renameSpy).not.toHaveBeenCalled();
      await expect(fs.readdir(tmpDir)).resolves.toEqual([]);
    } finally {
      openSpy.mockRestore();
      renameSpy.mockRestore();
    }
  });

  it("rejects a rename failure after temp fsync and removes the temp file", async () => {
    const storePath = path.join(tmpDir, "sessions.json");
    const failure = Object.assign(new Error("synthetic rename failure"), {
      code: "EIO",
    });
    const renameSpy = vi.spyOn(fs, "rename").mockRejectedValueOnce(failure);

    try {
      await expect(new JsonSessionStore(storePath).put(record())).rejects.toBe(
        failure,
      );
      await expect(fs.readdir(tmpDir)).resolves.toEqual([]);
    } finally {
      renameSpy.mockRestore();
    }
  });

  it("rejects a containing-directory sync failure after rename and preserves reload continuity", async () => {
    const storePath = path.join(tmpDir, "sessions.json");
    const rec = record();
    const failure = Object.assign(new Error("synthetic directory sync failure"), {
      code: "EIO",
    });
    const operations: string[] = [];
    let directorySyncs = 0;
    const originalOpen = fs.open.bind(fs);
    const originalRename = fs.rename.bind(fs);
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      const openedPath = String(args[0]);
      if (openedPath.endsWith(".tmp")) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          operations.push("temp-sync");
          await originalSync();
        });
      } else if (openedPath === tmpDir) {
        const originalSync = handle.sync.bind(handle);
        vi.spyOn(handle, "sync").mockImplementation(async () => {
          directorySyncs += 1;
          if (directorySyncs === 2) {
            operations.push("directory-sync");
            throw failure;
          }
          await originalSync();
        });
      }
      return handle;
    });
    const renameSpy = vi.spyOn(fs, "rename").mockImplementation(
      async (from, to) => {
        await originalRename(from, to);
        operations.push("rename");
      },
    );

    try {
      await expect(new JsonSessionStore(storePath).put(rec)).rejects.toBe(
        failure,
      );
      expect(operations).toEqual(["temp-sync", "rename", "directory-sync"]);
      await expect(
        new JsonSessionStore(storePath).get(rec.linearSessionId),
      ).resolves.toEqual(rec);
      await expect(fs.readdir(tmpDir)).resolves.toEqual(["sessions.json"]);
    } finally {
      openSpy.mockRestore();
      renameSpy.mockRestore();
    }
  });

  it("preserves the primary write error when temp cleanup also fails", async () => {
    const storePath = path.join(tmpDir, "sessions.json");
    const primary = Object.assign(new Error("synthetic temp fsync failure"), {
      code: "EIO",
    });
    const cleanup = Object.assign(new Error("synthetic temp cleanup failure"), {
      code: "EACCES",
    });
    const originalOpen = fs.open.bind(fs);
    const originalUnlink = fs.unlink.bind(fs);
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (String(args[0]).endsWith(".tmp")) {
        vi.spyOn(handle, "sync").mockRejectedValueOnce(primary);
      }
      return handle;
    });
    const unlinkSpy = vi.spyOn(fs, "unlink").mockImplementation(
      async (target) => {
        if (String(target).endsWith(".tmp")) {
          throw cleanup;
        }
        await originalUnlink(target);
      },
    );

    try {
      await expect(new JsonSessionStore(storePath).put(record())).rejects.toBe(
        primary,
      );
      expect(
        (await fs.readdir(tmpDir)).some((entry) => entry.endsWith(".tmp")),
      ).toBe(true);
    } finally {
      openSpy.mockRestore();
      unlinkSpy.mockRestore();
    }
  });

  it("does not unlink an unowned temp path when exclusive open fails", async () => {
    const storePath = path.join(tmpDir, "sessions.json");
    const failure = Object.assign(new Error("synthetic temp collision"), {
      code: "EEXIST",
    });
    const originalOpen = fs.open.bind(fs);
    const openSpy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      if (String(args[0]).endsWith(".tmp")) {
        throw failure;
      }
      return await originalOpen(...args);
    });
    const unlinkSpy = vi.spyOn(fs, "unlink");

    try {
      await expect(new JsonSessionStore(storePath).put(record())).rejects.toBe(
        failure,
      );
      expect(unlinkSpy).not.toHaveBeenCalled();
    } finally {
      openSpy.mockRestore();
      unlinkSpy.mockRestore();
    }
  });
});
