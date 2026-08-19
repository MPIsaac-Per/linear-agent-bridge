import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import * as path from "node:path";

export interface SessionRecord {
  linearSessionId: string;
  runtimeSessionId: string;
  runtime: string;
  issueIdentifier?: string | undefined;
  updatedAt: string;
}

type SessionMap = Record<string, SessionRecord>;

/**
 * Maps Linear agent-session ids to runtime session ids so follow-up
 * prompts resume the same agent conversation. JSON file on disk;
 * writes are atomic (write temp, rename).
 */
export class JsonSessionStore {
  constructor(private readonly path: string) {}

  async get(linearSessionId: string): Promise<SessionRecord | undefined> {
    const sessions = await this.readAll();
    return sessions[linearSessionId];
  }

  async put(record: SessionRecord): Promise<void> {
    const sessions = await this.readAll();
    sessions[record.linearSessionId] = record;
    await this.writeAll(sessions);
  }

  async listSessionIds(): Promise<string[]> {
    return Object.keys(await this.readAll()).sort();
  }

  /** Missing file or unparseable content both read as an empty store. */
  private async readAll(): Promise<SessionMap> {
    let raw: string;
    try {
      raw = await fs.readFile(this.path, "utf8");
    } catch {
      return {};
    }

    try {
      const parsed: unknown = JSON.parse(raw);
      if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed as SessionMap;
      }
      return {};
    } catch {
      return {};
    }
  }

  /** Write temp file in the same dir, then rename — atomic on POSIX. */
  private async writeAll(sessions: SessionMap): Promise<void> {
    const dir = path.dirname(this.path);
    await fs.mkdir(dir, { recursive: true });

    const tmpPath = path.join(dir, `.${path.basename(this.path)}.${randomUUID()}.tmp`);
    await fs.writeFile(tmpPath, JSON.stringify(sessions, null, 2), "utf8");
    await fs.rename(tmpPath, this.path);
  }
}
