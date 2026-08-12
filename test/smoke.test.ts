import { describe, expect, it } from "vitest";
import { loadConfig } from "../src/config.js";

describe("scaffold", () => {
  it("loads a valid config end-to-end", () => {
    const config = loadConfig({
      LINEAR_CLIENT_ID: "id",
      LINEAR_CLIENT_SECRET: "secret",
      LINEAR_WEBHOOK_SECRET: "whsec",
      LINEAR_ACCESS_TOKEN: "token",
    });

    expect(config.linearClientId).toBe("id");
    expect(config.port).toBe(3979);
  });
});
