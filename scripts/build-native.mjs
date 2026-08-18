import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { constants, promises as fs } from "node:fs";
import * as path from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";

const execFileAsync = promisify(execFile);
const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoDir = path.resolve(scriptDir, "..");
const sourcePath = path.join(repoDir, "native", "process_identity.c");
const outputDir = path.join(repoDir, "dist", "native");
const outputPath = path.join(outputDir, "process_identity");
const stampPath = `${outputPath}.sha256`;
const checkOnly = process.argv.slice(2).includes("--check");
const childEnvironment = Object.freeze({
  LC_ALL: "C",
  PATH: "/usr/bin:/bin",
});
const commonFlags = Object.freeze([
  "-std=c11",
  "-Wall",
  "-Wextra",
  "-Werror",
]);
const discoveryTimeoutMs = 10_000;
const compileTimeoutMs = 60_000;

async function findToolchain() {
  try {
    const [{ stdout: clangOutput }, { stdout: sdkOutput }] = await Promise.all([
      execFileAsync("/usr/bin/xcrun", ["--find", "clang"], {
        encoding: "utf8",
        env: childEnvironment,
        maxBuffer: 4 * 1024,
        timeout: discoveryTimeoutMs,
      }),
      execFileAsync("/usr/bin/xcrun", ["--sdk", "macosx", "--show-sdk-path"], {
        encoding: "utf8",
        env: childEnvironment,
        maxBuffer: 4 * 1024,
        timeout: discoveryTimeoutMs,
      }),
    ]);
    const clangPath = clangOutput.trim();
    const sdkPath = sdkOutput.trim();
    if (!path.isAbsolute(clangPath) || !path.isAbsolute(sdkPath)) {
      throw new Error("toolchain paths were not absolute");
    }
    return { clangPath, sdkPath };
  } catch {
    throw new Error(
      "Xcode Command Line Tools are required on macOS; run `xcode-select --install`, then retry.",
    );
  }
}

async function outputIsCurrent(fingerprint) {
  try {
    const [stamp] = await Promise.all([
      fs.readFile(stampPath, "utf8"),
      fs.access(outputPath, constants.X_OK),
    ]);
    return stamp.trim() === fingerprint;
  } catch {
    return false;
  }
}

async function main() {
  if (process.platform !== "darwin") {
    return;
  }

  const source = await fs.readFile(sourcePath);
  const fingerprint = createHash("sha256")
    .update(source)
    .update(
      JSON.stringify({
        version: 1,
        arch: process.arch,
        commonFlags,
        linkFlags: ["-lproc"],
      }),
    )
    .digest("hex");

  if (!checkOnly && (await outputIsCurrent(fingerprint))) {
    return;
  }

  const { clangPath, sdkPath } = await findToolchain();

  if (checkOnly) {
    await execFileAsync(
      clangPath,
      [...commonFlags, "-isysroot", sdkPath, "-fsyntax-only", sourcePath],
      {
        encoding: "utf8",
        env: childEnvironment,
        maxBuffer: 64 * 1024,
        timeout: compileTimeoutMs,
      },
    );
    return;
  }

  await fs.mkdir(outputDir, { recursive: true });
  const tempOutput = path.join(
    outputDir,
    `.process_identity.${process.pid}.tmp`,
  );
  const tempStamp = `${tempOutput}.sha256`;
  try {
    await execFileAsync(
      clangPath,
      [
        ...commonFlags,
        "-O2",
        "-isysroot",
        sdkPath,
        sourcePath,
        "-lproc",
        "-o",
        tempOutput,
      ],
      {
        encoding: "utf8",
        env: childEnvironment,
        maxBuffer: 64 * 1024,
        timeout: compileTimeoutMs,
      },
    );
    await fs.chmod(tempOutput, 0o755);
    await fs.writeFile(tempStamp, `${fingerprint}\n`, { mode: 0o600 });
    await fs.rename(tempOutput, outputPath);
    await fs.rename(tempStamp, stampPath);
  } finally {
    await Promise.all([
      fs.rm(tempOutput, { force: true }),
      fs.rm(tempStamp, { force: true }),
    ]);
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : "native build failed";
  console.error(`[native] ${message}`);
  process.exitCode = 1;
});
