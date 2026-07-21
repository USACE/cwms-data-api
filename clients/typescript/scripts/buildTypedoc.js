const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const rootDir = path.resolve(__dirname, "..");
const cwmsjsDir = path.join(rootDir, "cwmsjs");
const docsDir = path.join(cwmsjsDir, "docs");
const tempDocsDir = path.join(cwmsjsDir, "docs-typedoc");
const packageJson = JSON.parse(
  fs.readFileSync(path.join(cwmsjsDir, "package.json"), "utf8"),
);

removePath(tempDocsDir, { strict: true });

const result = spawnSync(
  `npx typedoc src/index.ts --name "cwmsjs v${packageJson.version}" --out docs-typedoc`,
  {
    cwd: cwmsjsDir,
    stdio: "inherit",
    shell: true,
  },
);

if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

fs.mkdirSync(docsDir, { recursive: true });

for (const entry of fs.readdirSync(tempDocsDir, { withFileTypes: true })) {
  const sourcePath = path.join(tempDocsDir, entry.name);
  const destinationPath = path.join(docsDir, entry.name);

  removePath(destinationPath);
  copyPath(sourcePath, destinationPath);
}

removePath(tempDocsDir, { strict: true });

function removePath(targetPath, { strict = false } = {}) {
  if (!fs.existsSync(targetPath)) {
    return;
  }

  const targetStats = fs.lstatSync(targetPath);

  try {
    fs.rmSync(targetPath, { recursive: true, force: true });
  } catch (error) {
    if (process.platform !== "win32") {
      throw error;
    }

    const cleanupCommand = targetStats.isDirectory()
      ? `if exist "${targetPath}" rmdir /s /q "${targetPath}"`
      : `if exist "${targetPath}" del /f /q "${targetPath}"`;
    const cleanup = spawnSync(
      "cmd.exe",
      ["/d", "/s", "/c", cleanupCommand],
      {
        stdio: "inherit",
      },
    );

    if (cleanup.status !== 0 && fs.existsSync(targetPath)) {
      throw error;
    }
  }

  if (fs.existsSync(targetPath)) {
    if (strict) {
      throw new Error(`Unable to remove ${targetPath}`);
    }
    console.warn(`Skipping cleanup for locked path: ${targetPath}`);
  }
}

function copyPath(sourcePath, destinationPath) {
  const sourceStats = fs.statSync(sourcePath);

  if (sourceStats.isDirectory()) {
    fs.mkdirSync(destinationPath, { recursive: true });

    for (const entry of fs.readdirSync(sourcePath, { withFileTypes: true })) {
      copyPath(
        path.join(sourcePath, entry.name),
        path.join(destinationPath, entry.name),
      );
    }
    return;
  }

  try {
    fs.copyFileSync(sourcePath, destinationPath);
  } catch (error) {
    if (error && error.code === "EPERM") {
      console.warn(`Skipping locked file: ${destinationPath}`);
      return;
    }
    throw error;
  }
}
