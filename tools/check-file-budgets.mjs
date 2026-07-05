#!/usr/bin/env bun

import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const configPath = path.join(rootDir, "tools", "file-size-budgets.json");
const config = JSON.parse(readFileSync(configPath, "utf8"));
const args = new Set(process.argv.slice(2));
const reportOnly = args.has("--report");

function fail(message) {
  console.error(message);
  process.exit(1);
}

const gitResult = spawnSync(
  "git",
  ["ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", ...config.roots],
  {
  cwd: rootDir,
  encoding: "utf8",
  },
);

if (gitResult.status !== 0) {
  fail(gitResult.stderr || "Unable to list tracked files.");
}

const trackedFiles = [...new Set(gitResult.stdout.split("\0").filter(Boolean))]
  .filter((filePath) => config.extensions.includes(path.extname(filePath)))
  .filter((filePath) => {
    const segments = filePath.split(path.sep);
    return !segments.some((segment) => config.ignoredSegments.includes(segment));
  })
  .filter((filePath) => existsSync(path.join(rootDir, filePath)));

const fileReports = trackedFiles.map((filePath) => {
  const absolutePath = path.join(rootDir, filePath);
  const source = readFileSync(absolutePath, "utf8");
  const loc = source.split(/\r?\n/).filter((line) => line.trim().length > 0).length;
  const baseline = config.allowlist[filePath];

  return {
    filePath,
    loc,
    baseline: typeof baseline === "number" ? baseline : null,
  };
});

fileReports.sort((left, right) => right.loc - left.loc || left.filePath.localeCompare(right.filePath));

const reviewFiles = fileReports.filter((entry) => entry.loc >= config.thresholds.review);
const oversizedNewFiles = fileReports.filter(
  (entry) => entry.loc > config.thresholds.freeze && entry.baseline === null,
);
const growthFailures = fileReports.filter(
  (entry) => entry.baseline !== null && entry.loc > entry.baseline,
);
const removableAllowlistEntries = fileReports.filter(
  (entry) => entry.baseline !== null && entry.loc <= config.thresholds.freeze,
);
const staleAllowlistEntries = Object.keys(config.allowlist).filter(
  (filePath) => !trackedFiles.includes(filePath),
);

function classify(entry) {
  if (entry.loc >= config.thresholds.hard) return "hard";
  if (entry.loc > config.thresholds.freeze) return "freeze";
  return "review";
}

console.log("File size report (non-empty LOC)");
console.log(
  `Thresholds: review>=${config.thresholds.review}, freeze>${config.thresholds.freeze}, hard>=${config.thresholds.hard}`,
);

if (reviewFiles.length === 0) {
  console.log("No tracked source files exceed the review threshold.");
} else {
  for (const entry of reviewFiles) {
    const level = classify(entry).padEnd(6, " ");
    const allowlistLabel =
      entry.baseline === null
        ? "new"
        : entry.loc === entry.baseline
          ? `allowlist=${entry.baseline}`
          : `allowlist=${entry.baseline} (shrunk)`;
    console.log(`${String(entry.loc).padStart(4, " ")}  ${level}  ${allowlistLabel.padEnd(22, " ")}  ${entry.filePath}`);
  }
}

if (staleAllowlistEntries.length > 0) {
  console.log("");
  console.log("Stale allowlist entries:");
  for (const filePath of staleAllowlistEntries) {
    console.log(`- ${filePath}`);
  }
}

if (removableAllowlistEntries.length > 0) {
  console.log("");
  console.log("Allowlist entries now at or below the freeze threshold:");
  for (const entry of removableAllowlistEntries) {
    console.log(`- ${entry.filePath} (${entry.loc} LOC, baseline ${entry.baseline})`);
  }
}

const failures = [];

if (oversizedNewFiles.length > 0) {
  failures.push(
    `New files exceed the shrink-only threshold (${config.thresholds.freeze} LOC):\n${oversizedNewFiles
      .map((entry) => `- ${entry.filePath} (${entry.loc} LOC)`)
      .join("\n")}`,
  );
}

if (growthFailures.length > 0) {
  failures.push(
    `Allowlisted files grew past their baseline:\n${growthFailures
      .map((entry) => `- ${entry.filePath} (${entry.loc} LOC, baseline ${entry.baseline})`)
      .join("\n")}`,
  );
}

if (failures.length > 0 && !reportOnly) {
  console.log("");
  console.error(failures.join("\n\n"));
  process.exit(1);
}

if (!reportOnly) {
  console.log("");
  console.log("File size guardrail passed.");
}
