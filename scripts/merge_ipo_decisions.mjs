#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) continue;
    const keyValue = token.slice(2).split("=", 2);
    const key = keyValue[0];
    const inlineValue = keyValue[1];
    if (inlineValue !== undefined) {
      out[key] = inlineValue;
      continue;
    }
    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      out[key] = next;
      i += 1;
    } else {
      out[key] = true;
    }
  }
  return out;
}

function normalizeSpace(value) {
  return String(value ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function decisionKeyFromDecision(decision) {
  return String(
    decision?.ids?.document_id
      || decision?.search_hit?.ids?.document_id
      || decision?.search_hit?.decision_url
      || decision?.source_url
      || ""
  );
}

async function loadArrayJson(filePath) {
  const text = await fs.readFile(filePath, "utf8");
  const parsed = JSON.parse(text);
  if (!Array.isArray(parsed)) {
    throw new Error(`JSON file is not an array: ${filePath}`);
  }
  return parsed;
}

function mergeDecisions(base, incremental) {
  const deduped = new Map();
  let duplicates = 0;

  for (const decision of [...base, ...incremental]) {
    const key = decisionKeyFromDecision(decision);
    if (!key) continue;
    if (deduped.has(key)) {
      duplicates += 1;
      continue;
    }
    deduped.set(key, decision);
  }

  return {
    merged: [...deduped.values()],
    duplicates
  };
}

function slugTimestamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const basePath = normalizeSpace(args.base);
  const incrementalPath = normalizeSpace(args.incremental);
  const outputPath = normalizeSpace(args.output);
  const createBackup = args["no-backup"] ? false : true;

  if (!basePath || !incrementalPath || !outputPath) {
    throw new Error("Usage: node scripts/merge_ipo_decisions.mjs --base <path> --incremental <path> --output <path> [--no-backup]");
  }

  const resolvedBase = path.resolve(basePath);
  const resolvedIncremental = path.resolve(incrementalPath);
  const resolvedOutput = path.resolve(outputPath);

  const [base, incremental] = await Promise.all([
    loadArrayJson(resolvedBase),
    loadArrayJson(resolvedIncremental)
  ]);

  if (resolvedBase === resolvedOutput && createBackup) {
    const backupPath = `${resolvedBase}.backup-${slugTimestamp()}.json`;
    await fs.copyFile(resolvedBase, backupPath);
    process.stdout.write(`Backup written: ${backupPath}\n`);
  }

  const { merged, duplicates } = mergeDecisions(base, incremental);
  await fs.mkdir(path.dirname(resolvedOutput), { recursive: true });
  await fs.writeFile(resolvedOutput, `${JSON.stringify(merged, null, 2)}\n`, "utf8");

  process.stdout.write(`Base records: ${base.length}\n`);
  process.stdout.write(`Incremental records: ${incremental.length}\n`);
  process.stdout.write(`Duplicates skipped: ${duplicates}\n`);
  process.stdout.write(`Merged records: ${merged.length}\n`);
  process.stdout.write(`Output: ${resolvedOutput}\n`);
}

main().catch((error) => {
  process.stderr.write(`Fatal error: ${error?.message || error}\n`);
  process.exit(1);
});

