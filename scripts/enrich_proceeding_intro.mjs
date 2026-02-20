#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import os from "node:os";
import { execFile as execFileCallback } from "node:child_process";
import { promisify } from "node:util";

const execFile = promisify(execFileCallback);

const HELP_TEXT = `
Enrich scraped TK decisions with the procedural intro line ("po rozpoznaniu...").

Usage:
  node scripts/enrich_proceeding_intro.mjs [options]

Options:
  --input <path>             Input decisions JSON (default: output/playwright/tk-all.decisions.json)
  --output <path>            Output decisions JSON (default: input path)
  --summary <path>           Summary JSON path (default: <output>.proceeding-intro-summary.json)
  --concurrency <n>          Parallel pages (default: 6, max: 16)
  --timeout-ms <n>           Timeout per page step (default: 60000)
  --checkpoint-every <n>     Persist output every N processed records (default: 50)
  --resume                   If output exists, continue from output file
  --force-refresh            Recheck records even if already checked
  --transport <mode>         Extraction transport: curl|playwright (default: curl)
  --headed                   When transport=playwright, run browser in headed mode
  --help                     Show this help
`;

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) continue;
    const [key, inline] = token.slice(2).split("=", 2);
    if (inline !== undefined) {
      out[key] = inline;
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

function toPositiveInt(value, fallback, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, max);
}

function normalizeSpace(value) {
  return String(value ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

function decodeHtmlEntities(value) {
  return String(value ?? "")
    .replace(/&#160;|&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
}

function stripTags(value) {
  const html = String(value ?? "");
  return normalizeSpace(
    decodeHtmlEntities(
      html
        .replace(/<br\s*\/?>/gi, " ")
        .replace(/<[^>]+>/g, " ")
    )
  );
}

function extractDocumentScope(html, documentId) {
  const fullHtml = String(html ?? "");
  const docId = normalizeSpace(documentId);
  if (!docId) {
    return fullHtml;
  }
  const marker = `id="sprawaForm:tabView:dok_${docId}"`;
  const start = fullHtml.indexOf(marker);
  if (start < 0) {
    return fullHtml;
  }
  const nextMarker = fullHtml.indexOf('id="sprawaForm:tabView:dok_', start + marker.length);
  if (nextMarker < 0) {
    return fullHtml.slice(start);
  }
  return fullHtml.slice(start, nextMarker);
}

function extractProceedingIntroFromHtml(html, documentId) {
  const scope = extractDocumentScope(html, documentId);
  const headingMatch = scope.match(
    /<[^>]*class="[^"]*wyrok_zaskarzenie_tytul[^"]*"[^>]*>([\s\S]*?)<\/[^>]+>/i
  );
  if (headingMatch) {
    const text = stripTags(headingMatch[1]);
    if (text.length >= 12) {
      return text;
    }
  }

  const fallbackMatch = scope.match(
    /<[^>]*class="[^"]*wyrok_zaskarzenie[^"]*"[^>]*>([\s\S]*?)<\/[^>]+>/i
  );
  if (!fallbackMatch) {
    return null;
  }

  const fallbackText = stripTags(fallbackMatch[1]);
  if (!fallbackText) {
    return null;
  }

  const cutAtList = fallbackText.match(/^(.*?)(?:\s+(?:1\)|1\.|I\.|I\s))/);
  const candidate = normalizeSpace(cutAtList?.[1] || fallbackText).slice(0, 600);
  return candidate || null;
}

async function fetchHtmlViaCurl(sourceUrl, timeoutMs) {
  const maxAttempts = 3;
  let lastError = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const maxTimeSeconds = Math.max(5, Math.ceil(timeoutMs / 1000));
    const cookieJar = path.join(
      os.tmpdir(),
      `tk-enrich-cookies-${Date.now()}-${Math.random().toString(36).slice(2)}.txt`
    );
    const args = [
      "-sSL",
      "--max-time",
      String(maxTimeSeconds),
      "--connect-timeout",
      "15",
      "--cookie-jar",
      cookieJar,
      "--cookie",
      cookieJar,
      sourceUrl
    ];
    try {
      const { stdout } = await execFile("curl", args, { maxBuffer: 25 * 1024 * 1024 });
      return String(stdout ?? "");
    } catch (error) {
      lastError = error;
      if (attempt < maxAttempts) {
        await sleep(attempt * 500);
      }
    } finally {
      await fs.unlink(cookieJar).catch(() => {});
    }
  }
  throw lastError || new Error("Unknown curl extraction error.");
}

async function extractProceedingIntro(sourceUrl, documentId, timeoutMs) {
  const html = await fetchHtmlViaCurl(sourceUrl, timeoutMs);
  const docId = normalizeSpace(documentId);
  if (docId && !html.includes(`id="sprawaForm:tabView:dok_${docId}"`)) {
    throw new Error(`document_marker_not_found:${docId}`);
  }
  return normalizeSpace(extractProceedingIntroFromHtml(html, documentId)) || null;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    process.stdout.write(HELP_TEXT.trimStart());
    process.stdout.write("\n");
    return;
  }

  const inputPath = path.resolve(args.input || "output/playwright/tk-all.decisions.json");
  const outputPath = path.resolve(args.output || inputPath);
  const summaryPath = path.resolve(
    args.summary
      || `${outputPath.replace(/\.json$/i, "")}.proceeding-intro-summary.json`
  );
  const concurrency = toPositiveInt(args.concurrency, 6, 16);
  const timeoutMs = toPositiveInt(args["timeout-ms"], 60000);
  const checkpointEvery = toPositiveInt(args["checkpoint-every"], 50);
  const resume = Boolean(args.resume);
  const forceRefresh = Boolean(args["force-refresh"]);
  const transport = normalizeSpace(args.transport || "curl").toLowerCase();
  const headed = Boolean(args.headed);
  if (!["curl", "playwright"].includes(transport)) {
    throw new Error(`Unsupported transport: ${transport}`);
  }

  if (!(await fileExists(inputPath)) && !(resume && (await fileExists(outputPath)))) {
    throw new Error(`Input file not found: ${inputPath}`);
  }

  const sourcePath = resume && (await fileExists(outputPath)) ? outputPath : inputPath;
  const decisionsRaw = await fs.readFile(sourcePath, "utf8");
  const decisions = JSON.parse(decisionsRaw);
  if (!Array.isArray(decisions)) {
    throw new Error("Input must be a JSON array.");
  }

  const pendingIndexes = [];
  for (let index = 0; index < decisions.length; index += 1) {
    const decision = decisions[index];
    const hasIntro = Boolean(normalizeSpace(decision?.proceeding_intro));
    const checked = Boolean(normalizeSpace(decision?.proceeding_intro_checked_at));
    if (forceRefresh || !hasIntro) {
      if (forceRefresh || !checked || !hasIntro) {
        pendingIndexes.push(index);
      }
    }
  }

  process.stdout.write(`Records total: ${decisions.length}\n`);
  process.stdout.write(`Records pending for enrichment: ${pendingIndexes.length}\n`);
  if (!pendingIndexes.length) {
    process.stdout.write("Nothing to enrich.\n");
    return;
  }

  const errors = [];
  let nextPointer = 0;
  let processed = 0;
  let found = 0;
  let missing = 0;
  let checkpoints = 0;
  let checkpointCounter = 0;

  const writeCheckpoint = async () => {
    checkpoints += 1;
    await fs.writeFile(outputPath, `${JSON.stringify(decisions, null, 2)}\n`, "utf8");
      const summary = {
        generated_at: new Date().toISOString(),
        input_path: inputPath,
        output_path: outputPath,
        resume_source_path: sourcePath,
        transport,
        totals: {
          records_total: decisions.length,
          pending_total: pendingIndexes.length,
        processed,
        found,
        missing,
        errors: errors.length,
        checkpoints
      },
      diagnostics: {
        errors: errors.slice(0, 300)
      }
    };
    await fs.writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  };

  async function worker() {
    while (true) {
      const pointer = nextPointer;
      if (pointer >= pendingIndexes.length) return;
      nextPointer += 1;

      const index = pendingIndexes[pointer];
      const decision = decisions[index];
      const sourceUrl = normalizeSpace(decision?.source_url);
      const documentId = decision?.ids?.document_id || decision?.document_id || decision?.search_hit?.ids?.document_id || null;
      const signature = normalizeSpace(decision?.case_signature || decision?.search_hit?.signature || "") || `#${index + 1}`;

      if (!sourceUrl) {
        decision.proceeding_intro = null;
        decision.proceeding_intro_checked_at = new Date().toISOString();
        decision.proceeding_intro_error = "missing_source_url";
        errors.push({ index, signature, source_url: null, error: "missing_source_url" });
        processed += 1;
        missing += 1;
        checkpointCounter += 1;
        if (checkpointCounter >= checkpointEvery) {
          checkpointCounter = 0;
          await writeCheckpoint();
        }
        continue;
      }

      process.stdout.write(`[${pointer + 1}/${pendingIndexes.length}] enriching ${signature}\n`);
      try {
        let intro = null;
        if (transport === "curl") {
          intro = await extractProceedingIntro(sourceUrl, documentId, timeoutMs);
        } else {
          const { chromium } = await import("playwright");
          const browser = await chromium.launch({ headless: !headed });
          const context = await browser.newContext({
            ignoreHTTPSErrors: true,
            locale: "pl-PL",
            viewport: { width: 1440, height: 1000 }
          });
          const page = await context.newPage();
          await page.goto(sourceUrl, { waitUntil: "domcontentloaded", timeout: timeoutMs });
          await page.waitForSelector("form#sprawaForm", { timeout: timeoutMs });
          const html = await page.content();
          intro = normalizeSpace(extractProceedingIntroFromHtml(html, documentId)) || null;
          await page.close().catch(() => {});
          await context.close().catch(() => {});
          await browser.close().catch(() => {});
        }
        decision.proceeding_intro = intro;
        decision.proceeding_intro_checked_at = new Date().toISOString();
        if (decision.proceeding_intro_error) {
          delete decision.proceeding_intro_error;
        }
        if (intro) {
          found += 1;
        } else {
          missing += 1;
        }
      } catch (error) {
        const message = String(error?.message || error);
        decision.proceeding_intro = null;
        decision.proceeding_intro_checked_at = new Date().toISOString();
        decision.proceeding_intro_error = message;
        errors.push({ index, signature, source_url: sourceUrl, error: message });
        missing += 1;
      }

      processed += 1;
      checkpointCounter += 1;
      if (checkpointCounter >= checkpointEvery) {
        checkpointCounter = 0;
        await writeCheckpoint();
      }
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, pendingIndexes.length) }, () => worker());
  await Promise.all(workers);
  await writeCheckpoint();
  process.stdout.write(`Done. Found intro: ${found}, missing: ${missing}, errors: ${errors.length}\n`);
  process.stdout.write(`Saved decisions: ${outputPath}\n`);
  process.stdout.write(`Saved summary: ${summaryPath}\n`);
}

main().catch((error) => {
  process.stderr.write(`Fatal error: ${error?.message || error}\n`);
  process.exit(1);
});
