#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { chromium } from "playwright";

const DEFAULT_URL = "https://ipo.trybunal.gov.pl/ipo/Szukaj?cid=1";
const DEFAULT_RESULTS_PER_PAGE = 500;
const DEFAULT_TIMEOUT_MS = 60000;

const SELECTORS = {
  searchInput: "#wyszukiwanie\\:tabView\\:szukajFrazaIT_1_input",
  resultTable: "#wyszukiwanie\\:dataTable_data",
  rowsSelect: "#wyszukiwanie\\:dataTable\\:rows",
  resultsRows: "#wyszukiwanie\\:dataTable_data > tr",
  resultLinks: "#wyszukiwanie\\:dataTable_data a[href*='/ipo/Sprawa?']",
  nextPaginator: "a.ui-paginator-next"
};

const IPO_BENCH_META = [
  { key: "pelny_sklad", label: "Pełny skład" },
  { key: "piecioosobowa", label: "Pięcioosobowa" },
  { key: "trojosobowa", label: "Trójosobowa" }
];

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) continue;
    const [k, v] = token.slice(2).split("=", 2);
    if (v !== undefined) {
      out[k] = v;
      continue;
    }
    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      out[k] = next;
      i += 1;
    } else {
      out[k] = true;
    }
  }
  return out;
}

function toPositiveInt(value, fallback) {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return parsed;
}

function escapeRegex(value) {
  return String(value ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function parseDecisionIds(url) {
  try {
    const parsed = new URL(url);
    return {
      case_id: parsed.searchParams.get("sprawa"),
      document_id: parsed.searchParams.get("dokument")
    };
  } catch {
    return { case_id: null, document_id: null };
  }
}

async function launchBrowser(headless) {
  try {
    return await chromium.launch({ headless, channel: "chrome" });
  } catch {
    return chromium.launch({ headless });
  }
}

async function gotoWithRetry(page, url, timeoutMs, retries = 2) {
  let lastError = null;
  for (let attempt = 1; attempt <= retries + 1; attempt += 1) {
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
      return;
    } catch (error) {
      lastError = error;
      if (attempt <= retries) await page.waitForTimeout(500);
    }
  }
  throw lastError;
}

async function ensureSearchReady(page, timeoutMs) {
  await page.waitForSelector(SELECTORS.searchInput, { timeout: timeoutMs });
  await page.waitForSelector(SELECTORS.resultTable, { timeout: timeoutMs });
}

async function firstResultHref(page) {
  return page
    .$eval(`${SELECTORS.resultsRows} a[href*='/ipo/Sprawa?']`, (node) => node.getAttribute("href") || "")
    .catch(() => "");
}

async function waitForSearchRefresh(page, previousHref, timeoutMs) {
  await page.waitForSelector(SELECTORS.resultTable, { timeout: timeoutMs });
  if (!previousHref) {
    await page.waitForTimeout(500);
    return;
  }
  await page
    .waitForFunction(
      ({ selector, previous }) => {
        const first = document.querySelector(selector);
        if (!first) return false;
        const href = first.getAttribute("href") || "";
        return href !== previous;
      },
      {
        selector: `${SELECTORS.resultsRows} a[href*='/ipo/Sprawa?']`,
        previous: previousHref
      },
      { timeout: timeoutMs }
    )
    .catch(async () => {
      await page.waitForTimeout(1000);
    });
}

async function setResultsPerPage(page, requested, timeoutMs) {
  const values = await page.$$eval(`${SELECTORS.rowsSelect} option`, (nodes) =>
    nodes
      .map((node) => Number.parseInt(node.value, 10))
      .filter((value) => Number.isFinite(value) && value > 0)
  );
  if (!values.length) {
    return null;
  }
  const target = values.includes(requested) ? requested : Math.max(...values);
  const before = await firstResultHref(page);
  await page.selectOption(SELECTORS.rowsSelect, String(target));
  await waitForSearchRefresh(page, before, timeoutMs);
  return target;
}

async function collectRows(page) {
  return page.$$eval(SELECTORS.resultsRows, (rows) => {
    const normalize = (value) =>
      String(value ?? "")
        .replace(/\u00a0/g, " ")
        .replace(/\s+/g, " ")
        .trim();

    return rows.map((row, rowIndex) => {
      const cells = Array.from(row.querySelectorAll("td"));
      const primaryCell = cells[0] || null;
      const link = primaryCell ? primaryCell.querySelector("a[href*='/ipo/Sprawa?']") : null;
      const href = link ? link.getAttribute("href") : null;
      const signature = normalize(link ? link.textContent : "");
      return {
        row_index: rowIndex + 1,
        signature: signature || null,
        decision_url: href ? new URL(href, window.location.origin).href : null
      };
    });
  });
}

async function goToNextResultsPage(page, timeoutMs) {
  const next = page.locator(SELECTORS.nextPaginator).last();
  if ((await next.count()) === 0) {
    return false;
  }
  const classes = (await next.getAttribute("class")) || "";
  if (classes.includes("ui-state-disabled")) {
    return false;
  }
  const before = await firstResultHref(page);
  await next.click();
  await waitForSearchRefresh(page, before, timeoutMs);
  return true;
}

async function clickBenchFacet(page, benchLabel, timeoutMs) {
  const before = await firstResultHref(page);
  const facetRegex = new RegExp(`^${escapeRegex(benchLabel)} \\((\\d+)\\)$`);
  const locator = page.locator("a", { hasText: facetRegex }).first();
  if ((await locator.count()) === 0) {
    throw new Error(`Bench facet not found: ${benchLabel}`);
  }
  await locator.click({ timeout: timeoutMs });
  await waitForSearchRefresh(page, before, timeoutMs);
}

async function collectHitsForBench(page, { url, resultsPerPage, timeoutMs, benchLabel }) {
  await gotoWithRetry(page, url, timeoutMs, 2);
  await ensureSearchReady(page, timeoutMs);
  await setResultsPerPage(page, resultsPerPage, timeoutMs);
  await clickBenchFacet(page, benchLabel, timeoutMs);

  const hits = [];
  const seen = new Set();
  let pageNumber = 1;

  while (true) {
    const rows = await collectRows(page);
    if (!rows.length) break;

    for (const row of rows) {
      if (!row.decision_url || seen.has(row.decision_url)) continue;
      seen.add(row.decision_url);
      const ids = parseDecisionIds(row.decision_url);
      hits.push({
        signature: row.signature,
        decision_url: row.decision_url,
        ids
      });
    }

    const hasNext = await goToNextResultsPage(page, timeoutMs);
    if (!hasNext) break;
    pageNumber += 1;
    if (pageNumber > 100) {
      throw new Error(`Paginator safety stop reached for facet ${benchLabel}`);
    }
  }

  return hits;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const url = String(args.url || DEFAULT_URL);
  const timeoutMs = toPositiveInt(args["timeout-ms"], DEFAULT_TIMEOUT_MS);
  const resultsPerPage = toPositiveInt(args["results-per-page"], DEFAULT_RESULTS_PER_PAGE);
  const headed = Boolean(args.headed);
  const outputPath = path.resolve(args.output || "output/analysis/ipo_bench_map.json");

  const browser = await launchBrowser(!headed);
  const context = await browser.newContext({ locale: "pl-PL" });
  const page = await context.newPage();

  const benchSets = {};
  const benchCounts = {};
  const benchConflicts = [];
  const byDocumentId = new Map();

  try {
    for (const bench of IPO_BENCH_META) {
      const hits = await collectHitsForBench(page, {
        url,
        timeoutMs,
        resultsPerPage,
        benchLabel: bench.label
      });

      const documentIds = [];
      for (const hit of hits) {
        const docId = String(hit?.ids?.document_id || "").trim();
        if (!docId) continue;
        documentIds.push(docId);
        const previous = byDocumentId.get(docId);
        if (previous && previous !== bench.key) {
          benchConflicts.push({ document_id: docId, first: previous, second: bench.key });
        } else if (!previous) {
          byDocumentId.set(docId, bench.key);
        }
      }

      benchSets[bench.key] = [...new Set(documentIds)].sort((a, b) => Number(a) - Number(b));
      benchCounts[bench.key] = benchSets[bench.key].length;
      process.stdout.write(`${bench.label}: ${benchCounts[bench.key]} dokumentów\n`);
    }
  } finally {
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }

  const payload = {
    generated_at: new Date().toISOString(),
    source_url: url,
    categories: IPO_BENCH_META,
    counts: benchCounts,
    document_count_mapped: byDocumentId.size,
    conflicts_count: benchConflicts.length,
    conflicts: benchConflicts,
    bench_sets: benchSets,
    by_document_id: Object.fromEntries([...byDocumentId.entries()].sort((a, b) => Number(a[0]) - Number(b[0])))
  };

  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  process.stdout.write(`Saved IPO bench map to: ${outputPath}\n`);
}

main().catch((error) => {
  process.stderr.write(`Fatal error: ${error?.message || error}\n`);
  process.exit(1);
});
