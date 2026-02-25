#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { chromium } from "playwright";

const DEFAULT_URL = "https://ipo.trybunal.gov.pl/ipo/Szukaj?cid=1";
const DEFAULT_LIMIT = 50;
const DEFAULT_RESULTS_PER_PAGE = 500;
const DEFAULT_WHERE = "uzasadnienie";
const DEFAULT_CHECKPOINT_EVERY = 25;

const SELECTORS = {
  searchInput: "#wyszukiwanie\\:tabView\\:szukajFrazaIT_1_input",
  whereSelect: "#wyszukiwanie\\:tabView\\:gdzieSzukaj_1_input",
  inflectionCheckbox: "#wyszukiwanie\\:tabView\\:odmianaSlow_1_input",
  searchButton: "#wyszukiwanie\\:tabView\\:szukaj_button_tab1",
  rowsSelect: "#wyszukiwanie\\:dataTable\\:rows",
  resultsRows: "#wyszukiwanie\\:dataTable_data > tr",
  resultLinks: "#wyszukiwanie\\:dataTable_data a[href*='/ipo/Sprawa?']",
  nextPaginator: "a.ui-paginator-next",
  resultTable: "#wyszukiwanie\\:dataTable_data",
  timeRangePanel: "[id='filtr:facetSzablon_wyszukiwanieOkres']",
  timeRangeLinks: "[id='filtr:facetSzablon_wyszukiwanieOkres'] a[id*='facetCommand-New']"
};

const HELP_TEXT = `
Scrape judicial decisions from https://ipo.trybunal.gov.pl

Usage:
  npm run scrape:ipo -- [options]

Options:
  --limit <n>                 Number of decisions to scrape (default: ${DEFAULT_LIMIT})
  --concurrency <n>           Number of parallel detail pages (default: 1)
  --pool-size <n>             Number of search hits to collect before sampling/filtering (default: limit)
  --random-sample             Randomize collected hits before scraping
  --seed <text|number>        Seed for random sampling (default: timestamp)
  --decision-type <text>      Keep only scraped decisions whose type matches text (e.g. "Wyrok")
  --phrase <text>             Phrase to search for
  --where <value>             Search field for phrase (default: "${DEFAULT_WHERE}")
                              Common values: wyrok, komparycja, tenor, uzasadnienie
  --time-range <text>         Apply "Zakres czasowy" facet by label (e.g. "od 1986 roku")
  --results-per-page <n>      Requested rows per page (default: ${DEFAULT_RESULTS_PER_PAGE})
  --checkpoint-every <n>      Persist summary/JSON snapshots every N new decisions (default: ${DEFAULT_CHECKPOINT_EVERY})
  --resume                    Resume from existing <prefix>.progress.jsonl / .decisions.json
  --existing-decisions <path> Existing decisions JSON used as baseline for skipping already-known records
  --disable-inflection        Disable "odmiana slow" checkbox
  --output-dir <path>         Output directory (default: output/playwright)
  --output-prefix <name>      Output filename prefix (default: auto timestamp)
  --url <url>                 Override search URL (default: ${DEFAULT_URL})
  --headed                    Run browser in headed mode
  --timeout-ms <n>            Timeout per browser step (default: 60000)
  --help                      Show this help
`;

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) {
      continue;
    }
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

function toPositiveInt(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function normalizeSpace(value) {
  return String(value ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeFold(value) {
  return normalizeSpace(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function stripFacetCountSuffix(value) {
  return normalizeSpace(value).replace(/\(\d+\)\s*$/, "").trim();
}

function hashStringToSeed(value) {
  const text = String(value ?? "");
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function createSeededRng(seedValue) {
  let seed = hashStringToSeed(seedValue);
  return () => {
    seed += 0x6d2b79f5;
    let t = seed;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function shuffleInPlace(items, rng) {
  for (let i = items.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rng() * (i + 1));
    [items[i], items[j]] = [items[j], items[i]];
  }
}

function slugTimestamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
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

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

function decisionKeyFromHit(hit) {
  return String(hit?.ids?.document_id || hit?.decision_url || "");
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

async function loadProgressDecisions(progressPath) {
  if (!(await fileExists(progressPath))) {
    return [];
  }

  const text = await fs.readFile(progressPath, "utf8");
  const rows = [];
  for (const line of text.split(/\r?\n/)) {
    if (!line.trim()) continue;
    try {
      const parsed = JSON.parse(line);
      if (parsed && typeof parsed === "object") {
        rows.push(parsed);
      }
    } catch {
      // keep best-effort behavior: skip malformed lines
    }
  }
  return rows;
}

async function loadDecisionsArray(jsonPath) {
  const parsed = JSON.parse(await fs.readFile(jsonPath, "utf8"));
  if (!Array.isArray(parsed)) {
    throw new Error(`File is not a JSON array: ${jsonPath}`);
  }
  return parsed;
}

function buildParagraphLines(decisions) {
  const lines = [];
  for (const decision of decisions) {
    for (const paragraph of decision.paragraphs || []) {
      lines.push(
        JSON.stringify({
          source_url: decision.source_url,
          case_id: decision.ids?.case_id || null,
          document_id: decision.ids?.document_id || null,
          case_signature: decision.case_signature || null,
          decision_type: decision.decision_type || null,
          decision_date: decision.decision_date || null,
          topic: decision.topic || null,
          section: paragraph.section || null,
          paragraph_index: paragraph.paragraph_index,
          paragraph_number: paragraph.paragraph_number || null,
          text: paragraph.text
        })
      );
    }
  }
  return lines;
}

function sortDecisionsByRank(decisions) {
  decisions.sort((a, b) => {
    const rankA = Number(a?.scrape_meta?.result_rank || 0);
    const rankB = Number(b?.scrape_meta?.result_rank || 0);
    return rankA - rankB;
  });
}

async function gotoWithRetry(page, url, timeoutMs, retries = 2) {
  let lastError = null;
  for (let attempt = 1; attempt <= retries + 1; attempt += 1) {
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
      return;
    } catch (error) {
      lastError = error;
      if (attempt <= retries) {
        await page.waitForTimeout(600);
      }
    }
  }
  throw lastError;
}

async function launchBrowser(headless) {
  try {
    return await chromium.launch({ headless, channel: "chrome" });
  } catch {
    return chromium.launch({ headless });
  }
}

async function ensureSearchReady(page, timeoutMs) {
  await page.waitForSelector(SELECTORS.searchInput, { timeout: timeoutMs });
  await page.waitForSelector(SELECTORS.resultTable, { timeout: timeoutMs });
}

async function applySearchInputs(page, { phrase, where, disableInflection, timeoutMs }) {
  await page.fill(SELECTORS.searchInput, phrase || "");

  if (phrase) {
    const options = await page.$$eval(`${SELECTORS.whereSelect} option`, (nodes) =>
      nodes.map((node) => node.value)
    );
    if (where && options.includes(where)) {
      await page.selectOption(SELECTORS.whereSelect, where);
    }
  }

  const inflectionEnabled = await page.isChecked(SELECTORS.inflectionCheckbox).catch(() => true);
  if (disableInflection && inflectionEnabled) {
    await page.uncheck(SELECTORS.inflectionCheckbox);
  } else if (!disableInflection && !inflectionEnabled) {
    await page.check(SELECTORS.inflectionCheckbox);
  }

  if (phrase) {
    const before = await firstResultHref(page);
    await page.click(SELECTORS.searchButton);
    await waitForSearchRefresh(page, before, timeoutMs);
  }
}

async function firstResultHref(page) {
  const href = await page
    .$eval(
      `${SELECTORS.resultsRows} a[href*='/ipo/Sprawa?']`,
      (node) => node.getAttribute("href") || ""
    )
    .catch(() => "");
  return href;
}

async function waitForSearchRefresh(page, previousHref, timeoutMs) {
  await page.waitForSelector(SELECTORS.resultTable, { timeout: timeoutMs });
  if (!previousHref) {
    await page.waitForTimeout(400);
    return;
  }
  await page
    .waitForFunction(
      ({ selector, previous }) => {
        const first = document.querySelector(selector);
        if (!first) {
          return false;
        }
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
      await page.waitForTimeout(900);
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

async function collectTimeRangeFacetOptions(page) {
  const labels = await page.$$eval(SELECTORS.timeRangeLinks, (links) =>
    links.map((link) => String(link.textContent || "").replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim())
  );
  return labels.filter(Boolean);
}

async function applyTimeRangeFacet(page, label, timeoutMs) {
  const requestedLabel = normalizeSpace(label);
  if (!requestedLabel) {
    return { requested_label: null, matched_label: null, applied: false, already_selected: false };
  }

  await page.waitForSelector(SELECTORS.timeRangePanel, { timeout: timeoutMs });
  await page.waitForSelector(SELECTORS.timeRangeLinks, { timeout: timeoutMs });

  const links = page.locator(SELECTORS.timeRangeLinks);
  const count = await links.count();
  const targetNorm = normalizeFold(stripFacetCountSuffix(requestedLabel));
  let targetIndex = -1;
  let matchedLabel = null;

  for (let index = 0; index < count; index += 1) {
    const rawText = await links.nth(index).innerText();
    const text = normalizeSpace(rawText);
    const textNorm = normalizeFold(stripFacetCountSuffix(text));
    if (!textNorm) continue;
    if (textNorm.includes(targetNorm) || targetNorm.includes(textNorm)) {
      targetIndex = index;
      matchedLabel = text;
      break;
    }
  }

  if (targetIndex < 0) {
    const available = await collectTimeRangeFacetOptions(page);
    throw new Error(
      `Time range facet "${requestedLabel}" not found. Available: ${available.join(" | ")}`
    );
  }

  const target = links.nth(targetIndex);
  const alreadySelected = await target.evaluate((node) => {
    return Boolean(
      node.querySelector("img[src*='check']") ||
      node.querySelector("span[style*='font-weight: bold']")
    );
  });
  if (alreadySelected) {
    return {
      requested_label: requestedLabel,
      matched_label: matchedLabel,
      applied: false,
      already_selected: true
    };
  }

  const before = await firstResultHref(page);
  await target.click();
  await waitForSearchRefresh(page, before, timeoutMs);
  return {
    requested_label: requestedLabel,
    matched_label: matchedLabel,
    applied: true,
    already_selected: false
  };
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
      const rowText = normalize(primaryCell ? primaryCell.innerText : "");
      const snippets = cells
        .slice(1)
        .map((cell) => normalize(cell.innerText))
        .filter(Boolean);

      return {
        row_index: rowIndex + 1,
        signature: signature || null,
        decision_url: href ? new URL(href, window.location.origin).href : null,
        row_text: rowText || null,
        snippets
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

async function collectSearchHits(page, limit, timeoutMs) {
  const hits = [];
  const seen = new Set();
  let pageNumber = 1;

  while (hits.length < limit) {
    await page.waitForSelector(SELECTORS.resultsRows, { timeout: timeoutMs });
    const rows = await collectRows(page);
    if (!rows.length) {
      break;
    }

    for (const row of rows) {
      if (!row.decision_url) {
        continue;
      }
      const ids = parseDecisionIds(row.decision_url);
      const dedupeKey = ids.document_id || row.decision_url;
      if (seen.has(dedupeKey)) {
        continue;
      }
      seen.add(dedupeKey);
      hits.push({
        ...row,
        result_page: pageNumber,
        result_rank: hits.length + 1,
        ids
      });
      if (hits.length >= limit) {
        break;
      }
    }

    if (hits.length >= limit) {
      break;
    }
    const moved = await goToNextResultsPage(page, timeoutMs);
    if (!moved) {
      break;
    }
    pageNumber += 1;
  }

  return hits;
}

async function scrapeDecision(context, hit, timeoutMs) {
  const page = await context.newPage();
  let lastError = null;
  try {
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try {
        await gotoWithRetry(page, hit.decision_url, timeoutMs, 1);
        await page.waitForSelector("form#sprawaForm", { timeout: timeoutMs });

        const data = await page.evaluate(({ hitData }) => {
          const normalize = (value) =>
            String(value ?? "")
              .replace(/\u00a0/g, " ")
              .replace(/\s+/g, " ")
              .trim();
          const ascii = (value) =>
            normalize(value)
              .toLowerCase()
              .normalize("NFD")
              .replace(/[\u0300-\u036f]/g, "");
          const safeAbs = (href) => {
            try {
              return new URL(href, window.location.origin).href;
            } catch {
              return href;
            }
          };
          const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

          const params = new URLSearchParams(window.location.search);
          const documentId = hitData.ids.document_id || params.get("dokument");
          const caseId = hitData.ids.case_id || params.get("sprawa");

          const docRoot =
            document.getElementById(`sprawaForm:tabView:dok_${documentId}`) ||
            document.querySelector("[id^='sprawaForm:tabView:dok_']");

          if (!docRoot) {
            return {
              ids: { case_id: caseId, document_id: documentId },
              error: "document_root_not_found"
            };
          }

          const metadataEntries = Array.from(docRoot.querySelectorAll(".prop"))
            .map((prop) => {
              const key = normalize(prop.querySelector(".name")?.textContent);
              if (!key) {
                return null;
              }
              const valueNode = prop.querySelector(".value");
              const value = normalize(valueNode ? valueNode.innerText : prop.innerText);
              const links = Array.from(prop.querySelectorAll("a[href]")).map((anchor) => ({
                text: normalize(anchor.textContent),
                href: safeAbs(anchor.getAttribute("href"))
              }));
              return { key, value, links };
            })
            .filter(Boolean);

          const metadata = {};
          for (const entry of metadataEntries) {
            if (!(entry.key in metadata)) {
              metadata[entry.key] = entry.value;
            }
          }

          const publicationEntry =
            metadataEntries.find((entry) => ascii(entry.key).includes("miejsce publikacji")) || null;

          const publicationEntries = publicationEntry
            ? publicationEntry.links.filter((item) => item.href)
            : [];

          const findMetaValue = (needle) => {
            const target = needle.toLowerCase();
            for (const entry of metadataEntries) {
              if (ascii(entry.key).includes(target)) {
                return entry.value;
              }
            }
            return null;
          };

          const judges = [];
          const judgeTable = Array.from(docRoot.querySelectorAll("table")).find((table) => {
            const headerText = normalize(
              Array.from(table.querySelectorAll("tr:first-child th, tr:first-child td"))
                .map((cell) => cell.textContent)
                .join(" ")
            );
            const low = ascii(headerText);
            return low.includes("sedzia") && low.includes("funkcja");
          });

          if (judgeTable) {
            const rows = Array.from(judgeTable.querySelectorAll("tbody tr"));
            for (const row of rows) {
              const cells = Array.from(row.querySelectorAll("td"));
              if (!cells.length) {
                continue;
              }
              const name = normalize(cells[0]?.textContent);
              const role = normalize(cells[1]?.textContent);
              if (!name || ascii(name) === "sedzia") {
                continue;
              }
              judges.push({ name, role: role || null });
            }
          }

          const downloadAnchor =
            docRoot.querySelector(`a[id$='pobierzDoc${documentId}']`) ||
            docRoot.querySelector("a[href*='downloadOrzeczenieDoc?dok=']");

          const download = downloadAnchor
            ? {
                text: normalize(downloadAnchor.textContent),
                href: safeAbs(downloadAnchor.getAttribute("href"))
              }
            : null;

          const tocItems = [];
          const seenToc = new Set();
          for (const anchor of docRoot.querySelectorAll("a[href^='#']")) {
            const text = normalize(anchor.textContent);
            const href = anchor.getAttribute("href");
            if (!text || text.length < 2 || !href) {
              continue;
            }
            const key = `${text}|${href}`;
            if (seenToc.has(key)) {
              continue;
            }
            seenToc.add(key);
            tocItems.push({ text, href });
          }

          const proceedingIntro = Array.from(docRoot.querySelectorAll(".wyrok_zaskarzenie_tytul"))
            .map((node) => normalize(node.textContent))
            .find((value) => value.length >= 12) || null;

          const headingSelector = [
            ".wyrok_uzasadnienie_tytul",
            ".wyrok_uzasadnienie_czesc",
            ".wyrok_sentencja_tytul",
            ".wyrok_sentencja_czesc",
            ".wyrok_naglowekNumerowany"
          ].join(", ");
          const paragraphSelector = [".wyrok_akapitCaly", ".wyrok_akapit", ".wyrok_akapitNumerowany"].join(
            ", "
          );
          const stream = Array.from(docRoot.querySelectorAll(`${headingSelector}, ${paragraphSelector}`));

          const paragraphs = [];
          const sections = [];
          const seenParagraph = new Set();
          let currentSection = null;

          for (const node of stream) {
            const isHeading = node.matches(headingSelector);
            if (isHeading) {
              const headingText = normalize(node.textContent);
              if (headingText && sections[sections.length - 1] !== headingText) {
                sections.push(headingText);
                currentSection = headingText;
              } else if (headingText) {
                currentSection = headingText;
              }
              continue;
            }

            if (node.classList.contains("wyrok_akapitNumerowany") && node.closest(".wyrok_akapitCaly")) {
              continue;
            }

            let paragraphNumber = normalize(node.querySelector(".wyrok_akapitNr")?.textContent);
            let text = normalize(node.textContent);

            if (paragraphNumber && text) {
              const prefix = new RegExp(`^${escapeRegExp(paragraphNumber)}\\s*`);
              text = normalize(text.replace(prefix, ""));
            }
            if (!text) {
              continue;
            }
            if (!paragraphNumber) {
              const inferred = text.match(/^(\d+[.)]?)\s+/);
              if (inferred) {
                paragraphNumber = normalize(inferred[1]);
                text = normalize(text.slice(inferred[0].length));
              }
            }
            if (!text) {
              continue;
            }
            if (currentSection && ascii(text) === ascii(currentSection)) {
              continue;
            }

            const dedupe = `${paragraphNumber || ""}|${currentSection || ""}|${text}`;
            if (seenParagraph.has(dedupe)) {
              continue;
            }
            seenParagraph.add(dedupe);

            paragraphs.push({
              paragraph_index: paragraphs.length + 1,
              paragraph_number: paragraphNumber || null,
              section: currentSection,
              text
            });
          }

          if (!paragraphs.length) {
            const lines = normalize(docRoot.innerText)
              .split(/(?<=\.)\s+/)
              .map((line) => normalize(line))
              .filter((line) => line.length > 30);
            for (const line of lines) {
              paragraphs.push({
                paragraph_index: paragraphs.length + 1,
                paragraph_number: null,
                section: null,
                text: line
              });
            }
          }

          const signatureCandidate = Array.from(docRoot.querySelectorAll("strong, p, div, span"))
            .map((node) => normalize(node.textContent))
            .find((value) => ascii(value).startsWith("sygn. akt"));
          const signature = signatureCandidate
            ? normalize(signatureCandidate.replace(/^sygn\.?\s*akt\s*/i, ""))
            : hitData.signature || null;

          return {
            source_url: window.location.href,
            ids: {
              case_id: caseId,
              document_id: documentId
            },
            case_signature: signature || hitData.signature || null,
            decision_type: findMetaValue("rodzaj orzeczenia"),
            decision_date: findMetaValue("data"),
            topic: findMetaValue("dotyczy"),
            proceeding_intro: proceedingIntro,
            metadata,
            metadata_entries: metadataEntries,
            publication_entries: publicationEntries,
            judges,
            table_of_contents: tocItems,
            sections,
            paragraphs,
            download
          };
        }, { hitData: hit });

        return data;
      } catch (error) {
        lastError = error;
        await page.waitForTimeout(500);
      }
    }
    throw lastError;
  } finally {
    await page.close().catch(() => {});
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    process.stdout.write(HELP_TEXT.trimStart());
    process.stdout.write("\n");
    return;
  }

  const limit = toPositiveInt(args.limit, DEFAULT_LIMIT);
  const concurrency = Math.min(12, Math.max(1, toPositiveInt(args.concurrency, 1)));
  const checkpointEvery = toPositiveInt(args["checkpoint-every"], DEFAULT_CHECKPOINT_EVERY);
  const resume = Boolean(args.resume);
  const poolSize = toPositiveInt(args["pool-size"], limit);
  const resultsPerPage = toPositiveInt(args["results-per-page"], DEFAULT_RESULTS_PER_PAGE);
  const timeoutMs = toPositiveInt(args["timeout-ms"], 60000);
  const randomSample = Boolean(args["random-sample"]);
  const samplingSeed = String(args.seed ?? slugTimestamp());
  const decisionTypeFilter = typeof args["decision-type"] === "string" ? normalizeFold(args["decision-type"]) : "";
  const phrase = typeof args.phrase === "string" ? args.phrase : "";
  const where = typeof args.where === "string" ? args.where : DEFAULT_WHERE;
  const timeRangeLabel = typeof args["time-range"] === "string" ? normalizeSpace(args["time-range"]) : "";
  const disableInflection = Boolean(args["disable-inflection"]);
  const headed = Boolean(args.headed);
  const baseUrl = typeof args.url === "string" ? args.url : DEFAULT_URL;
  const existingDecisionsPath =
    typeof args["existing-decisions"] === "string"
      ? path.resolve(args["existing-decisions"])
      : null;
  const outputDir = path.resolve(typeof args["output-dir"] === "string" ? args["output-dir"] : "output/playwright");
  const outputPrefix =
    typeof args["output-prefix"] === "string" ? args["output-prefix"] : `ipo-${slugTimestamp()}-limit-${limit}`;

  await fs.mkdir(outputDir, { recursive: true });

  const decisionsPath = path.join(outputDir, `${outputPrefix}.decisions.json`);
  const progressPath = path.join(outputDir, `${outputPrefix}.progress.jsonl`);
  const hitsPath = path.join(outputDir, `${outputPrefix}.hits.json`);
  const paragraphsPath = path.join(outputDir, `${outputPrefix}.paragraphs.jsonl`);
  const summaryPath = path.join(outputDir, `${outputPrefix}.summary.json`);

  const existingDecisions = [];
  const existingKnownKeys = new Set();
  if (existingDecisionsPath) {
    if (!(await fileExists(existingDecisionsPath))) {
      throw new Error(`Existing decisions file not found: ${existingDecisionsPath}`);
    }
    existingDecisions.push(...(await loadDecisionsArray(existingDecisionsPath)));
    for (const decision of existingDecisions) {
      const key = decisionKeyFromDecision(decision);
      if (!key) continue;
      existingKnownKeys.add(key);
    }
    process.stdout.write(
      `Loaded baseline decisions: ${existingDecisions.length} (${existingKnownKeys.size} unique keys) from ${existingDecisionsPath}\n`
    );
  }

  let decisions = [];
  if (resume) {
    decisions = await loadProgressDecisions(progressPath);
    if (!decisions.length && (await fileExists(decisionsPath))) {
      const fromJson = JSON.parse(await fs.readFile(decisionsPath, "utf8"));
      if (Array.isArray(fromJson)) {
        decisions = fromJson;
      }
    }
  } else {
    await fs.writeFile(progressPath, "", "utf8");
  }

  const deduped = new Map();
  for (const decision of decisions) {
    const key = decisionKeyFromDecision(decision);
    if (!key || deduped.has(key)) continue;
    deduped.set(key, decision);
  }
  decisions = [...deduped.values()];
  sortDecisionsByRank(decisions);
  if (decisions.length > limit) {
    decisions.length = limit;
  }
  const resumeLoaded = decisions.length;
  if (resumeLoaded) {
    process.stdout.write(`Resume mode: loaded ${resumeLoaded} decisions from progress.\n`);
  }

  const browser = await launchBrowser(!headed);
  const context = await browser.newContext({
    ignoreHTTPSErrors: true,
    locale: "pl-PL",
    viewport: { width: 1440, height: 1000 }
  });
  const page = await context.newPage();

  try {
    let rawHits = [];
    let appliedRows = null;
    let appliedTimeRange = {
      requested_label: timeRangeLabel || null,
      matched_label: null,
      applied: false,
      already_selected: false
    };

    if (resume && (await fileExists(hitsPath))) {
      const cached = JSON.parse(await fs.readFile(hitsPath, "utf8"));
      if (Array.isArray(cached)) {
        rawHits = cached;
      }
      process.stdout.write(`Resume mode: loaded ${rawHits.length} cached hits.\n`);
    }

    if (!rawHits.length) {
      await gotoWithRetry(page, baseUrl, timeoutMs, 2);
      await ensureSearchReady(page, timeoutMs);

      await applySearchInputs(page, {
        phrase,
        where,
        disableInflection,
        timeoutMs
      });

      if (timeRangeLabel) {
        appliedTimeRange = await applyTimeRangeFacet(page, timeRangeLabel, timeoutMs);
        const stateLabel = appliedTimeRange.already_selected
          ? "already selected"
          : (appliedTimeRange.applied ? "applied" : "not applied");
        process.stdout.write(
          `Time range facet ${stateLabel}: ${appliedTimeRange.matched_label || appliedTimeRange.requested_label}\n`
        );
      }

      appliedRows = await setResultsPerPage(page, resultsPerPage, timeoutMs);
      rawHits = await collectSearchHits(page, Math.max(limit, poolSize), timeoutMs);
      await fs.writeFile(hitsPath, `${JSON.stringify(rawHits, null, 2)}\n`, "utf8");
    }

    let hits = rawHits.slice();

    if (decisionTypeFilter) {
      hits = hits.filter((hit) => normalizeFold(hit.row_text || "").includes(decisionTypeFilter));
    }

    const knownKeys = new Set([
      ...existingKnownKeys,
      ...decisions.map((decision) => decisionKeyFromDecision(decision)).filter(Boolean)
    ]);
    let skippedKnownHits = 0;
    if (knownKeys.size) {
      const freshHits = [];
      for (const hit of hits) {
        const hitKey = decisionKeyFromHit(hit);
        if (hitKey && knownKeys.has(hitKey)) {
          skippedKnownHits += 1;
          continue;
        }
        freshHits.push(hit);
      }
      hits = freshHits;
      process.stdout.write(
        `Known-record prefilter: skipped ${skippedKnownHits} hits, ${hits.length} candidate hits remain.\n`
      );
    }

    if (randomSample) {
      const rng = createSeededRng(samplingSeed);
      shuffleInPlace(hits, rng);
    }

    if (!rawHits.length) {
      throw new Error("No search hits found. Try a different phrase or remove filters.");
    }
    if (!hits.length) {
      process.stdout.write("No new hits to scrape after applying filters and baseline dedupe.\n");
    }

    const scrapeErrors = [];
    const rejectedByDecisionType = [];
    let nextHitIndex = 0;
    let stopRequested = false;
    let scrapedSinceCheckpoint = 0;
    let appendQueue = Promise.resolve();
    let appendError = null;

    const buildSummary = (totalParagraphs, isCheckpoint = false) => ({
      generated_at: new Date().toISOString(),
      source_url: baseUrl,
      query: {
        phrase,
        where,
        time_range_label: timeRangeLabel || null,
        time_range_matched_label: appliedTimeRange.matched_label || null,
        time_range_already_selected: Boolean(appliedTimeRange.already_selected),
        disable_inflection: disableInflection,
        decision_type_filter: decisionTypeFilter || null
      },
      sampling: {
        pool_size_requested: poolSize,
        random_sample: randomSample,
        seed: randomSample ? samplingSeed : null
      },
      runtime: {
        concurrency,
        resume,
        checkpoint_every: checkpointEvery,
        stop_requested: stopRequested,
        is_checkpoint: isCheckpoint
      },
      crawl: {
        requested_limit: limit,
        collected_hits: rawHits.length,
        collected_hits_after_prefilter: hits.length,
        skipped_known_hits: skippedKnownHits,
        scraped_decisions: decisions.length,
        resumed_decisions: resumeLoaded,
        rejected_by_decision_type: rejectedByDecisionType.length,
        scrape_errors: scrapeErrors.length,
        requested_results_per_page: resultsPerPage,
        applied_results_per_page: appliedRows,
        next_hit_index: nextHitIndex
      },
      stats: {
        total_paragraphs: totalParagraphs,
        avg_paragraphs_per_decision:
          decisions.length > 0
            ? Number((totalParagraphs / decisions.length).toFixed(2))
            : 0
      },
      files: {
        decisions_json: decisionsPath,
        progress_jsonl: progressPath,
        hits_json: hitsPath,
        paragraphs_jsonl: paragraphsPath
      },
      incremental: {
        baseline_decisions_path: existingDecisionsPath || null,
        baseline_known_keys: existingKnownKeys.size
      },
      diagnostics: {
        scrape_errors: scrapeErrors.slice(0, 100),
        rejected_by_decision_type: rejectedByDecisionType.slice(0, 100)
      }
    });

    const writeCheckpoint = async () => {
      sortDecisionsByRank(decisions);
      if (decisions.length > limit) {
        decisions.length = limit;
      }
      const totalParagraphs = decisions.reduce(
        (sum, decision) => sum + ((decision.paragraphs && decision.paragraphs.length) || 0),
        0
      );
      await fs.writeFile(decisionsPath, `${JSON.stringify(decisions, null, 2)}\n`, "utf8");
      await fs.writeFile(summaryPath, `${JSON.stringify(buildSummary(totalParagraphs, true), null, 2)}\n`, "utf8");
    };

    const queueProgressAppend = (record) => {
      appendQueue = appendQueue
        .then(() => fs.appendFile(progressPath, `${JSON.stringify(record)}\n`, "utf8"))
        .catch((error) => {
          appendError = error;
          throw error;
        });
    };

    const onSigint = () => {
      if (stopRequested) {
        process.stdout.write("\nForce stopping...\n");
        process.exit(130);
      }
      stopRequested = true;
      process.stdout.write("\nSIGINT received: stopping after current pages and saving progress...\n");
    };
    process.on("SIGINT", onSigint);

    async function runWorker() {
      while (true) {
        if (stopRequested) {
          return;
        }
        const index = nextHitIndex;
        if (index >= hits.length) {
          return;
        }
        nextHitIndex += 1;
        if (decisions.length >= limit) {
          return;
        }

        const hit = hits[index];
        const hitKey = decisionKeyFromHit(hit);
        if (hitKey && knownKeys.has(hitKey)) {
          skippedKnownHits += 1;
          continue;
        }
        process.stdout.write(
          `[${index + 1}/${hits.length}] scraping ${hit.signature || hit.decision_url}\n`
        );

        let scraped;
        try {
          scraped = await scrapeDecision(context, hit, timeoutMs);
        } catch (error) {
          scrapeErrors.push({
            decision_url: hit.decision_url,
            signature: hit.signature || null,
            error: String(error?.message || error)
          });
          process.stdout.write(`  !! scrape failed, skipping (${error?.message || error})\n`);
          continue;
        }

        if (decisionTypeFilter) {
          const normalizedType = normalizeFold(scraped.decision_type || "");
          if (!normalizedType.includes(decisionTypeFilter)) {
            rejectedByDecisionType.push({
              decision_url: hit.decision_url,
              signature: scraped.case_signature || hit.signature || null,
              decision_type: scraped.decision_type || null
            });
            continue;
          }
        }

        const record = {
          scrape_meta: {
            scraped_at: new Date().toISOString(),
            result_rank: hit.result_rank,
            result_page: hit.result_page
          },
          search_hit: hit,
          ...scraped
        };

        decisions.push(record);
        if (hitKey) {
          knownKeys.add(hitKey);
        }
        queueProgressAppend(record);
        scrapedSinceCheckpoint += 1;

        if (scrapedSinceCheckpoint >= checkpointEvery) {
          scrapedSinceCheckpoint = 0;
          await writeCheckpoint();
        }
      }
    }

    if (decisions.length < limit) {
      const workers = Array.from({ length: Math.min(concurrency, hits.length) }, () => runWorker());
      await Promise.all(workers);
    }

    await appendQueue.catch(() => {});
    if (appendError) {
      throw appendError;
    }
    process.removeListener("SIGINT", onSigint);

    sortDecisionsByRank(decisions);
    if (decisions.length > limit) {
      decisions.length = limit;
    }

    const paragraphLines = buildParagraphLines(decisions);
    const summary = buildSummary(paragraphLines.length, false);

    await fs.writeFile(decisionsPath, `${JSON.stringify(decisions, null, 2)}\n`, "utf8");
    await fs.writeFile(paragraphsPath, paragraphLines.join("\n"), "utf8");
    await fs.writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");

    process.stdout.write(`\nSaved decisions: ${decisionsPath}\n`);
    process.stdout.write(`Saved paragraphs: ${paragraphsPath}\n`);
    process.stdout.write(`Saved summary: ${summaryPath}\n`);
    process.stdout.write(
      `Extracted ${decisions.length} decisions and ${paragraphLines.length} paragraphs.\n`
    );
  } finally {
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

main().catch((error) => {
  process.stderr.write(`Fatal error: ${error?.message || error}\n`);
  process.exit(1);
});
