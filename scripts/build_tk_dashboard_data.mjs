#!/usr/bin/env node

import fs from "node:fs/promises";
import { createHash } from "node:crypto";
import path from "node:path";
import process from "node:process";

const normalizeDictionaryEntry = (value) => String(value ?? "")
  .toLowerCase()
  .normalize("NFD")
  .replace(/[\u0300-\u036f]/g, "")
  .replace(/ł/g, "l")
  .replace(/\s+/g, " ")
  .trim();

const SECTION_META = {
  komparycja: { label: "Komparycja / Skład", color: "#6b7280" },
  tenor: { label: "Tenor", color: "#be123c" },
  orzeka: { label: "Orzeka", color: "#dc2626" },
  postanawia: { label: "Postanawia", color: "#f97316" },
  uzasadnienie_historyczne: { label: "Uzasadnienie: część historyczna", color: "#2563eb" },
  uzasadnienie_postepowanie: { label: "Uzasadnienie: część postępowania", color: "#0ea5e9" },
  uzasadnienie_prawne: { label: "Uzasadnienie prawne", color: "#16a34a" },
  uzasadnienie_ogolne: { label: "Uzasadnienie (ogólne)", color: "#65a30d" },
  zdanie_odrebne: { label: "Zdanie odrębne", color: "#7c3aed" },
  sentencja_inna: { label: "Sentencja (inne)", color: "#f59e0b" },
  inne: { label: "Inne", color: "#94a3b8" }
};

const IPO_DECISION_TYPE_META = [
  { key: "postanowienie", label: "Postanowienie" },
  { key: "postanowienie_tymczasowe", label: "Postanowienie Tymczasowe" },
  { key: "rozstrzygniecie", label: "Rozstrzygnięcie" },
  { key: "wyrok", label: "Wyrok" }
];
const IPO_DECISION_TYPE_BY_KEY = new Map(IPO_DECISION_TYPE_META.map((entry) => [entry.key, entry]));

const IPO_BENCH_META = [
  { key: "pelny_sklad", label: "Pełny skład" },
  { key: "piecioosobowa", label: "Pięcioosobowa" },
  { key: "trojosobowa", label: "Trójosobowa" }
];
const IPO_BENCH_BY_KEY = new Map(IPO_BENCH_META.map((entry) => [entry.key, entry]));
const DEFAULT_IPO_BENCH_MAP_PATH = path.resolve("output/analysis/ipo_bench_map.json");

const NORMALIZATION_VERSION = "tk-norm-v2";
const SCHEMA_VERSION = "tk-dashboard-v2";
const BIGRAM_STOPWORDS = new Set([
  "i", "oraz", "lub", "na", "w", "z", "do", "od", "o", "u", "a", "ze", "sie", "jest", "sa", "to", "ten", "ta", "te",
  "nie", "dla", "przez", "ktory", "ktora", "ktore", "jako", "po", "za", "co", "czy", "art", "ust", "pkt", "par", "sygn",
  "trybunal", "konstytucyjny", "ustawy", "ustawa", "konstytucji", "dnia", "roku", "orzeczenie", "postepowania",
  "otk", "zu", "poz", "nr", "dz", "lit", "dalej", "tk", "rp"
]);
const BIGRAM_NOISE_PHRASES_RAW = [
  "trybunału konstytucyjnego",
  "trybunale konstytucyjnym",
  "trybunałem konstytucyjnym",
  "rzeczypospolitej polskiej",
  "niniejszej sprawie",
  "tym samym",
  "tym zakresie",
  "punktu widzenia",
  "przede wszystkim",
  "przy tym",
  "pod uwagę",
  "tego rodzaju",
  "tego przepisu",
  "których mowa",
  "którym mowa",
  "w związku",
  "związku tym",
  "może być",
  "mogą być",
  "musi być",
  "zob wyrok",
  "por wyrok",
  "sierpnia trybunale",
  "sąd pytający",
  "pytający sąd",
  "zakresie jakim",
  "przed trybunałem"
];
const BIGRAM_NOISE_PHRASES = new Set(BIGRAM_NOISE_PHRASES_RAW.map((phrase) => normalizeDictionaryEntry(phrase)));
const BIGRAMS_FILTERING_DISCLAIMER = "Ranking bigramów wyklucza frazy boilerplate, zwroty techniczne i nazwy instytucjonalne o niskiej wartości argumentacyjnej dla analizy prawniczej.";
const BIGRAM_CANONICAL_ALIASES_RAW = [
  ["marszałek sejmu", "marszałek sejmu"],
  ["marszałka sejmu", "marszałek sejmu"],
  ["marszałkowi sejmu", "marszałek sejmu"],
  ["prokurator generalny", "prokurator generalny"],
  ["prokuratora generalnego", "prokurator generalny"],
  ["prokuratorem generalnym", "prokurator generalny"],
  ["rada ministrów", "rada ministrów"],
  ["rady ministrów", "rada ministrów"],
  ["radzie ministrów", "rada ministrów"],
  ["pytanie prawne", "pytanie prawne"],
  ["pytania prawnego", "pytanie prawne"],
  ["pytaniem prawnym", "pytanie prawne"]
];
const BIGRAM_CANONICAL_ALIASES = new Map(
  BIGRAM_CANONICAL_ALIASES_RAW.map(([rawAlias, rawCanonical]) => [
    normalizeDictionaryEntry(rawAlias),
    normalizeSpace(rawCanonical)
  ])
);

const SENTENCJA_SECTION_KEYS = new Set(["tenor", "orzeka", "postanawia", "sentencja_inna"]);
const REASONING_SECTION_KEYS = new Set([
  "uzasadnienie_historyczne",
  "uzasadnienie_postepowanie",
  "uzasadnienie_prawne",
  "uzasadnienie_ogolne"
]);
const ALL_SECTION_KEYS = Object.keys(SECTION_META);
const DEFAULT_LLM_MODEL = "gpt-5-mini";
const DEFAULT_LLM_BATCH_SIZE = 20;
const DEFAULT_LLM_MIN_CONFIDENCE = 0.75;
const DEFAULT_LLM_TIMEOUT_MS = 120000;
const DEFAULT_LLM_MAX_RETRIES = 3;
const DEFAULT_LLM_BASE_URL = "https://api.openai.com/v1";

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) {
      continue;
    }
    const [rawKey, rawValue] = token.slice(2).split("=", 2);
    if (rawValue !== undefined) {
      out[rawKey] = rawValue;
      continue;
    }
    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      out[rawKey] = next;
      i += 1;
    } else {
      out[rawKey] = true;
    }
  }
  return out;
}

function parseBooleanArg(value, fallback = false) {
  if (value === true) return true;
  if (value === false || value === null || value === undefined) return fallback;
  const norm = normalizeText(value);
  if (["1", "true", "yes", "y", "tak"].includes(norm)) return true;
  if (["0", "false", "no", "n", "nie"].includes(norm)) return false;
  return fallback;
}

function parseIntegerArg(value, fallback, min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  if (parsed < min || parsed > max) return fallback;
  return parsed;
}

function parseFloatArg(value, fallback, min = Number.NEGATIVE_INFINITY, max = Number.POSITIVE_INFINITY) {
  const parsed = Number.parseFloat(String(value ?? ""));
  if (!Number.isFinite(parsed)) return fallback;
  if (parsed < min || parsed > max) return fallback;
  return parsed;
}

function toNonEmptyString(value, fallback = "") {
  const normalized = normalizeSpace(value);
  return normalized || fallback;
}

function stripMarkdownFences(text) {
  const value = normalizeSpace(text);
  if (!value.startsWith("```")) return value;
  return value
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/, "")
    .trim();
}

function safeJsonParse(value, fallback = null) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function truncateForLlm(value, limit = 900) {
  const normalized = normalizeSpace(value);
  if (normalized.length <= limit) return normalized;
  return `${normalized.slice(0, Math.max(0, limit - 1))}…`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeSpace(value) {
  return String(value ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeText(value) {
  return normalizeSpace(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/ł/g, "l");
}

function tokenizeForBigrams(value) {
  return normalizeSpace(value)
    .toLowerCase()
    .split(/[^\p{L}\p{N}]+/u)
    .filter(Boolean)
    .map((token) => ({
      token,
      norm: normalizeText(token)
    }))
    .filter(({ norm }) => norm.length >= 3 && !/^\d+$/.test(norm) && !BIGRAM_STOPWORDS.has(norm));
}

function canonicalizeBigram(rawKey, fallbackLabel) {
  const normalizedKey = normalizeDictionaryEntry(rawKey);
  const canonicalLabel = BIGRAM_CANONICAL_ALIASES.get(normalizedKey);
  if (!canonicalLabel) {
    return {
      key: normalizedKey,
      label: normalizeSpace(fallbackLabel)
    };
  }
  return {
    key: normalizeDictionaryEntry(canonicalLabel),
    label: canonicalLabel
  };
}

function judgePairKey(a, b) {
  if (!a || !b) return "";
  return a.localeCompare(b, "pl") <= 0 ? `${a}|||${b}` : `${b}|||${a}`;
}

function monthToNumber(monthNorm) {
  const map = {
    stycznia: 1,
    lutego: 2,
    marca: 3,
    kwietnia: 4,
    maja: 5,
    czerwca: 6,
    lipca: 7,
    sierpnia: 8,
    wrzesnia: 9,
    "wrzesnia": 9,
    "wrzesnia": 9,
    pazdziernika: 10,
    listopada: 11,
    grudnia: 12
  };
  return map[monthNorm] || null;
}

function parsePolishDate(raw) {
  const text = normalizeSpace(raw);
  if (!text) {
    return { iso: null, year: null };
  }

  let match = text.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (match) {
    const day = Number(match[1]);
    const month = Number(match[2]);
    const year = Number(match[3]);
    return { iso: `${year.toString().padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`, year };
  }

  match = text.match(/^(\d{1,2})\s+([\p{L}]+)\s+(\d{4})$/u);
  if (match) {
    const day = Number(match[1]);
    const monthNorm = normalizeText(match[2]);
    const month = monthToNumber(monthNorm);
    const year = Number(match[3]);
    if (month) {
      return {
        iso: `${year.toString().padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`,
        year
      };
    }
  }

  match = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (match) {
    const year = Number(match[1]);
    return { iso: text, year };
  }

  return { iso: null, year: null };
}

function classifySectionFromText(sectionText) {
  const norm = normalizeText(sectionText);
  if (!norm) {
    return "inne";
  }
  if (norm.includes("komparycja")) return "komparycja";
  if (norm === "tenor" || norm.includes("tenor")) return "tenor";
  if (norm.includes("zdanie odrebne")) return "zdanie_odrebne";
  if (norm.includes("orzeka")) return "orzeka";
  if (norm.includes("postanawia") || norm.includes("umorzyc postepowanie") || norm.includes("umorzyc postepowanie")) return "postanawia";
  if (norm.includes("sentencja")) return "sentencja_inna";
  if (norm.includes("uzasadnienie")) {
    if (norm.includes("historycz")) return "uzasadnienie_historyczne";
    if (norm.includes("rozpraw") || norm.includes("posiedzen") || norm.includes("przed rozpraw")) return "uzasadnienie_postepowanie";
    if (norm.includes("prawne")) return "uzasadnienie_prawne";
    return "uzasadnienie_ogolne";
  }
  if (norm.includes("czesc history")) return "uzasadnienie_historyczne";
  if (norm.includes("czesc na rozpraw") || norm.includes("czesc przed rozpraw") || norm.includes("posiedzen")) return "uzasadnienie_postepowanie";
  if (norm.includes("uzasadnienie prawne")) return "uzasadnienie_prawne";
  return "inne";
}

function extractRoman(value) {
  const text = normalizeSpace(value);
  if (!text) return null;
  const match = text.match(/^([IVXLCDM]+)(?:\b|\s*[-–:])/i);
  return match ? match[1].toUpperCase() : null;
}

function buildRomanSectionMap(tableOfContents) {
  const romanMap = new Map();
  for (const entry of tableOfContents || []) {
    const text = normalizeSpace(entry?.text);
    if (!text) continue;
    const roman = extractRoman(text);
    if (!roman) continue;

    let classified = classifySectionFromText(text);
    if (classified === "inne") {
      const rest = text.replace(/^([IVXLCDM]+)\s*[-–:]?\s*/i, "");
      classified = classifySectionFromText(rest);
    }
    if (classified !== "inne") {
      romanMap.set(roman, classified);
    }
  }
  return romanMap;
}

function resolveSection(paragraph, romanMap) {
  const raw = normalizeSpace(paragraph?.section);
  const direct = classifySectionFromText(raw);
  if (direct !== "inne") {
    return {
      key: direct,
      label: SECTION_META[direct].label,
      raw,
      confidence: 0.98
    };
  }

  const roman = extractRoman(raw);
  if (roman && romanMap.has(roman)) {
    const key = romanMap.get(roman);
    return {
      key,
      label: SECTION_META[key].label,
      raw,
      confidence: 0.9
    };
  }

  if (/^[IVXLCDM]+$/i.test(raw) && romanMap.has(raw.toUpperCase())) {
    const key = romanMap.get(raw.toUpperCase());
    return {
      key,
      label: SECTION_META[key].label,
      raw,
      confidence: 0.88
    };
  }

  return {
    key: "inne",
    label: SECTION_META.inne.label,
    raw,
    confidence: 0.35
  };
}

function looksLikeTenorParagraph(text, paragraphNumber, paragraphIndex) {
  const norm = normalizeText(text);
  if (!norm) return false;
  if (paragraphIndex <= 3 && paragraphNumber) return true;
  if (paragraphIndex <= 2 && norm.startsWith("czy ")) return true;
  if (paragraphIndex <= 3 && /\bart\.?\s*\d/.test(norm)) return true;
  return false;
}

function mergeSplitDissentHeadings(paragraphs) {
  const merged = [];

  for (let i = 0; i < paragraphs.length; i += 1) {
    const current = paragraphs[i];
    if (!current || current.section_key !== "zdanie_odrebne") {
      merged.push(current);
      continue;
    }

    const currentNorm = normalizeText(current.text || "");
    const startsJudgeLine = /^sedziego\s+tk\b/.test(currentNorm);
    if (!startsJudgeLine) {
      merged.push(current);
      continue;
    }

    const parts = [normalizeSpace(current.text)];
    let lookahead = i + 1;

    while (lookahead < paragraphs.length) {
      const next = paragraphs[lookahead];
      if (!next || next.section_key !== "zdanie_odrebne") break;
      const nextText = normalizeSpace(next.text);
      const nextNorm = normalizeText(nextText);
      const isHeadingContinuation =
        nextText.length <= 220
        && (
          nextNorm.startsWith("do ")
          || nextNorm.startsWith("z dnia ")
          || nextNorm.startsWith("sygn.")
          || nextNorm.startsWith("sygn ")
        );
      if (!isHeadingContinuation) break;
      parts.push(nextText);
      lookahead += 1;
    }

    if (parts.length > 1) {
      const mergedText = `Zdanie odrębne ${parts.join(" ")}`.replace(/\s+/g, " ").trim();
      merged.push({
        ...current,
        text: mergedText
      });
      i = lookahead - 1;
      continue;
    }

    merged.push(current);
  }

  return merged;
}

function findNeighborKnownSection(paragraphs, startIndex, direction, maxDistance = 10) {
  for (let distance = 1; distance <= maxDistance; distance += 1) {
    const index = startIndex + (direction * distance);
    if (index < 0 || index >= paragraphs.length) break;
    const paragraph = paragraphs[index];
    if (!paragraph) continue;
    const key = paragraph.section_key;
    if (key && key !== "inne") {
      return {
        key,
        distance,
        index
      };
    }
  }
  return null;
}

function looksLikeSectionDivider(normText) {
  if (!normText) return false;
  return /^([ivxlcdm]+)\.?$/i.test(normText)
    || /^[ivxlcdm]+\.$/i.test(normText)
    || /^czesc\s+[ivxlcdm]+/i.test(normText);
}

function looksLikeListItem(normText) {
  if (!normText) return false;
  return /^(\(?\d{1,3}[).]|[a-z][).]|[ivxlcdm]+\.)\s+/i.test(normText);
}

function looksLikeSentencjaLine(normText) {
  if (!normText) return false;
  return /^[-–—]/.test(normText)
    || /^(art\.?|§|paragraf|pkt\.?|ust\.?|orzeczenie)\b/.test(normText)
    || looksLikeListItem(normText)
    || /\b(jest|sa)\s+(niezgodn|zgodn|dopuszczaln|niedopuszczaln)/.test(normText)
    || /\b(umorzyc|umarza|postanawia|orzeka|odmawia|odrzuca|oddalic)\b/.test(normText);
}

function looksLikeReasoningHistory(normText) {
  if (!normText) return false;
  return /\b(wnioskiem z dnia|w skardze konstytucyjnej|postepowanie zostalo|prokurator generalny|marszalek sejmu|uczestnik postepowania|stan faktyczny|w pismie z|na rozprawie)\b/.test(normText);
}

function looksLikeReasoningLegal(normText) {
  if (!normText) return false;
  return /\b(trybunal konstytucyjny zwazyl|w ocenie trybunalu|nalezy uznac|wzorcem kontroli|kontrola konstytucyjnosci|narusza zasade|zgodnie z utrwalonym orzecznictwem)\b/.test(normText);
}

function inferUnknownSection(paragraphs, paragraphIndex, caseMeta = {}) {
  const paragraph = paragraphs[paragraphIndex];
  if (!paragraph || paragraph.section_key !== "inne") return null;

  const text = normalizeSpace(paragraph.text || "");
  const normText = normalizeText(text);
  if (!normText) return null;

  const rawNorm = normalizeText(paragraph.section_raw || "");
  const totalParagraphs = Math.max(1, paragraphs.length);
  const position = (paragraphIndex + 1) / totalParagraphs;
  const prevKnown = findNeighborKnownSection(paragraphs, paragraphIndex, -1);
  const nextKnown = findNeighborKnownSection(paragraphs, paragraphIndex, +1);

  if (normText.startsWith("zdanie odrebne")) {
    return { key: "zdanie_odrebne", confidence: 0.98, reason: "dissent-heading" };
  }

  if (rawNorm.includes("trybunal zwazyl")) {
    return { key: "uzasadnienie_prawne", confidence: 0.94, reason: "raw-legal-heading" };
  }
  if (rawNorm.includes("stan faktyczny") || rawNorm.includes("wnioskiem z") || rawNorm.includes("w skardze konstytucyjnej")) {
    return { key: "uzasadnienie_historyczne", confidence: 0.9, reason: "raw-history-heading" };
  }
  if (rawNorm.includes("na rozprawie") || rawNorm.includes("posiedzeniu")) {
    return { key: "uzasadnienie_postepowanie", confidence: 0.9, reason: "raw-procedure-heading" };
  }
  if (rawNorm.includes("postanawia")) {
    return { key: "postanawia", confidence: 0.9, reason: "raw-postanawia-heading" };
  }
  if (rawNorm.includes("orzeka")) {
    return { key: "orzeka", confidence: 0.9, reason: "raw-orzeka-heading" };
  }

  if (prevKnown && nextKnown && prevKnown.key === nextKnown.key && prevKnown.distance <= 2 && nextKnown.distance <= 2) {
    return { key: prevKnown.key, confidence: 0.95, reason: "bridged-between-same-sections" };
  }

  if (looksLikeSectionDivider(normText)) {
    if (nextKnown && nextKnown.distance <= 2) {
      return { key: nextKnown.key, confidence: 0.86, reason: "section-divider-before-next-section" };
    }
    if (prevKnown && prevKnown.distance <= 2) {
      return { key: prevKnown.key, confidence: 0.83, reason: "section-divider-after-prev-section" };
    }
  }

  const continuationLike = /^[-–—]/.test(normText) || /^(oraz|a takze|w zwiazku z|z art\.|z dnia)\b/.test(normText);
  if (continuationLike && prevKnown && prevKnown.distance <= 3) {
    return { key: prevKnown.key, confidence: 0.88, reason: "continuation-of-previous-section" };
  }

  const sentencjaLike = looksLikeSentencjaLine(normText);
  const decisionTypeNorm = normalizeText(caseMeta.decisionType || "");
  if (sentencjaLike && position <= 0.35) {
    if (/\b(postanawia|umorzyc|umarza|odmawia|odrzuca|pozostawic bez rozpoznania)\b/.test(normText)) {
      return { key: "postanawia", confidence: 0.9, reason: "sentencja-postanawia-lexical" };
    }
    if (/\b(orzeka|stwierdza)\b/.test(normText)) {
      return { key: "orzeka", confidence: 0.88, reason: "sentencja-orzeka-lexical" };
    }
    if (prevKnown && SENTENCJA_SECTION_KEYS.has(prevKnown.key) && prevKnown.distance <= 4) {
      return { key: prevKnown.key, confidence: 0.84, reason: "sentencja-neighbor-prev" };
    }
    if (nextKnown && SENTENCJA_SECTION_KEYS.has(nextKnown.key) && nextKnown.distance <= 4) {
      return { key: nextKnown.key, confidence: 0.84, reason: "sentencja-neighbor-next" };
    }
    if (decisionTypeNorm.includes("postanow")) {
      return { key: "postanawia", confidence: 0.8, reason: "decision-type-postanowienie" };
    }
    return { key: "tenor", confidence: 0.79, reason: "early-sentencja-default" };
  }

  if (looksLikeReasoningHistory(normText)) {
    if (/\b(na rozprawie|na posiedzeniu|przed rozprawa)\b/.test(normText)) {
      return { key: "uzasadnienie_postepowanie", confidence: 0.84, reason: "history-procedure-cues" };
    }
    return { key: "uzasadnienie_historyczne", confidence: 0.82, reason: "history-cues" };
  }

  if (looksLikeReasoningLegal(normText)) {
    return { key: "uzasadnienie_prawne", confidence: 0.8, reason: "legal-reasoning-cues" };
  }

  if (prevKnown && REASONING_SECTION_KEYS.has(prevKnown.key) && position > 0.2 && prevKnown.distance <= 4) {
    return { key: prevKnown.key, confidence: 0.74, reason: "reasoning-neighbor-prev" };
  }
  if (nextKnown && REASONING_SECTION_KEYS.has(nextKnown.key) && position > 0.2 && nextKnown.distance <= 4) {
    return { key: nextKnown.key, confidence: 0.74, reason: "reasoning-neighbor-next" };
  }

  return null;
}

function reclassifyUnknownParagraphs(paragraphs, caseMeta = {}) {
  const result = paragraphs.map((paragraph) => ({ ...paragraph }));
  const reclassifiedBySection = {};
  const reviewRows = [];
  let reclassifiedCount = 0;

  for (let index = 0; index < result.length; index += 1) {
    const paragraph = result[index];
    if (!paragraph || paragraph.section_key !== "inne") continue;

    const guess = inferUnknownSection(result, index, caseMeta);
    if (!guess || guess.confidence < 0.72) continue;

    const sectionLabel = SECTION_META[guess.key]?.label || guess.key;
    paragraph.section_key = guess.key;
    paragraph.section_label = sectionLabel;
    paragraph.section_confidence = Number(Math.max(paragraph.section_confidence || 0, guess.confidence).toFixed(2));
    paragraph.section_reclassified_from = "inne";
    paragraph.section_reclass_reason = guess.reason;

    reclassifiedCount += 1;
    reclassifiedBySection[guess.key] = (reclassifiedBySection[guess.key] || 0) + 1;
    reviewRows.push({
      paragraph_id: paragraph.paragraph_id,
      paragraph_index: paragraph.paragraph_index,
      paragraph_number: paragraph.paragraph_number || null,
      source_section_key: "inne",
      predicted_section_key: guess.key,
      predicted_section_label: sectionLabel,
      confidence: Number(guess.confidence.toFixed(2)),
      reason: guess.reason,
      section_raw: paragraph.section_raw || null,
      text: paragraph.text
    });
  }

  return {
    paragraphs: result,
    reclassifiedCount,
    reclassifiedBySection,
    reviewRows
  };
}

function collectRemainingUnknownCandidates(cases) {
  const candidates = [];
  for (let caseIndex = 0; caseIndex < cases.length; caseIndex += 1) {
    const caseItem = cases[caseIndex];
    const paragraphs = caseItem?.paragraphs || [];

    for (let paragraphIndex = 0; paragraphIndex < paragraphs.length; paragraphIndex += 1) {
      const paragraph = paragraphs[paragraphIndex];
      if (!paragraph || paragraph.section_key !== "inne") continue;

      const prev = paragraphs[paragraphIndex - 1] || null;
      const next = paragraphs[paragraphIndex + 1] || null;
      const position = paragraphs.length ? Number(((paragraphIndex + 1) / paragraphs.length).toFixed(4)) : 1;

      candidates.push({
        caseIndex,
        paragraphIndex,
        paragraph_id: paragraph.paragraph_id,
        case_signature: caseItem.case_signature,
        document_id: caseItem.document_id,
        decision_type: caseItem.decision_type,
        decision_date: caseItem.decision_date_raw,
        paragraph_number: paragraph.paragraph_number || null,
        position_ratio: position,
        section_raw: paragraph.section_raw || null,
        text: paragraph.text || "",
        previous: prev
          ? {
              section_key: prev.section_key,
              section_label: prev.section_label,
              paragraph_number: prev.paragraph_number || null,
              text: prev.text || ""
            }
          : null,
        next: next
          ? {
              section_key: next.section_key,
              section_label: next.section_label,
              paragraph_number: next.paragraph_number || null,
              text: next.text || ""
            }
          : null
      });
    }
  }
  return candidates;
}

function chunkArray(items, chunkSize) {
  const size = Math.max(1, chunkSize);
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function normalizeLlmPrediction(rawPrediction = {}) {
  const paragraphId = normalizeSpace(rawPrediction.paragraph_id || rawPrediction.id || "");
  const key = normalizeSpace(rawPrediction.predicted_section_key || rawPrediction.section_key || "").toLowerCase();
  const confidence = parseFloatArg(rawPrediction.confidence, 0.5, 0, 1);
  const reason = truncateForLlm(rawPrediction.reason || rawPrediction.rationale || "", 240);

  if (!paragraphId) {
    return null;
  }
  if (!ALL_SECTION_KEYS.includes(key)) {
    return {
      paragraph_id: paragraphId,
      predicted_section_key: "inne",
      confidence: Math.min(0.4, confidence),
      reason: reason || "llm-invalid-section-key"
    };
  }
  return {
    paragraph_id: paragraphId,
    predicted_section_key: key,
    confidence: Number(confidence.toFixed(3)),
    reason: reason || "llm-no-reason"
  };
}

function createLlmSystemPrompt() {
  return [
    "Jesteś asystentem prawnym klasyfikującym akapity orzeczeń polskiego Trybunału Konstytucyjnego.",
    "Dla każdego rekordu wybierz dokładnie jeden klucz sekcji.",
    `Dozwolone klucze: ${ALL_SECTION_KEYS.join(", ")}.`,
    "Użyj treści akapitu, kontekstu (poprzedni i następny akapit), pozycji w dokumencie i języka prawniczego.",
    "Jeżeli nie masz pewności, wybierz 'inne'.",
    "Zwróć wyłącznie poprawny JSON bez markdown:",
    "{\"predictions\":[{\"paragraph_id\":\"...\",\"predicted_section_key\":\"...\",\"confidence\":0.0,\"reason\":\"krótkie uzasadnienie\"}]}",
    "confidence musi być w zakresie 0..1."
  ].join(" ");
}

function createLlmUserPrompt(batch) {
  const payload = batch.map((entry) => ({
    paragraph_id: entry.paragraph_id,
    case_signature: entry.case_signature,
    decision_type: entry.decision_type,
    paragraph_number: entry.paragraph_number,
    position_ratio: entry.position_ratio,
    section_raw: truncateForLlm(entry.section_raw || "", 360),
    text: truncateForLlm(entry.text || "", 1200),
    previous: entry.previous
      ? {
          section_key: entry.previous.section_key,
          paragraph_number: entry.previous.paragraph_number,
          text: truncateForLlm(entry.previous.text || "", 600)
        }
      : null,
    next: entry.next
      ? {
          section_key: entry.next.section_key,
          paragraph_number: entry.next.paragraph_number,
          text: truncateForLlm(entry.next.text || "", 600)
        }
      : null
  }));

  return [
    "Sklasyfikuj każdy rekord i zwróć predictions o identycznej liczbie elementów.",
    "Dane wejściowe JSON:",
    JSON.stringify(payload)
  ].join("\n");
}

async function callOpenAiForBatch({
  apiKey,
  baseUrl,
  model,
  timeoutMs,
  maxRetries,
  systemPrompt,
  userPrompt
}) {
  const endpoint = `${baseUrl.replace(/\/+$/g, "")}/chat/completions`;
  let lastError = null;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          model,
          temperature: 0,
          response_format: { type: "json_object" },
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userPrompt }
          ]
        }),
        signal: controller.signal
      });

      const rawText = await response.text();
      if (!response.ok) {
        const retriable = response.status === 408 || response.status === 409 || response.status === 429 || response.status >= 500;
        const error = new Error(`LLM API error (${response.status}): ${truncateForLlm(rawText, 240)}`);
        if (!retriable || attempt === maxRetries) {
          throw error;
        }
        lastError = error;
        await sleep(500 * attempt);
        continue;
      }

      const payload = safeJsonParse(rawText, null);
      const content = payload?.choices?.[0]?.message?.content;
      if (typeof content !== "string" || !content.trim()) {
        throw new Error("LLM API returned empty content.");
      }
      const parsed = safeJsonParse(stripMarkdownFences(content), null);
      if (!parsed || !Array.isArray(parsed.predictions)) {
        throw new Error("LLM API response JSON does not contain predictions[]");
      }
      return parsed.predictions;
    } catch (error) {
      lastError = error;
      if (attempt === maxRetries) {
        throw lastError;
      }
      await sleep(500 * attempt);
    } finally {
      clearTimeout(timeout);
    }
  }

  throw lastError || new Error("LLM API call failed.");
}

function recomputeCaseSectionCounts(cases) {
  for (const caseItem of cases) {
    const sectionCounts = {};
    for (const paragraph of caseItem.paragraphs || []) {
      sectionCounts[paragraph.section_key] = (sectionCounts[paragraph.section_key] || 0) + 1;
    }
    caseItem.section_counts = sectionCounts;
  }
}

async function runLlmReviewForUnknownSections(cases, options = {}) {
  const apiKey = toNonEmptyString(options.apiKey);
  if (!apiKey) {
    return {
      enabled: false,
      skipped: true,
      reason: "missing_api_key",
      model: toNonEmptyString(options.model, DEFAULT_LLM_MODEL),
      reviewedCount: 0,
      acceptedCount: 0,
      acceptedBySection: {},
      rows: []
    };
  }

  const candidates = collectRemainingUnknownCandidates(cases);
  if (!candidates.length) {
    return {
      enabled: true,
      skipped: false,
      reason: "no_unknown_paragraphs",
      model: toNonEmptyString(options.model, DEFAULT_LLM_MODEL),
      reviewedCount: 0,
      acceptedCount: 0,
      acceptedBySection: {},
      rows: []
    };
  }

  const model = toNonEmptyString(options.model, DEFAULT_LLM_MODEL);
  const batchSize = parseIntegerArg(options.batchSize, DEFAULT_LLM_BATCH_SIZE, 1, 100);
  const minConfidence = parseFloatArg(options.minConfidence, DEFAULT_LLM_MIN_CONFIDENCE, 0, 1);
  const timeoutMs = parseIntegerArg(options.timeoutMs, DEFAULT_LLM_TIMEOUT_MS, 1000, 600000);
  const maxRetries = parseIntegerArg(options.maxRetries, DEFAULT_LLM_MAX_RETRIES, 1, 10);
  const baseUrl = toNonEmptyString(options.baseUrl, DEFAULT_LLM_BASE_URL);
  const batches = chunkArray(candidates, batchSize);

  const acceptedBySection = {};
  const rows = [];
  let acceptedCount = 0;
  let reviewedCount = 0;

  process.stdout.write(`LLM review: ${candidates.length} akapitów 'Inne' w ${batches.length} batchach.\n`);
  for (let batchIndex = 0; batchIndex < batches.length; batchIndex += 1) {
    const batch = batches[batchIndex];
    const systemPrompt = createLlmSystemPrompt();
    const userPrompt = createLlmUserPrompt(batch);
    const predictionsRaw = await callOpenAiForBatch({
      apiKey,
      baseUrl,
      model,
      timeoutMs,
      maxRetries,
      systemPrompt,
      userPrompt
    });
    const predictions = predictionsRaw
      .map((entry) => normalizeLlmPrediction(entry))
      .filter(Boolean);
    const predictionMap = new Map(predictions.map((entry) => [entry.paragraph_id, entry]));

    for (const candidate of batch) {
      reviewedCount += 1;
      const prediction = predictionMap.get(candidate.paragraph_id) || {
        paragraph_id: candidate.paragraph_id,
        predicted_section_key: "inne",
        confidence: 0.01,
        reason: "llm-missing-prediction"
      };

      const accepted = prediction.predicted_section_key !== "inne" && prediction.confidence >= minConfidence;
      if (accepted) {
        const targetCase = cases[candidate.caseIndex];
        const targetParagraph = targetCase?.paragraphs?.[candidate.paragraphIndex];
        if (targetParagraph) {
          targetParagraph.section_key = prediction.predicted_section_key;
          targetParagraph.section_label = SECTION_META[prediction.predicted_section_key]?.label || prediction.predicted_section_key;
          targetParagraph.section_confidence = Number(Math.max(targetParagraph.section_confidence || 0, prediction.confidence).toFixed(2));
          targetParagraph.section_reclassified_from = "inne";
          targetParagraph.section_reclass_reason = `llm:${prediction.reason}`;
          acceptedCount += 1;
          acceptedBySection[prediction.predicted_section_key] = (acceptedBySection[prediction.predicted_section_key] || 0) + 1;
        }
      }

      rows.push({
        case_signature: candidate.case_signature,
        document_id: candidate.document_id,
        paragraph_id: candidate.paragraph_id,
        paragraph_index: candidate.paragraphIndex + 1,
        paragraph_number: candidate.paragraph_number || null,
        source_section_key: "inne",
        predicted_section_key: prediction.predicted_section_key,
        confidence: Number(prediction.confidence.toFixed(3)),
        accepted,
        reason: prediction.reason,
        model
      });
    }

    process.stdout.write(`LLM review: batch ${batchIndex + 1}/${batches.length}, zaakceptowano łącznie ${acceptedCount}.\n`);
  }

  recomputeCaseSectionCounts(cases);
  return {
    enabled: true,
    skipped: false,
    reason: "ok",
    model,
    reviewedCount,
    acceptedCount,
    acceptedBySection,
    rows
  };
}

function pickForcedFallbackSection(paragraphs, paragraphIndex, caseItem) {
  const paragraph = paragraphs[paragraphIndex];
  const textNorm = normalizeText(paragraph?.text || "");
  const decisionTypeNorm = normalizeText(caseItem?.decision_type || "");
  const totalParagraphs = Math.max(1, paragraphs.length);
  const position = (paragraphIndex + 1) / totalParagraphs;

  if (textNorm.startsWith("zdanie odrebne") || /sedziego\s+tk/.test(textNorm)) {
    return { key: "zdanie_odrebne", confidence: 0.78, reason: "forced:dissent-lexical" };
  }

  const guess = inferUnknownSection(paragraphs, paragraphIndex, { decisionType: caseItem?.decision_type });
  if (guess?.key && guess.key !== "inne") {
    return {
      key: guess.key,
      confidence: Number(Math.max(0.72, guess.confidence || 0.72).toFixed(2)),
      reason: `forced:${guess.reason || "infer"}`
    };
  }

  const prevKnown = findNeighborKnownSection(paragraphs, paragraphIndex, -1, 30);
  const nextKnown = findNeighborKnownSection(paragraphs, paragraphIndex, +1, 30);
  if (prevKnown && nextKnown) {
    const prevScore = prevKnown.distance + (SENTENCJA_SECTION_KEYS.has(prevKnown.key) && position <= 0.35 ? -1 : 0);
    const nextScore = nextKnown.distance + (SENTENCJA_SECTION_KEYS.has(nextKnown.key) && position <= 0.35 ? -1 : 0);
    const chosen = prevScore <= nextScore ? prevKnown : nextKnown;
    return { key: chosen.key, confidence: 0.7, reason: "forced:nearest-neighbor" };
  }
  if (prevKnown) {
    return { key: prevKnown.key, confidence: 0.68, reason: "forced:prev-neighbor" };
  }
  if (nextKnown) {
    return { key: nextKnown.key, confidence: 0.68, reason: "forced:next-neighbor" };
  }

  if (position <= 0.35) {
    if (decisionTypeNorm.includes("postanow")) {
      return { key: "postanawia", confidence: 0.65, reason: "forced:early-postanowienie" };
    }
    return { key: "tenor", confidence: 0.65, reason: "forced:early-default-tenor" };
  }

  if (looksLikeReasoningLegal(textNorm)) {
    return { key: "uzasadnienie_prawne", confidence: 0.64, reason: "forced:legal-cues" };
  }
  if (/\b(na rozprawie|na posiedzeniu|przed rozprawa)\b/.test(textNorm)) {
    return { key: "uzasadnienie_postepowanie", confidence: 0.64, reason: "forced:procedure-cues" };
  }
  if (looksLikeReasoningHistory(textNorm)) {
    return { key: "uzasadnienie_historyczne", confidence: 0.64, reason: "forced:history-cues" };
  }

  if (position > 0.8) {
    return { key: "uzasadnienie_prawne", confidence: 0.62, reason: "forced:late-default-legal" };
  }
  return { key: "uzasadnienie_historyczne", confidence: 0.62, reason: "forced:default-history" };
}

function forceAssignRemainingUnknownSections(cases) {
  const rows = [];
  const acceptedBySection = {};
  let acceptedCount = 0;

  for (let caseIndex = 0; caseIndex < cases.length; caseIndex += 1) {
    const caseItem = cases[caseIndex];
    for (let paragraphIndex = 0; paragraphIndex < (caseItem.paragraphs || []).length; paragraphIndex += 1) {
      const paragraph = caseItem.paragraphs[paragraphIndex];
      if (!paragraph || paragraph.section_key !== "inne") continue;

      const forced = pickForcedFallbackSection(caseItem.paragraphs, paragraphIndex, caseItem);
      if (!forced?.key || forced.key === "inne") continue;

      paragraph.section_key = forced.key;
      paragraph.section_label = SECTION_META[forced.key]?.label || forced.key;
      paragraph.section_confidence = Number(Math.max(paragraph.section_confidence || 0, forced.confidence || 0.62).toFixed(2));
      paragraph.section_reclassified_from = "inne";
      paragraph.section_reclass_reason = forced.reason;

      acceptedCount += 1;
      acceptedBySection[forced.key] = (acceptedBySection[forced.key] || 0) + 1;
      rows.push({
        case_signature: caseItem.case_signature,
        document_id: caseItem.document_id,
        paragraph_id: paragraph.paragraph_id,
        paragraph_index: paragraph.paragraph_index,
        paragraph_number: paragraph.paragraph_number || null,
        source_section_key: "inne",
        predicted_section_key: forced.key,
        confidence: Number(forced.confidence.toFixed(3)),
        accepted: true,
        reason: forced.reason,
        model: "forced-fallback"
      });
    }
  }

  recomputeCaseSectionCounts(cases);
  return {
    acceptedCount,
    acceptedBySection,
    rows
  };
}

function deriveBenchInfo(judges = []) {
  const count = (judges || []).filter(Boolean).length;
  if (count >= 11) return { count, key: "pelny_sklad", label: "Pełny skład", isFullBench: true };
  if (count === 5) return { count, key: "piecioosobowa", label: "Pięcioosobowa", isFullBench: false };
  if (count === 3) return { count, key: "trojosobowa", label: "Trójosobowa", isFullBench: false };
  if (count > 0) return { count, key: `${count}_osobowa`, label: `${count}-osobowa`, isFullBench: false };
  return { count: 0, key: "nieustalony", label: "Nieustalony", isFullBench: false };
}

function deriveIpoDecisionTypeInfo(decisionType) {
  const norm = normalizeText(decisionType);
  if (!norm) {
    return {
      key: null,
      label: null,
      visible: false
    };
  }

  if (norm.includes("postanow") && norm.includes("tymczas")) {
    const entry = IPO_DECISION_TYPE_BY_KEY.get("postanowienie_tymczasowe");
    return { key: entry.key, label: entry.label, visible: true };
  }
  if (norm.includes("rozstrzygn")) {
    const entry = IPO_DECISION_TYPE_BY_KEY.get("rozstrzygniecie");
    return { key: entry.key, label: entry.label, visible: true };
  }
  if (norm.includes("wyrok")) {
    const entry = IPO_DECISION_TYPE_BY_KEY.get("wyrok");
    return { key: entry.key, label: entry.label, visible: true };
  }
  if (norm.includes("postanow")) {
    const entry = IPO_DECISION_TYPE_BY_KEY.get("postanowienie");
    return { key: entry.key, label: entry.label, visible: true };
  }

  return {
    key: null,
    label: null,
    visible: false
  };
}

function deriveIpoBenchInfo(benchInfo) {
  const key = normalizeSpace(benchInfo?.key);
  const matched = IPO_BENCH_BY_KEY.get(key);
  if (matched) {
    return {
      key: matched.key,
      label: matched.label,
      visible: true
    };
  }
  return {
    key: "poza_klasyfikacja_ipo",
    label: "Poza klasyfikacją IPO",
    visible: false
  };
}

function deriveIpoBenchInfoFromKey(rawKey) {
  const key = normalizeSpace(rawKey);
  const matched = IPO_BENCH_BY_KEY.get(key);
  if (matched) {
    return {
      key: matched.key,
      label: matched.label,
      visible: true
    };
  }
  return {
    key: "poza_klasyfikacja_ipo",
    label: "Poza klasyfikacją IPO",
    visible: false
  };
}

async function loadIpoBenchOverrides(mapPath) {
  if (!mapPath) return new Map();
  const resolved = path.resolve(mapPath);
  if (!(await fileExists(resolved))) return new Map();

  const content = await fs.readFile(resolved, "utf8");
  const parsed = JSON.parse(content);
  const byDocumentId = parsed?.by_document_id;
  if (!byDocumentId || typeof byDocumentId !== "object") return new Map();

  const overrides = new Map();
  for (const [documentId, benchKey] of Object.entries(byDocumentId)) {
    const normalizedDocumentId = normalizeSpace(documentId);
    const normalizedBenchKey = normalizeSpace(benchKey);
    if (!normalizedDocumentId || !normalizedBenchKey) continue;
    if (!IPO_BENCH_BY_KEY.has(normalizedBenchKey)) continue;
    overrides.set(normalizedDocumentId, normalizedBenchKey);
  }
  return overrides;
}

function canonicalIpoCaseUrl(sourceUrl, caseId, documentId) {
  const caseValue = normalizeSpace(caseId);
  const documentValue = normalizeSpace(documentId);
  if (caseValue && documentValue) {
    return `https://ipo.trybunal.gov.pl/ipo/Sprawa?dokument=${encodeURIComponent(documentValue)}&sprawa=${encodeURIComponent(caseValue)}`;
  }
  return normalizeSpace(sourceUrl) || null;
}

function canonicalIpoDownloadUrl(downloadUrl, documentId) {
  const documentValue = normalizeSpace(documentId);
  if (documentValue) {
    return `https://ipo.trybunal.gov.pl/ipo/downloadOrzeczenieDoc?dok=${encodeURIComponent(documentValue)}`;
  }
  return normalizeSpace(downloadUrl) || null;
}

function pickInputFile(inputArg) {
  if (inputArg) {
    return path.resolve(inputArg);
  }
  return path.resolve("output/playwright/tk-all.decisions.json");
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

function normalizeDecision(decision, index, diagnosticsCollector = null, options = {}) {
  const caseId = decision?.ids?.case_id || null;
  const documentId = decision?.ids?.document_id || null;
  const signature = normalizeSpace(decision?.case_signature) || `case-${index + 1}`;
  const decisionType = normalizeSpace(decision?.decision_type) || "Nieustalony typ";
  const topic = normalizeSpace(decision?.topic) || null;
  const proceedingIntro = normalizeSpace(decision?.proceeding_intro) || null;
  const dateRaw = normalizeSpace(decision?.decision_date) || null;
  const parsedDate = parsePolishDate(dateRaw);

  const judges = (decision?.judges || [])
    .map((judge) => ({
      name: normalizeSpace(judge?.name),
      role: normalizeSpace(judge?.role) || null
    }))
    .filter((judge) => judge.name);
  const judgeNames = judges.map((judge) => judge.name);
  const benchInfo = deriveBenchInfo(judgeNames);
  const overrideBenchKey = options?.ipoBenchOverrides instanceof Map
    ? options.ipoBenchOverrides.get(normalizeSpace(documentId))
    : null;
  const ipoBenchInfo = overrideBenchKey
    ? deriveIpoBenchInfoFromKey(overrideBenchKey)
    : deriveIpoBenchInfo(benchInfo);
  const ipoDecisionTypeInfo = deriveIpoDecisionTypeInfo(decisionType);

  const tableOfContents = (decision?.table_of_contents || [])
    .map((item) => ({
      text: normalizeSpace(item?.text),
      href: normalizeSpace(item?.href) || null,
      key: classifySectionFromText(item?.text),
      label: SECTION_META[classifySectionFromText(item?.text)]?.label || SECTION_META.inne.label
    }))
    .filter((item) => item.text);

  const romanMap = buildRomanSectionMap(tableOfContents);

  const rawParagraphs = (decision?.paragraphs || [])
    .map((paragraph, pIndex) => {
      const text = normalizeSpace(paragraph?.text);
      if (!text) return null;

      const section = resolveSection(paragraph, romanMap);
      let sectionKey = section.key;
      let sectionConfidence = section.confidence;
      if (sectionKey === "inne" && looksLikeTenorParagraph(text, paragraph?.paragraph_number, pIndex + 1)) {
        sectionKey = "tenor";
        sectionConfidence = 0.72;
      }

      return {
        paragraph_id: `${documentId || "doc"}-${pIndex + 1}`,
        paragraph_index: paragraph?.paragraph_index || pIndex + 1,
        paragraph_number: normalizeSpace(paragraph?.paragraph_number) || null,
        section_key: sectionKey,
        section_label: SECTION_META[sectionKey]?.label || SECTION_META.inne.label,
        section_confidence: Number(sectionConfidence.toFixed(2)),
        section_raw: section.raw || null,
        text
      };
    })
    .filter(Boolean);

  const mergedParagraphs = mergeSplitDissentHeadings(rawParagraphs);
  const reclassified = reclassifyUnknownParagraphs(mergedParagraphs, {
    decisionType
  });
  const paragraphs = reclassified.paragraphs;

  const sectionCounts = {};
  for (const paragraph of paragraphs) {
    sectionCounts[paragraph.section_key] = (sectionCounts[paragraph.section_key] || 0) + 1;
  }

  if (diagnosticsCollector) {
    diagnosticsCollector.reclassifiedFromInneCount = (diagnosticsCollector.reclassifiedFromInneCount || 0) + reclassified.reclassifiedCount;
    diagnosticsCollector.reclassifiedFromInneBySection = diagnosticsCollector.reclassifiedFromInneBySection || {};
    for (const [key, count] of Object.entries(reclassified.reclassifiedBySection)) {
      diagnosticsCollector.reclassifiedFromInneBySection[key] = (diagnosticsCollector.reclassifiedFromInneBySection[key] || 0) + count;
    }
    diagnosticsCollector.inneReviewRows = diagnosticsCollector.inneReviewRows || [];
    if (reclassified.reviewRows.length) {
      const decisionRows = reclassified.reviewRows.map((row) => ({
        case_signature: signature,
        document_id: documentId,
        decision_type: decisionType,
        decision_date: dateRaw,
        ...row
      }));
      diagnosticsCollector.inneReviewRows.push(...decisionRows);
    }
  }

  return {
    case_id: caseId,
    document_id: documentId,
    case_signature: signature,
    decision_type: decisionType,
    decision_type_ipo_key: ipoDecisionTypeInfo.key,
    decision_type_ipo_label: ipoDecisionTypeInfo.label,
    decision_type_ipo_visible: ipoDecisionTypeInfo.visible,
    decision_date_raw: dateRaw,
    decision_date_iso: parsedDate.iso,
    year: parsedDate.year,
    topic,
    proceeding_intro: proceedingIntro,
    source_url: canonicalIpoCaseUrl(decision?.source_url, caseId, documentId),
    download_url: canonicalIpoDownloadUrl(decision?.download?.href, documentId),
    publication_entries: decision?.publication_entries || [],
    metadata: decision?.metadata || {},
    judges,
    judge_names: judgeNames,
    judge_count: benchInfo.count,
    bench_size_key: benchInfo.key,
    bench_size_label: benchInfo.label,
    bench_size_ipo_key: ipoBenchInfo.key,
    bench_size_ipo_label: ipoBenchInfo.label,
    bench_size_ipo_visible: ipoBenchInfo.visible,
    is_full_bench: ipoBenchInfo.key === "pelny_sklad" ? true : benchInfo.isFullBench,
    table_of_contents: tableOfContents,
    paragraph_count: paragraphs.length,
    section_counts: sectionCounts,
    normalization_version: NORMALIZATION_VERSION,
    paragraphs
  };
}

function validateNormalizedCase(caseItem) {
  let errors = 0;
  if (!caseItem || typeof caseItem !== "object") return 1;
  if (!caseItem.case_signature) errors += 1;
  if (!caseItem.document_id) errors += 1;
  if (!Array.isArray(caseItem.paragraphs) || !caseItem.paragraphs.length) errors += 1;

  for (const paragraph of caseItem.paragraphs || []) {
    if (!paragraph.paragraph_id) errors += 1;
    if (!paragraph.section_key) errors += 1;
    if (!paragraph.text) errors += 1;
    if (!Number.isFinite(paragraph.section_confidence)) errors += 1;
  }
  return errors;
}

function topEntries(counter, limit = 20) {
  return Object.entries(counter)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], "pl"))
    .slice(0, limit);
}

function buildStats(cases, sourceFile, diagnostics = {}) {
  const decisionTypeCounts = {};
  const decisionTypeIpoCounts = Object.fromEntries(IPO_DECISION_TYPE_META.map((entry) => [entry.label, 0]));
  const sectionCounts = {};
  const benchSizeIpoCounts = Object.fromEntries(IPO_BENCH_META.map((entry) => [entry.label, 0]));
  const yearCounts = {};
  const topicCounts = {};
  const judgeCounts = {};
  const bigramCounts = new Map();
  const judgePairCounts = new Map();
  let totalParagraphs = 0;
  let datedCases = 0;
  let unknownSections = 0;
  let benchOutsideIpoCount = 0;

  for (const caseItem of cases) {
    totalParagraphs += caseItem.paragraph_count || 0;

    decisionTypeCounts[caseItem.decision_type] = (decisionTypeCounts[caseItem.decision_type] || 0) + 1;
    if (caseItem.decision_type_ipo_visible && caseItem.decision_type_ipo_label) {
      decisionTypeIpoCounts[caseItem.decision_type_ipo_label] = (decisionTypeIpoCounts[caseItem.decision_type_ipo_label] || 0) + 1;
    }
    if (caseItem.bench_size_ipo_visible && caseItem.bench_size_ipo_label) {
      benchSizeIpoCounts[caseItem.bench_size_ipo_label] = (benchSizeIpoCounts[caseItem.bench_size_ipo_label] || 0) + 1;
    } else {
      benchOutsideIpoCount += 1;
    }

    if (caseItem.year) {
      yearCounts[String(caseItem.year)] = (yearCounts[String(caseItem.year)] || 0) + 1;
      datedCases += 1;
    }

    if (caseItem.topic) {
      topicCounts[caseItem.topic] = (topicCounts[caseItem.topic] || 0) + 1;
    }

    const uniqueJudges = [...new Set((caseItem.judge_names || []).map((judge) => normalizeSpace(judge)).filter(Boolean))];
    for (const judge of uniqueJudges) {
      judgeCounts[judge] = (judgeCounts[judge] || 0) + 1;
    }
    for (let i = 0; i < uniqueJudges.length; i += 1) {
      for (let j = i + 1; j < uniqueJudges.length; j += 1) {
        const pairKey = judgePairKey(uniqueJudges[i], uniqueJudges[j]);
        if (!pairKey) continue;
        judgePairCounts.set(pairKey, (judgePairCounts.get(pairKey) || 0) + 1);
      }
    }

    for (const paragraph of caseItem.paragraphs || []) {
      sectionCounts[paragraph.section_key] = (sectionCounts[paragraph.section_key] || 0) + 1;
      if (paragraph.section_key === "inne") unknownSections += 1;

      const tokens = tokenizeForBigrams(paragraph.text);
      for (let idx = 0; idx < tokens.length - 1; idx += 1) {
        const current = tokens[idx];
        const next = tokens[idx + 1];
        const rawKey = `${current.norm} ${next.norm}`;
        const canonical = canonicalizeBigram(rawKey, `${current.token} ${next.token}`);
        const key = canonical.key;
        const existing = bigramCounts.get(key);
        if (existing) {
          existing.count += 1;
          if (canonical.label && existing.label !== canonical.label) {
            existing.label = canonical.label;
          }
        } else {
          bigramCounts.set(key, {
            label: canonical.label || `${current.token} ${next.token}`,
            count: 1
          });
        }
      }
    }
  }

  const years = Object.keys(yearCounts).map((value) => Number(value)).filter(Number.isFinite);
  years.sort((a, b) => a - b);

  const filteredBigramEntries = [];
  const removedBigramEntries = [];
  let removedBigramsTotalCount = 0;
  for (const [key, entry] of bigramCounts.entries()) {
    if (BIGRAM_NOISE_PHRASES.has(key)) {
      removedBigramsTotalCount += entry.count;
      removedBigramEntries.push([entry.label, entry.count]);
      continue;
    }
    filteredBigramEntries.push(entry);
  }

  const topBigrams = filteredBigramEntries
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label, "pl"))
    .slice(0, 80)
    .map((entry) => [entry.label, entry.count]);
  const removedBigrams = removedBigramEntries
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], "pl"))
    .slice(0, 40);

  const topJudgePairs = [...judgePairCounts.entries()]
    .map(([key, count]) => {
      const [left, right] = key.split("|||");
      return {
        left,
        right,
        label: `${left} × ${right}`,
        count
      };
    })
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label, "pl"));

  const heatmapJudges = topEntries(judgeCounts, 14).map(([name]) => name);
  const heatmapMatrix = heatmapJudges.map((rowJudge) => (
    heatmapJudges.map((colJudge) => {
      if (rowJudge === colJudge) return judgeCounts[rowJudge] || 0;
      return judgePairCounts.get(judgePairKey(rowJudge, colJudge)) || 0;
    })
  ));

  return {
    generated_at: new Date().toISOString(),
    source_file: sourceFile,
    schema_version: SCHEMA_VERSION,
    summary: {
      total_cases: cases.length,
      dated_cases: datedCases,
      undated_cases: cases.length - datedCases,
      total_paragraphs: totalParagraphs,
      avg_paragraphs_per_case: cases.length ? Number((totalParagraphs / cases.length).toFixed(2)) : 0,
      unique_decision_types: Object.keys(decisionTypeCounts).length,
      unique_decision_types_ipo: Object.keys(decisionTypeIpoCounts).length,
      unique_judges: Object.keys(judgeCounts).length,
      date_range: years.length
        ? {
            from: years[0],
            to: years[years.length - 1]
          }
        : null
    },
    series: {
      cases_by_year: years.map((year) => [year, yearCounts[String(year)] || 0])
    },
    rankings: {
      decision_types: topEntries(decisionTypeCounts, 20),
      decision_types_ipo: IPO_DECISION_TYPE_META.map((entry) => [entry.label, decisionTypeIpoCounts[entry.label] || 0]),
      bench_sizes_ipo: IPO_BENCH_META.map((entry) => [entry.label, benchSizeIpoCounts[entry.label] || 0]),
      sections: topEntries(sectionCounts, 20).map(([key, count]) => [SECTION_META[key]?.label || key, count]),
      topics: topEntries(topicCounts, 20),
      judges: topEntries(judgeCounts, 25),
      bigrams: topBigrams,
      bigrams_removed: removedBigrams,
      bigrams_filtering: {
        disclaimer: BIGRAMS_FILTERING_DISCLAIMER,
        removed_total_count: removedBigramsTotalCount,
        removed_unique_count: removedBigramEntries.length,
        removed_phrase_catalog: [...BIGRAM_NOISE_PHRASES_RAW]
      },
      judge_pairs: topJudgePairs.slice(0, 50).map((entry) => [entry.label, entry.count])
    },
    cooccurrence: {
      judges: heatmapJudges,
      matrix: heatmapMatrix,
      diagonal_label: "liczba spraw z udziałem sędziego",
      off_diagonal_label: "liczba spraw, w których para sędziów orzekała razem"
    },
    quality: {
      schema_version: SCHEMA_VERSION,
      section_unknown_share: totalParagraphs ? Number((unknownSections / totalParagraphs).toFixed(6)) : 0,
      validation_errors_count: diagnostics.validationErrorsCount || 0,
      dropped_invalid_cases_count: diagnostics.droppedInvalidCasesCount || 0,
      section_reclassified_from_inne_count: diagnostics.reclassifiedFromInneCount || 0,
      section_reclassified_from_inne_by_section: diagnostics.reclassifiedFromInneBySection || {},
      section_reclassified_by_llm_count: diagnostics.reclassifiedByLlmCount || 0,
      section_reclassified_by_llm_by_section: diagnostics.reclassifiedByLlmBySection || {},
      section_reclassified_by_forced_count: diagnostics.reclassifiedByForcedCount || 0,
      section_reclassified_by_forced_by_section: diagnostics.reclassifiedByForcedBySection || {},
      llm_review_model: diagnostics.llmReviewModel || null,
      llm_review_attempted: Boolean(diagnostics.llmReviewAttempted),
      force_no_inne_applied: Boolean(diagnostics.forceNoInneApplied),
      bench_outside_ipo_count: benchOutsideIpoCount
    },
    section_meta: SECTION_META
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const inputFile = pickInputFile(args.input);
  const outputDir = path.resolve(args.outdir || "docs/data");
  const jsonlPath = path.join(outputDir, args.jsonl || "tk_cases.jsonl");
  const sample50Path = path.join(outputDir, args.sample || args.sample50 || "tk_cases_sample50.jsonl");
  const sample200Path = path.join(outputDir, args.sample200 || "tk_cases_sample200.jsonl");
  const statsPath = path.join(outputDir, args.stats || "stats.json");
  const inneReviewPath = args.inne_report
    ? path.resolve(args.inne_report)
    : null;
  const llmReviewInne = parseBooleanArg(args.llm_review_inne, false);
  const llmModel = toNonEmptyString(args.llm_model, DEFAULT_LLM_MODEL);
  const llmBatchSize = parseIntegerArg(args.llm_batch_size, DEFAULT_LLM_BATCH_SIZE, 1, 100);
  const llmMinConfidence = parseFloatArg(args.llm_min_confidence, DEFAULT_LLM_MIN_CONFIDENCE, 0, 1);
  const llmTimeoutMs = parseIntegerArg(args.llm_timeout_ms, DEFAULT_LLM_TIMEOUT_MS, 1000, 600000);
  const llmMaxRetries = parseIntegerArg(args.llm_max_retries, DEFAULT_LLM_MAX_RETRIES, 1, 10);
  const llmBaseUrl = toNonEmptyString(args.llm_base_url || process.env.OPENAI_BASE_URL, DEFAULT_LLM_BASE_URL);
  const llmApiKey = toNonEmptyString(args.llm_api_key || process.env.OPENAI_API_KEY || "");
  const forceNoInne = parseBooleanArg(args.force_no_inne, false);
  const ipoBenchMapPath = args.ipo_bench_map ? path.resolve(args.ipo_bench_map) : DEFAULT_IPO_BENCH_MAP_PATH;

  if (!(await fileExists(inputFile))) {
    throw new Error(`Input file not found: ${inputFile}`);
  }

  const rawContent = await fs.readFile(inputFile, "utf8");
  const raw = JSON.parse(rawContent);
  if (!Array.isArray(raw)) {
    throw new Error("Input must be a JSON array of decisions.");
  }
  const ipoBenchOverrides = await loadIpoBenchOverrides(ipoBenchMapPath);
  if (ipoBenchOverrides.size) {
    process.stdout.write(`Wczytano mapę składu IPO: ${ipoBenchOverrides.size} dokumentów (${ipoBenchMapPath}).\n`);
  } else if (args.ipo_bench_map) {
    process.stdout.write(`Mapa składu IPO nie zawiera nadpisań lub brak pliku: ${ipoBenchMapPath}. Używam fallbacku heurystycznego.\n`);
  }

  const diagnosticsCollector = {
    reclassifiedFromInneCount: 0,
    reclassifiedFromInneBySection: {},
    inneReviewRows: [],
    llmReviewAttempted: false,
    llmReviewModel: null,
    reclassifiedByLlmCount: 0,
    reclassifiedByLlmBySection: {},
    llmReviewRows: [],
    llmReviewSkippedReason: null,
    forceNoInneApplied: false,
    reclassifiedByForcedCount: 0,
    reclassifiedByForcedBySection: {},
    forcedRows: [],
    droppedInvalidCasesCount: 0,
    droppedInvalidCases: []
  };
  const normalized = raw.map((decision, index) => normalizeDecision(decision, index, diagnosticsCollector, {
    ipoBenchOverrides
  }));

  if (llmReviewInne) {
    diagnosticsCollector.llmReviewAttempted = true;
    diagnosticsCollector.llmReviewModel = llmModel;
    if (!llmApiKey) {
      diagnosticsCollector.llmReviewSkippedReason = "missing_api_key";
      process.stdout.write("LLM review pominięty: brak OPENAI_API_KEY / --llm_api_key.\n");
    } else {
      const llmReviewResult = await runLlmReviewForUnknownSections(normalized, {
        apiKey: llmApiKey,
        baseUrl: llmBaseUrl,
        model: llmModel,
        batchSize: llmBatchSize,
        minConfidence: llmMinConfidence,
        timeoutMs: llmTimeoutMs,
        maxRetries: llmMaxRetries
      });
      diagnosticsCollector.reclassifiedByLlmCount = llmReviewResult.acceptedCount || 0;
      diagnosticsCollector.reclassifiedByLlmBySection = llmReviewResult.acceptedBySection || {};
      diagnosticsCollector.llmReviewRows = llmReviewResult.rows || [];
      diagnosticsCollector.llmReviewSkippedReason = llmReviewResult.skipped ? llmReviewResult.reason : null;
      process.stdout.write(
        `LLM review zakończony: przeanalizowano ${llmReviewResult.reviewedCount || 0}, zaakceptowano ${llmReviewResult.acceptedCount || 0}.\n`
      );
    }
  }

  if (forceNoInne) {
    diagnosticsCollector.forceNoInneApplied = true;
    const forcedResult = forceAssignRemainingUnknownSections(normalized);
    diagnosticsCollector.reclassifiedByForcedCount = forcedResult.acceptedCount || 0;
    diagnosticsCollector.reclassifiedByForcedBySection = forcedResult.acceptedBySection || {};
    diagnosticsCollector.forcedRows = forcedResult.rows || [];
    process.stdout.write(`Force-no-inne: przypisano ${forcedResult.acceptedCount || 0} pozostałych akapitów.\n`);
  }

  const normalizedValid = [];
  const droppedCases = [];
  for (const caseItem of normalized) {
    const hasParagraphs = Array.isArray(caseItem?.paragraphs) && caseItem.paragraphs.length > 0;
    if (!hasParagraphs) {
      droppedCases.push({
        case_signature: caseItem?.case_signature || null,
        document_id: caseItem?.document_id || null,
        source_url: caseItem?.source_url || null,
        reason: "empty_paragraphs"
      });
      continue;
    }
    normalizedValid.push(caseItem);
  }
  diagnosticsCollector.droppedInvalidCasesCount = droppedCases.length;
  diagnosticsCollector.droppedInvalidCases = droppedCases.slice(0, 100);
  if (droppedCases.length) {
    process.stdout.write(`Odrzucono ${droppedCases.length} rekordów bez akapitów (empty_paragraphs).\n`);
  }

  const validationErrorsCount = normalizedValid.reduce((sum, caseItem) => sum + validateNormalizedCase(caseItem), 0);
  const datasetGeneratedAt = new Date().toISOString();
  const digestSource = normalizedValid.map((item) => JSON.stringify(item)).join("\n");
  const datasetHash = createHash("sha256").update(digestSource, "utf8").digest("hex");
  const finalized = normalizedValid.map((item) => ({
    ...item,
    dataset_hash: datasetHash,
    dataset_generated_at: datasetGeneratedAt
  }));

  await fs.mkdir(outputDir, { recursive: true });

  const jsonl = finalized.map((item) => JSON.stringify(item)).join("\n");
  const sample50Jsonl = finalized.slice(0, 50).map((item) => JSON.stringify(item)).join("\n");
  const sample200Jsonl = finalized.slice(0, 200).map((item) => JSON.stringify(item)).join("\n");
  await fs.writeFile(jsonlPath, `${jsonl}\n`, "utf8");
  await fs.writeFile(sample50Path, `${sample50Jsonl}\n`, "utf8");
  await fs.writeFile(sample200Path, `${sample200Jsonl}\n`, "utf8");

  const stats = buildStats(finalized, inputFile, {
    validationErrorsCount,
    droppedInvalidCasesCount: diagnosticsCollector.droppedInvalidCasesCount,
    reclassifiedFromInneCount: diagnosticsCollector.reclassifiedFromInneCount,
    reclassifiedFromInneBySection: diagnosticsCollector.reclassifiedFromInneBySection,
    reclassifiedByLlmCount: diagnosticsCollector.reclassifiedByLlmCount,
    reclassifiedByLlmBySection: diagnosticsCollector.reclassifiedByLlmBySection,
    reclassifiedByForcedCount: diagnosticsCollector.reclassifiedByForcedCount,
    reclassifiedByForcedBySection: diagnosticsCollector.reclassifiedByForcedBySection,
    llmReviewModel: diagnosticsCollector.llmReviewModel,
    llmReviewAttempted: diagnosticsCollector.llmReviewAttempted,
    forceNoInneApplied: diagnosticsCollector.forceNoInneApplied
  });
  stats.dataset_hash = datasetHash;
  stats.dataset_generated_at = datasetGeneratedAt;
  stats.normalization_version = NORMALIZATION_VERSION;
  await fs.writeFile(statsPath, `${JSON.stringify(stats, null, 2)}\n`, "utf8");

  if (inneReviewPath) {
    await fs.mkdir(path.dirname(inneReviewPath), { recursive: true });
    const reviewPayload = {
      generated_at: new Date().toISOString(),
      source_file: inputFile,
      records: diagnosticsCollector.inneReviewRows,
      llm_review: {
        attempted: diagnosticsCollector.llmReviewAttempted,
        model: diagnosticsCollector.llmReviewModel,
        skipped_reason: diagnosticsCollector.llmReviewSkippedReason,
        accepted_count: diagnosticsCollector.reclassifiedByLlmCount,
        accepted_by_section: diagnosticsCollector.reclassifiedByLlmBySection,
        records: diagnosticsCollector.llmReviewRows
      },
      forced_review: {
        applied: diagnosticsCollector.forceNoInneApplied,
        accepted_count: diagnosticsCollector.reclassifiedByForcedCount,
        accepted_by_section: diagnosticsCollector.reclassifiedByForcedBySection,
        records: diagnosticsCollector.forcedRows
      },
      dropped_invalid_cases: {
        count: diagnosticsCollector.droppedInvalidCasesCount,
        records: diagnosticsCollector.droppedInvalidCases
      }
    };
    await fs.writeFile(inneReviewPath, `${JSON.stringify(reviewPayload, null, 2)}\n`, "utf8");
  }

  process.stdout.write(`Saved ${finalized.length} normalized cases to: ${jsonlPath}\n`);
  process.stdout.write(`Saved 50-case sample dataset to: ${sample50Path}\n`);
  process.stdout.write(`Saved 200-case sample dataset to: ${sample200Path}\n`);
  process.stdout.write(`Saved stats payload to: ${statsPath}\n`);
  if (inneReviewPath) {
    process.stdout.write(`Saved INNE reclassification review to: ${inneReviewPath}\n`);
  }
}

main().catch((error) => {
  process.stderr.write(`Fatal error: ${error?.message || error}\n`);
  process.exit(1);
});
