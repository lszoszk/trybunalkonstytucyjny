#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const SCHEMA_VERSION = "tk-similarity-v1";
const SIMILARITY_VERSION = "tk-sim-hybrid-v2-dotyczy";

const DEFAULT_INPUT = path.resolve("docs/data/tk_cases.jsonl");
const DEFAULT_OUTDIR = path.resolve("docs/data");
const DEFAULT_OUTPUT = "similar_cases.json";
const DEFAULT_SAMPLE50_DATASET = path.resolve("docs/data/tk_cases_sample50.jsonl");
const DEFAULT_SAMPLE200_DATASET = path.resolve("docs/data/tk_cases_sample200.jsonl");
const DEFAULT_SAMPLE50_OUTPUT = "similar_cases_sample50.json";
const DEFAULT_SAMPLE200_OUTPUT = "similar_cases_sample200.json";
const DEFAULT_AUDIT_PATH = path.resolve("output/analysis/similarity_audit.json");

const DEFAULT_TOP_K = 5;
const DEFAULT_THRESHOLD = 0.12;
const EPS = 1e-9;

const SECTION_WEIGHTS = {
  tenor: 1.35,
  orzeka: 1.3,
  postanawia: 1.3,
  uzasadnienie_prawne: 1.25,
  uzasadnienie_postepowanie: 0.95,
  uzasadnienie_historyczne: 0.9,
  uzasadnienie_ogolne: 0.85,
  zdanie_odrebne: 0.8,
  komparycja: 0.6,
  sentencja_inna: 0.6,
  inne: 0.6
};

const SECTION_CAPS = {
  sentencja: 1200,
  uzasadnienie_prawne: 5000,
  uzasadnienie_pozostale: 3000,
  zdanie_odrebne: 1500,
  inne: 1600
};

const SENTENCJA_KEYS = new Set(["tenor", "orzeka", "postanawia"]);
const UZASADNIENIE_POZOSTALE_KEYS = new Set(["uzasadnienie_historyczne", "uzasadnienie_postepowanie", "uzasadnienie_ogolne"]);

const METADATA_WEIGHTS = {
  sameRepertory: 0.4,
  sameBenchIpo: 0.2,
  yearProximity: 0.2,
  judgeOverlap: 0.2
};

const FINAL_WEIGHTS = {
  lexical: 0.52,
  citations: 0.2,
  metadata: 0.08,
  dotyczyBigrams: 0.2
};

const MAX_YEAR_DISTANCE = 20;
const TOPIC_BIGRAM_MAX_TERMS = 160;

const TOKEN_MIN_LEN = 3;
const MAX_DF_RATIO = 0.12;
const MIN_DF = 2;

const STOPWORDS = new Set([
  "i", "oraz", "lub", "na", "w", "z", "do", "od", "o", "u", "a", "ze", "się", "sie", "jest", "są", "sa", "to", "ten", "ta", "te",
  "nie", "dla", "przez", "który", "która", "które", "ktory", "ktora", "ktore", "jako", "po", "za", "co", "czy", "art", "ust", "pkt", "par", "sygn",
  "trybunał", "trybunal", "konstytucyjny", "ustawy", "ustawa", "konstytucji", "dnia", "roku", "orzeczenie", "postępowania", "postepowania",
  "otk", "zu", "poz", "nr", "dz", "lit", "dalej", "tk", "rp"
].map((item) => normalizeToken(item)));

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

function normalizeToken(value) {
  return normalizeText(value).replace(/[^\p{L}\p{N}]+/gu, "").trim();
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) continue;
    const [rawKey, rawValue] = token.slice(2).split("=", 2);
    if (rawValue !== undefined) {
      out[rawKey] = rawValue;
      continue;
    }
    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      out[rawKey] = next;
      i += 1;
      continue;
    }
    out[rawKey] = true;
  }
  return out;
}

function parseBooleanArg(value, fallback = false) {
  if (value === true) return true;
  if (value === false || value === undefined || value === null) return fallback;
  const normalized = normalizeText(value);
  if (["1", "true", "yes", "y", "tak"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "nie"].includes(normalized)) return false;
  return fallback;
}

function parseIntArg(value, fallback, min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER) {
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

function parseDateToTs(value) {
  const normalized = normalizeSpace(value);
  if (!normalized) return 0;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) return 0;
  return date.getTime();
}

function extractRepertory(signature) {
  const normalized = normalizeSpace(signature);
  if (!normalized) return "";
  const match = normalized.match(/^([A-Za-z]{1,4})\b/);
  return match ? match[1].toUpperCase() : "";
}

function yearProximity(aYear, bYear) {
  if (!Number.isFinite(aYear) || !Number.isFinite(bYear)) return 0;
  const distance = Math.min(Math.abs(aYear - bYear), MAX_YEAR_DISTANCE);
  return Math.max(0, 1 - (distance / MAX_YEAR_DISTANCE));
}

function jaccard(setA, setB) {
  if (!setA.size || !setB.size) return 0;
  let intersection = 0;
  const smaller = setA.size <= setB.size ? setA : setB;
  const larger = smaller === setA ? setB : setA;
  for (const value of smaller) {
    if (larger.has(value)) intersection += 1;
  }
  const union = setA.size + setB.size - intersection;
  if (!union) return 0;
  return intersection / union;
}

function tokenize(value) {
  return normalizeSpace(value)
    .split(/[^\p{L}\p{N}]+/u)
    .map((token) => normalizeToken(token))
    .filter((token) => token && token.length >= TOKEN_MIN_LEN && !/^\d+$/.test(token) && !STOPWORDS.has(token));
}

function sectionBucket(sectionKey) {
  const key = normalizeSpace(sectionKey);
  if (SENTENCJA_KEYS.has(key)) return "sentencja";
  if (key === "uzasadnienie_prawne") return "uzasadnienie_prawne";
  if (UZASADNIENIE_POZOSTALE_KEYS.has(key)) return "uzasadnienie_pozostale";
  if (key === "zdanie_odrebne") return "zdanie_odrebne";
  return "inne";
}

function countTokenWeighted(tokens, sectionWeight, cap, counter, stats) {
  let consumed = 0;
  for (const token of tokens) {
    if (consumed >= cap) break;
    counter.set(token, (counter.get(token) || 0) + sectionWeight);
    consumed += 1;
  }
  stats.consumed += consumed;
  stats.truncated += Math.max(0, tokens.length - consumed);
}

function buildBigramCounter(tokens, maxTerms = TOPIC_BIGRAM_MAX_TERMS) {
  const counter = new Map();
  if (!Array.isArray(tokens) || tokens.length < 2) return counter;

  let produced = 0;
  for (let i = 0; i < tokens.length - 1; i += 1) {
    if (produced >= maxTerms) break;
    const left = normalizeToken(tokens[i]);
    const right = normalizeToken(tokens[i + 1]);
    if (!left || !right) continue;
    const key = `${left} ${right}`;
    counter.set(key, (counter.get(key) || 0) + 1);
    produced += 1;
  }

  return counter;
}

function counterWeightTotal(counter) {
  if (!counter || !counter.size) return 0;
  let total = 0;
  for (const value of counter.values()) total += value;
  return total;
}

function extractLegalCitationsFromText(text) {
  const normalized = normalizeSpace(text);
  if (!normalized) return [];

  const citations = [];
  const artRegex = /art\.?\s*(\d+[a-zA-Z]?)(?:\s*ust\.?\s*(\d+[a-zA-Z]?))?(?:\s*§\s*(\d+[a-zA-Z]?))?(?:\s*pkt\.?\s*(\d+[a-zA-Z]?))?/giu;
  const hasArt = /art\.?\s*\d/iu.test(normalized);

  let match;
  while ((match = artRegex.exec(normalized)) !== null) {
    const art = match[1] ? normalizeToken(match[1]) : "";
    if (!art) continue;
    const ust = match[2] ? normalizeToken(match[2]) : "";
    const par = match[3] ? normalizeToken(match[3]) : "";
    const pkt = match[4] ? normalizeToken(match[4]) : "";

    let label = `art. ${art}`;
    if (ust) label += ` ust. ${ust}`;
    if (par) label += ` § ${par}`;
    if (pkt) label += ` pkt ${pkt}`;
    citations.push(label);
  }

  if (!hasArt) {
    const standaloneParRegex = /§\s*(\d+[a-zA-Z]?)/gu;
    while ((match = standaloneParRegex.exec(normalized)) !== null) {
      const par = match[1] ? normalizeToken(match[1]) : "";
      if (!par) continue;
      citations.push(`§ ${par}`);
    }
  }

  return citations;
}

function buildCaseFeatures(caseItem, index) {
  const tokenCounter = new Map();
  const bucketStats = {
    sentencja: { consumed: 0, truncated: 0 },
    uzasadnienie_prawne: { consumed: 0, truncated: 0 },
    uzasadnienie_pozostale: { consumed: 0, truncated: 0 },
    zdanie_odrebne: { consumed: 0, truncated: 0 },
    inne: { consumed: 0, truncated: 0 }
  };

  const citationCounter = new Map();

  for (const paragraph of caseItem.paragraphs || []) {
    const sectionKey = normalizeSpace(paragraph.section_key);
    const sectionWeight = SECTION_WEIGHTS[sectionKey] || SECTION_WEIGHTS.inne;
    const bucket = sectionBucket(sectionKey);
    const cap = SECTION_CAPS[bucket] ?? SECTION_CAPS.inne;

    const tokens = tokenize(paragraph.text || "");
    countTokenWeighted(tokens, sectionWeight, cap, tokenCounter, bucketStats[bucket]);

    const citations = extractLegalCitationsFromText(paragraph.text || "");
    for (const citation of citations) {
      citationCounter.set(citation, (citationCounter.get(citation) || 0) + 1);
    }
  }

  const judgeSet = new Set((caseItem.judge_names || []).map((name) => normalizeSpace(name)).filter(Boolean));
  const repertory = extractRepertory(caseItem.case_signature);
  const topicTokens = tokenize(caseItem.topic || "");
  const topicBigramCounter = buildBigramCounter(topicTokens, TOPIC_BIGRAM_MAX_TERMS);

  return {
    index,
    caseItem,
    docId: normalizeSpace(caseItem.document_id),
    caseSignature: normalizeSpace(caseItem.case_signature),
    decisionDateIso: normalizeSpace(caseItem.decision_date_iso),
    decisionTs: parseDateToTs(caseItem.decision_date_iso || caseItem.decision_date_raw),
    decisionTypeIpoKey: normalizeSpace(caseItem.decision_type_ipo_key),
    decisionTypeIpoLabel: normalizeSpace(caseItem.decision_type_ipo_label),
    benchSizeIpoKey: normalizeSpace(caseItem.bench_size_ipo_key),
    year: Number.isFinite(caseItem.year) ? caseItem.year : Number.parseInt(caseItem.year, 10),
    repertory,
    judgeSet,
    tokenCounter,
    citationCounter,
    topicBigramCounter,
    citationWeightTotal: [...citationCounter.values()].reduce((sum, value) => sum + value, 0),
    topicBigramWeightTotal: counterWeightTotal(topicBigramCounter),
    bucketStats
  };
}

function compareCandidates(a, b, docs) {
  if (Math.abs(b.score - a.score) > EPS) return b.score - a.score;
  const docA = docs[a.targetIndex];
  const docB = docs[b.targetIndex];
  if ((docB.decisionTs || 0) !== (docA.decisionTs || 0)) return (docB.decisionTs || 0) - (docA.decisionTs || 0);
  const bySig = String(docA.caseSignature || "").localeCompare(String(docB.caseSignature || ""), "pl");
  if (bySig !== 0) return bySig;
  return String(docA.docId || "").localeCompare(String(docB.docId || ""), "pl");
}

function addTopCandidate(list, candidate, limit, docs) {
  const existingIndex = list.findIndex((entry) => entry.targetIndex === candidate.targetIndex);
  if (existingIndex >= 0) {
    if (compareCandidates(candidate, list[existingIndex], docs) < 0) {
      list[existingIndex] = candidate;
    }
  } else {
    list.push(candidate);
  }
  list.sort((a, b) => compareCandidates(a, b, docs));
  if (list.length > limit) list.length = limit;
}

function parseJsonl(text) {
  return normalizeSpace(text)
    ? text
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line, idx) => {
        try {
          return JSON.parse(line);
        } catch (error) {
          throw new Error(`Invalid JSONL at line ${idx + 1}: ${error?.message || error}`);
        }
      })
    : [];
}

function stringifyStable(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((entry) => stringifyStable(entry)).join(",")}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${stringifyStable(value[key])}`).join(",")}}`;
}

function seededMulberry32(seed) {
  let t = seed >>> 0;
  return () => {
    t += 0x6D2B79F5;
    let r = t;
    r = Math.imul(r ^ (r >>> 15), r | 1);
    r ^= r + Math.imul(r ^ (r >>> 7), r | 61);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

function seedFromHash(hash) {
  const normalized = normalizeSpace(hash).replace(/[^a-fA-F0-9]/g, "");
  const short = normalized.slice(0, 8) || "1234abcd";
  return Number.parseInt(short, 16) >>> 0;
}

function pickRandomDistinctIndices(total, count, randomFn) {
  const target = Math.min(total, count);
  const indices = Array.from({ length: total }, (_, index) => index);
  for (let i = total - 1; i > 0; i -= 1) {
    const j = Math.floor(randomFn() * (i + 1));
    const tmp = indices[i];
    indices[i] = indices[j];
    indices[j] = tmp;
  }
  return indices.slice(0, target).sort((a, b) => a - b);
}

function buildSparseTfidfVectors(docs) {
  const tokenDf = new Map();
  for (const doc of docs) {
    for (const token of doc.tokenCounter.keys()) {
      tokenDf.set(token, (tokenDf.get(token) || 0) + 1);
    }
  }

  const docCount = docs.length;
  const maxDf = Math.max(MIN_DF, Math.floor(docCount * MAX_DF_RATIO));

  const tokenIndex = new Map();
  const tokenList = [];
  let nextTokenId = 0;

  for (const [token, df] of tokenDf.entries()) {
    if (df < MIN_DF) continue;
    if (df > maxDf) continue;
    tokenIndex.set(token, nextTokenId);
    tokenList.push(token);
    nextTokenId += 1;
  }

  const idf = new Float64Array(tokenList.length);
  for (let i = 0; i < tokenList.length; i += 1) {
    const df = tokenDf.get(tokenList[i]) || 1;
    idf[i] = Math.log((docCount + 1) / (df + 1)) + 1;
  }

  const vectors = docs.map(() => new Map());
  const postings = Array.from({ length: tokenList.length }, () => []);

  for (let docIndex = 0; docIndex < docs.length; docIndex += 1) {
    const doc = docs[docIndex];
    const vector = new Map();
    let norm2 = 0;

    for (const [token, tf] of doc.tokenCounter.entries()) {
      const tokenId = tokenIndex.get(token);
      if (tokenId === undefined) continue;
      const weight = tf * idf[tokenId];
      if (weight <= 0) continue;
      vector.set(tokenId, weight);
      norm2 += weight * weight;
    }

    if (norm2 <= 0) {
      vectors[docIndex] = vector;
      continue;
    }

    const norm = Math.sqrt(norm2);
    for (const [tokenId, weight] of vector.entries()) {
      const normalizedWeight = weight / norm;
      vector.set(tokenId, normalizedWeight);
      postings[tokenId].push({ docIndex, weight: normalizedWeight });
    }

    vectors[docIndex] = vector;
  }

  return {
    tokenList,
    vectors,
    postings,
    maxDf
  };
}

function buildLexicalMatrix(docs, postings) {
  const n = docs.length;
  const matrix = new Float32Array(n * n);

  for (let tokenId = 0; tokenId < postings.length; tokenId += 1) {
    const list = postings[tokenId];
    if (!list || list.length < 2) continue;

    for (let i = 0; i < list.length; i += 1) {
      const a = list[i];
      const rowOffset = a.docIndex * n;
      for (let j = i + 1; j < list.length; j += 1) {
        const b = list[j];
        const contribution = a.weight * b.weight;
        matrix[rowOffset + b.docIndex] += contribution;
        matrix[(b.docIndex * n) + a.docIndex] += contribution;
      }
    }
  }

  return matrix;
}

function buildCitationMatrix(docs) {
  const n = docs.length;
  const numerator = new Float32Array(n * n);
  const citationPostings = new Map();

  for (let docIndex = 0; docIndex < docs.length; docIndex += 1) {
    for (const [citation, count] of docs[docIndex].citationCounter.entries()) {
      const list = citationPostings.get(citation) || [];
      list.push({ docIndex, count });
      citationPostings.set(citation, list);
    }
  }

  for (const list of citationPostings.values()) {
    if (list.length < 2) continue;
    for (let i = 0; i < list.length; i += 1) {
      const a = list[i];
      const rowOffset = a.docIndex * n;
      for (let j = i + 1; j < list.length; j += 1) {
        const b = list[j];
        const overlap = Math.min(a.count, b.count);
        numerator[rowOffset + b.docIndex] += overlap;
        numerator[(b.docIndex * n) + a.docIndex] += overlap;
      }
    }
  }

  return numerator;
}

function buildTopicBigramMatrix(docs) {
  const n = docs.length;
  const numerator = new Float32Array(n * n);
  const posting = new Map();

  for (let docIndex = 0; docIndex < docs.length; docIndex += 1) {
    for (const [bigram, count] of docs[docIndex].topicBigramCounter.entries()) {
      const list = posting.get(bigram) || [];
      list.push({ docIndex, count });
      posting.set(bigram, list);
    }
  }

  for (const list of posting.values()) {
    if (list.length < 2) continue;
    for (let i = 0; i < list.length; i += 1) {
      const a = list[i];
      const rowOffset = a.docIndex * n;
      for (let j = i + 1; j < list.length; j += 1) {
        const b = list[j];
        const overlap = Math.min(a.count, b.count);
        numerator[rowOffset + b.docIndex] += overlap;
        numerator[(b.docIndex * n) + a.docIndex] += overlap;
      }
    }
  }

  return numerator;
}

function computeMetadataScore(docA, docB) {
  const sameRepertory = docA.repertory && docB.repertory && docA.repertory === docB.repertory ? 1 : 0;
  const sameBenchIpo = docA.benchSizeIpoKey && docB.benchSizeIpoKey && docA.benchSizeIpoKey === docB.benchSizeIpoKey ? 1 : 0;
  const years = yearProximity(docA.year, docB.year);
  const judge = jaccard(docA.judgeSet, docB.judgeSet);

  return Number((
    (METADATA_WEIGHTS.sameRepertory * sameRepertory)
    + (METADATA_WEIGHTS.sameBenchIpo * sameBenchIpo)
    + (METADATA_WEIGHTS.yearProximity * years)
    + (METADATA_WEIGHTS.judgeOverlap * judge)
  ).toFixed(6));
}

function computeCitationOverlap(docA, docB, numerator) {
  const denominator = docA.citationWeightTotal + docB.citationWeightTotal - numerator;
  if (denominator <= 0) return 0;
  return Number((numerator / denominator).toFixed(6));
}

function computeTopicBigramOverlap(docA, docB, numerator) {
  const denominator = docA.topicBigramWeightTotal + docB.topicBigramWeightTotal - numerator;
  if (denominator <= 0) return 0;
  return Number((numerator / denominator).toFixed(6));
}

function computeFinalScore(lexical, citations, metadata, dotyczyBigrams) {
  return Number((
    (FINAL_WEIGHTS.lexical * lexical)
    + (FINAL_WEIGHTS.citations * citations)
    + (FINAL_WEIGHTS.metadata * metadata)
    + (FINAL_WEIGHTS.dotyczyBigrams * dotyczyBigrams)
  ).toFixed(6));
}

function evaluatePairComponents(docA, docB, lexicalMatrix, citationNumeratorMatrix, topicBigramNumeratorMatrix, n) {
  const lexical = Number((lexicalMatrix[(docA.index * n) + docB.index] || 0).toFixed(6));
  const citationNumerator = citationNumeratorMatrix[(docA.index * n) + docB.index] || 0;
  const citations = computeCitationOverlap(docA, docB, citationNumerator);
  const topicBigramNumerator = topicBigramNumeratorMatrix[(docA.index * n) + docB.index] || 0;
  const dotyczyBigrams = computeTopicBigramOverlap(docA, docB, topicBigramNumerator);
  const metadata = computeMetadataScore(docA, docB);
  const score = computeFinalScore(lexical, citations, metadata, dotyczyBigrams);
  return {
    score,
    components: {
      lexical,
      citations,
      metadata,
      dotyczy_bigrams: dotyczyBigrams
    }
  };
}

function collectSharedCitationReasons(docA, docB, limit = 3) {
  if (!docA.citationCounter.size || !docB.citationCounter.size) return [];
  const shared = [];
  const iterateMap = docA.citationCounter.size <= docB.citationCounter.size ? docA.citationCounter : docB.citationCounter;
  const otherMap = iterateMap === docA.citationCounter ? docB.citationCounter : docA.citationCounter;

  for (const [citation, countA] of iterateMap.entries()) {
    const countB = otherMap.get(citation);
    if (!countB) continue;
    shared.push({ citation, weight: Math.min(countA, countB) });
  }

  if (!shared.length) return [];
  shared.sort((a, b) => b.weight - a.weight || a.citation.localeCompare(b.citation, "pl"));
  const top = shared.slice(0, limit).map((entry) => entry.citation);
  return [`Wspólne przepisy: ${top.join(", ")}`];
}

function collectSharedTermReasons(docA, docB, tokenList, limit = 3) {
  const shared = [];
  const vecA = docA.vector;
  const vecB = docB.vector;
  if (!vecA.size || !vecB.size) return [];

  const iterate = vecA.size <= vecB.size ? vecA : vecB;
  const other = iterate === vecA ? vecB : vecA;

  for (const [tokenId, weightA] of iterate.entries()) {
    const weightB = other.get(tokenId);
    if (weightB === undefined) continue;
    shared.push({ token: tokenList[tokenId], contribution: weightA * weightB });
  }

  if (!shared.length) return [];
  shared.sort((a, b) => b.contribution - a.contribution || a.token.localeCompare(b.token, "pl"));
  const top = shared.slice(0, limit).map((entry) => entry.token);
  return [`Wspólne frazy w tekście: ${top.join(", ")}`];
}

function collectSharedTopicBigramsReasons(docA, docB, limit = 3) {
  if (!docA.topicBigramCounter.size || !docB.topicBigramCounter.size) return [];
  const shared = [];
  const iterateMap = docA.topicBigramCounter.size <= docB.topicBigramCounter.size ? docA.topicBigramCounter : docB.topicBigramCounter;
  const otherMap = iterateMap === docA.topicBigramCounter ? docB.topicBigramCounter : docA.topicBigramCounter;

  for (const [bigram, countA] of iterateMap.entries()) {
    const countB = otherMap.get(bigram);
    if (!countB) continue;
    shared.push({ bigram, weight: Math.min(countA, countB) });
  }

  if (!shared.length) return [];
  shared.sort((a, b) => b.weight - a.weight || a.bigram.localeCompare(b.bigram, "pl"));
  const top = shared.slice(0, limit).map((entry) => entry.bigram);
  return [`Wspólne frazy w tytule: ${top.join(", ")}`];
}

function collectMetadataReasons(docA, docB) {
  const reasons = [];
  if (docA.repertory && docB.repertory && docA.repertory === docB.repertory) {
    reasons.push(`To samo repertorium: ${docA.repertory}`);
  }
  return reasons;
}

function buildReasons(docA, docB, tokenList) {
  const reasons = [
    ...collectSharedCitationReasons(docA, docB, 3),
    ...collectSharedTopicBigramsReasons(docA, docB, 3),
    ...collectSharedTermReasons(docA, docB, tokenList, 3),
    ...collectMetadataReasons(docA, docB)
  ];

  if (!reasons.length) {
    reasons.push("Podobna struktura argumentacji i metadanych.");
  }

  return reasons.slice(0, 3);
}

function buildTopCandidatesForScope(docIndices, docs, lexicalMatrix, citationNumeratorMatrix, topicBigramNumeratorMatrix, topK, threshold) {
  const strictTop = new Map();
  const anyTop = new Map();

  for (const docIndex of docIndices) {
    strictTop.set(docIndex, []);
    anyTop.set(docIndex, []);
  }

  for (let iIdx = 0; iIdx < docIndices.length; iIdx += 1) {
    const i = docIndices[iIdx];
    for (let jIdx = iIdx + 1; jIdx < docIndices.length; jIdx += 1) {
      const j = docIndices[jIdx];
      const docI = docs[i];
      const docJ = docs[j];
      const pair = evaluatePairComponents(docI, docJ, lexicalMatrix, citationNumeratorMatrix, topicBigramNumeratorMatrix, docs.length);
      if (pair.score <= 0) continue;

      const candidateIJ = {
        targetIndex: j,
        score: pair.score,
        components: pair.components
      };
      const candidateJI = {
        targetIndex: i,
        score: pair.score,
        components: pair.components
      };

      addTopCandidate(anyTop.get(i), candidateIJ, topK, docs);
      addTopCandidate(anyTop.get(j), candidateJI, topK, docs);

      if (pair.score >= threshold) {
        addTopCandidate(strictTop.get(i), candidateIJ, topK, docs);
        addTopCandidate(strictTop.get(j), candidateJI, topK, docs);
      }
    }
  }

  const finalTop = new Map();
  for (const docIndex of docIndices) {
    const strict = [...(strictTop.get(docIndex) || [])];
    const strictSet = new Set(strict.map((item) => item.targetIndex));
    const any = anyTop.get(docIndex) || [];

    for (const candidate of any) {
      if (strict.length >= topK) break;
      if (strictSet.has(candidate.targetIndex)) continue;
      strict.push(candidate);
      strictSet.add(candidate.targetIndex);
    }

    strict.sort((a, b) => compareCandidates(a, b, docs));
    finalTop.set(docIndex, strict.slice(0, topK));
  }

  return finalTop;
}

function buildSimilarityPayload({
  docs,
  topMap,
  tokenList,
  datasetHash,
  datasetGeneratedAt,
  generatedAt,
  config,
  sourceName,
  scopeLabel
}) {
  const byDocumentId = {};
  let totalRecommendations = 0;
  let top5CompleteCases = 0;
  let scoreSum = 0;
  let withCitationOverlap = 0;
  let withDotyczyBigramOverlap = 0;

  for (const [sourceIndex, candidates] of topMap.entries()) {
    const source = docs[sourceIndex];
    if (!source?.docId) continue;

    if (candidates.length >= config.top_k) top5CompleteCases += 1;

    byDocumentId[source.docId] = candidates.map((candidate) => {
      const target = docs[candidate.targetIndex];
      const reasons = buildReasons(source, target, tokenList);
      totalRecommendations += 1;
      scoreSum += candidate.score;
      if ((candidate.components?.citations || 0) > 0) withCitationOverlap += 1;
      if ((candidate.components?.dotyczy_bigrams || 0) > 0) withDotyczyBigramOverlap += 1;

      return {
        document_id: target.docId,
        case_signature: target.caseSignature,
        decision_date_iso: target.decisionDateIso || null,
        decision_type_ipo_label: target.decisionTypeIpoLabel || null,
        score: Number(candidate.score.toFixed(6)),
        components: {
          lexical: Number((candidate.components?.lexical || 0).toFixed(6)),
          citations: Number((candidate.components?.citations || 0).toFixed(6)),
          metadata: Number((candidate.components?.metadata || 0).toFixed(6)),
          dotyczy_bigrams: Number((candidate.components?.dotyczy_bigrams || 0).toFixed(6))
        },
        reasons,
        source_url: normalizeSpace(target.caseItem?.source_url) || null
      };
    });
  }

  return {
    schema_version: SCHEMA_VERSION,
    similarity_version: SIMILARITY_VERSION,
    generated_at: generatedAt,
    dataset_hash: datasetHash,
    dataset_generated_at: datasetGeneratedAt,
    source_name: sourceName,
    scope: scopeLabel,
    config,
    metrics: {
      source_cases: Object.keys(byDocumentId).length,
      coverage_top5: Number((Object.keys(byDocumentId).length ? (top5CompleteCases / Object.keys(byDocumentId).length) : 0).toFixed(6)),
      avg_similarity_score_top5: Number((totalRecommendations ? (scoreSum / totalRecommendations) : 0).toFixed(6)),
      share_with_common_citation: Number((totalRecommendations ? (withCitationOverlap / totalRecommendations) : 0).toFixed(6)),
      share_with_common_dotyczy_bigram: Number((totalRecommendations ? (withDotyczyBigramOverlap / totalRecommendations) : 0).toFixed(6))
    },
    by_document_id: byDocumentId
  };
}

async function loadCasesFromJsonl(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return parseJsonl(raw);
}

function getDatasetMeta(cases) {
  const first = cases.find((item) => normalizeSpace(item?.dataset_hash) && normalizeSpace(item?.dataset_generated_at));
  return {
    datasetHash: normalizeSpace(first?.dataset_hash),
    datasetGeneratedAt: normalizeSpace(first?.dataset_generated_at)
  };
}

function ensureUniqueDocIds(docs) {
  const seen = new Set();
  for (const doc of docs) {
    if (!doc.docId) {
      throw new Error(`Missing document_id for case: ${doc.caseSignature || "(unknown)"}`);
    }
    if (seen.has(doc.docId)) {
      throw new Error(`Duplicate document_id detected: ${doc.docId}`);
    }
    seen.add(doc.docId);
  }
}

async function maybeReadSampleIndices(samplePath, docIndexById) {
  if (!samplePath) return null;
  try {
    const rows = await loadCasesFromJsonl(samplePath);
    const indices = [];
    for (const row of rows) {
      const documentId = normalizeSpace(row?.document_id);
      if (!documentId) continue;
      const index = docIndexById.get(documentId);
      if (index !== undefined) indices.push(index);
    }
    const unique = [...new Set(indices)].sort((a, b) => a - b);
    if (!unique.length) return null;
    return unique;
  } catch {
    return null;
  }
}

function buildAuditPayload(docs, topMap, tokenList, datasetHash, config, sampleSize = 50) {
  const rng = seededMulberry32(seedFromHash(datasetHash));
  const sampledIndices = pickRandomDistinctIndices(docs.length, sampleSize, rng);

  const records = sampledIndices.map((sourceIndex) => {
    const source = docs[sourceIndex];
    const candidates = topMap.get(sourceIndex) || [];
    return {
      document_id: source.docId,
      case_signature: source.caseSignature,
      decision_type_ipo_label: source.decisionTypeIpoLabel,
      year: source.year,
      top_similar: candidates.map((candidate) => {
        const target = docs[candidate.targetIndex];
        return {
          document_id: target.docId,
          case_signature: target.caseSignature,
          score: Number(candidate.score.toFixed(6)),
          components: {
            lexical: Number((candidate.components?.lexical || 0).toFixed(6)),
            citations: Number((candidate.components?.citations || 0).toFixed(6)),
            metadata: Number((candidate.components?.metadata || 0).toFixed(6)),
            dotyczy_bigrams: Number((candidate.components?.dotyczy_bigrams || 0).toFixed(6))
          },
          reasons: buildReasons(source, target, tokenList)
        };
      })
    };
  });

  return {
    generated_at: new Date().toISOString(),
    schema_version: SCHEMA_VERSION,
    similarity_version: SIMILARITY_VERSION,
    dataset_hash: datasetHash,
    config,
    sample_size: records.length,
    checklist_manual_validation: {
      target_cases: 30,
      scoring_scale: "1-5",
      dimensions: [
        "trafność merytoryczna",
        "podobieństwo problemu konstytucyjnego",
        "użyteczność do argumentacji"
      ],
      mark_false_positives: true
    },
    records
  };
}

async function writeJson(filePath, payload) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  const inputPath = path.resolve(args.input || DEFAULT_INPUT);
  const outdir = path.resolve(args.outdir || DEFAULT_OUTDIR);
  const outputName = args.output || DEFAULT_OUTPUT;
  const outputPath = path.join(outdir, outputName);

  const sample50DatasetPath = args.sample50 || DEFAULT_SAMPLE50_DATASET;
  const sample200DatasetPath = args.sample200 || DEFAULT_SAMPLE200_DATASET;
  const sample50OutName = args.sample50_output || DEFAULT_SAMPLE50_OUTPUT;
  const sample200OutName = args.sample200_output || DEFAULT_SAMPLE200_OUTPUT;
  const sample50OutputPath = path.join(outdir, sample50OutName);
  const sample200OutputPath = path.join(outdir, sample200OutName);

  const topK = parseIntArg(args.topk, DEFAULT_TOP_K, 1, 20);
  const threshold = parseFloatArg(args.threshold, DEFAULT_THRESHOLD, 0, 1);
  const auditEnabled = parseBooleanArg(args.audit, false);
  const auditOutPath = path.resolve(args.audit_out || DEFAULT_AUDIT_PATH);
  const auditSampleSize = parseIntArg(args.audit_sample_size, 50, 1, 500);

  const rawCases = await loadCasesFromJsonl(inputPath);
  if (!rawCases.length) {
    throw new Error(`Input JSONL is empty: ${inputPath}`);
  }

  const datasetMeta = getDatasetMeta(rawCases);
  if (!datasetMeta.datasetHash) {
    throw new Error("Input dataset does not contain dataset_hash.");
  }

  const docs = rawCases.map((caseItem, index) => buildCaseFeatures(caseItem, index));
  ensureUniqueDocIds(docs);

  const docIndexById = new Map(docs.map((doc) => [doc.docId, doc.index]));

  const { tokenList, vectors, postings, maxDf } = buildSparseTfidfVectors(docs);
  for (let i = 0; i < docs.length; i += 1) {
    docs[i].vector = vectors[i];
  }

  const lexicalMatrix = buildLexicalMatrix(docs, postings);
  const citationNumeratorMatrix = buildCitationMatrix(docs);
  const topicBigramNumeratorMatrix = buildTopicBigramMatrix(docs);

  const fullIndices = docs.map((doc) => doc.index);
  const fullTop = buildTopCandidatesForScope(fullIndices, docs, lexicalMatrix, citationNumeratorMatrix, topicBigramNumeratorMatrix, topK, threshold);

  const generatedAt = new Date().toISOString();
  const config = {
    top_k: topK,
    threshold,
    section_weights: SECTION_WEIGHTS,
    section_caps: SECTION_CAPS,
    metadata_weights: METADATA_WEIGHTS,
    final_weights: FINAL_WEIGHTS,
    topic_bigram: {
      source_field: "topic",
      ngram_n: 2,
      max_terms: TOPIC_BIGRAM_MAX_TERMS
    },
    tokenization: {
      min_token_length: TOKEN_MIN_LEN,
      stopwords: "built-in polish legal stopwords",
      min_df: MIN_DF,
      max_df_ratio: MAX_DF_RATIO,
      max_df_applied: maxDf
    }
  };

  const fullPayload = buildSimilarityPayload({
    docs,
    topMap: fullTop,
    tokenList,
    datasetHash: datasetMeta.datasetHash,
    datasetGeneratedAt: datasetMeta.datasetGeneratedAt,
    generatedAt,
    config,
    sourceName: path.basename(inputPath),
    scopeLabel: "full"
  });

  await writeJson(outputPath, fullPayload);

  const sample50Indices = await maybeReadSampleIndices(sample50DatasetPath, docIndexById);
  if (sample50Indices && sample50Indices.length >= 2) {
    const topSample50 = buildTopCandidatesForScope(sample50Indices, docs, lexicalMatrix, citationNumeratorMatrix, topicBigramNumeratorMatrix, topK, threshold);
    const samplePayload = buildSimilarityPayload({
      docs,
      topMap: topSample50,
      tokenList,
      datasetHash: datasetMeta.datasetHash,
      datasetGeneratedAt: datasetMeta.datasetGeneratedAt,
      generatedAt,
      config,
      sourceName: path.basename(sample50DatasetPath),
      scopeLabel: "sample50"
    });
    await writeJson(sample50OutputPath, samplePayload);
  }

  const sample200Indices = await maybeReadSampleIndices(sample200DatasetPath, docIndexById);
  if (sample200Indices && sample200Indices.length >= 2) {
    const topSample200 = buildTopCandidatesForScope(sample200Indices, docs, lexicalMatrix, citationNumeratorMatrix, topicBigramNumeratorMatrix, topK, threshold);
    const samplePayload = buildSimilarityPayload({
      docs,
      topMap: topSample200,
      tokenList,
      datasetHash: datasetMeta.datasetHash,
      datasetGeneratedAt: datasetMeta.datasetGeneratedAt,
      generatedAt,
      config,
      sourceName: path.basename(sample200DatasetPath),
      scopeLabel: "sample200"
    });
    await writeJson(sample200OutputPath, samplePayload);
  }

  if (auditEnabled) {
    const auditPayload = buildAuditPayload(docs, fullTop, tokenList, datasetMeta.datasetHash, config, auditSampleSize);
    await writeJson(auditOutPath, auditPayload);
  }

  process.stdout.write(`Similarity build complete.\n`);
  process.stdout.write(`Source cases: ${docs.length}\n`);
  process.stdout.write(`Tokens in model: ${tokenList.length}\n`);
  process.stdout.write(`Saved full similarity: ${outputPath}\n`);
  if (sample50Indices && sample50Indices.length >= 2) {
    process.stdout.write(`Saved sample50 similarity: ${sample50OutputPath}\n`);
  }
  if (sample200Indices && sample200Indices.length >= 2) {
    process.stdout.write(`Saved sample200 similarity: ${sample200OutputPath}\n`);
  }
  if (auditEnabled) {
    process.stdout.write(`Saved audit report: ${auditOutPath}\n`);
  }

  // Optional deterministic signature printout.
  process.stdout.write(`Digest key: ${stringifyStable({ hash: datasetMeta.datasetHash, generatedAt, topK, threshold }).slice(0, 80)}...\n`);
}

main().catch((error) => {
  process.stderr.write(`Fatal error: ${error?.message || error}\n`);
  process.exit(1);
});
