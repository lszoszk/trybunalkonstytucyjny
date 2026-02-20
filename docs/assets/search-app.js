const SAMPLE_DATA_URL = "data/tk_cases_sample200.jsonl";
const FULL_BENCH_DATA_URL = "data/tk_cases_full_bench.jsonl";
const SIMILARITY_DATA_URL = "data/similar_cases.json";
const SIMILARITY_SAMPLE200_URL = "data/similar_cases_sample200.json";
const SIMILARITY_SAMPLE50_URL = "data/similar_cases_sample50.json";
const SIMILARITY_FULL_BENCH_URL = "data/similar_cases_full_bench.json";
const SIMILARITY_SCHEMA_VERSION = "tk-similarity-v1";
const PAGE_SIZE = 10;
const TOOL_VERSION = "tk-dashboard-v2";
const NORMALIZATION_VERSION = "tk-norm-v2";
const MAX_FILE_BYTES = 80 * 1024 * 1024;

const STORAGE_KEYS = {
  savedQueries: "tk_saved_queries",
  caseFolder: "tk_case_folder",
  uiPrefs: "tk_ui_prefs"
};

const FIELD_OPERATOR_MAP = {
  sygn: "signature",
  sedzia: "judge",
  typ: "decisionType",
  sekcja: "section",
  rok: "year",
  teza: "thesis"
};

const IPO_DECISION_TYPES = [
  { key: "postanowienie", label: "Postanowienie" },
  { key: "postanowienie_tymczasowe", label: "Postanowienie Tymczasowe" },
  { key: "rozstrzygniecie", label: "Rozstrzygnięcie" },
  { key: "wyrok", label: "Wyrok" }
];
const IPO_DECISION_TYPE_BY_KEY = new Map(IPO_DECISION_TYPES.map((entry) => [entry.key, entry]));

const IPO_BENCH_SIZES = [
  { key: "pelny_sklad", label: "Pełny skład" },
  { key: "piecioosobowa", label: "Pięcioosobowa" },
  { key: "trojosobowa", label: "Trójosobowa" }
];
const IPO_BENCH_BY_KEY = new Map(IPO_BENCH_SIZES.map((entry) => [entry.key, entry]));

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

const SECTION_SCORE_WEIGHT = {
  tenor: 1.45,
  orzeka: 1.4,
  postanawia: 1.35,
  uzasadnienie_prawne: 1.22,
  zdanie_odrebne: 1.15,
  uzasadnienie_postepowanie: 1.08,
  uzasadnienie_historyczne: 1.04,
  uzasadnienie_ogolne: 1.02,
  komparycja: 0.92,
  sentencja_inna: 1.02,
  inne: 1
};

const QUICK_PRESETS = {
  holding: {
    sectionKeys: ["tenor", "orzeka", "postanawia"],
    typeKeys: []
  },
  dissent: {
    sectionKeys: ["zdanie_odrebne"],
    typeKeys: []
  },
  tenor: {
    sectionKeys: ["tenor"],
    typeKeys: []
  },
  uzasadnienie_prawne: {
    sectionKeys: ["uzasadnienie_prawne"],
    typeKeys: []
  },
  postanowienie: {
    sectionKeys: [],
    typeKeys: ["postanowienie"]
  }
};

const STOPWORDS = new Set([
  "i", "oraz", "lub", "na", "w", "z", "do", "od", "o", "u", "a", "ze", "się", "jest", "są", "to", "ten", "ta", "te",
  "nie", "dla", "przez", "który", "która", "które", "jako", "po", "za", "co", "czy", "art", "ust", "pkt", "par", "sygn",
  "trybunał", "konstytucyjny", "ustawy", "ustawa", "konstytucji", "dnia", "roku", "orzeczenie", "postępowania"
]);

function deriveBenchInfo(judgeNames) {
  const count = (judgeNames || []).filter(Boolean).length;
  if (count >= 11) {
    return {
      count,
      key: "pelny_sklad",
      label: "Pełny skład",
      isFullBench: true
    };
  }
  if (count === 5) {
    return {
      count,
      key: "piecioosobowa",
      label: "Pięcioosobowa",
      isFullBench: false
    };
  }
  if (count === 3) {
    return {
      count,
      key: "trojosobowa",
      label: "Trójosobowa",
      isFullBench: false
    };
  }
  if (count > 0) {
    return {
      count,
      key: `${count}_osobowa`,
      label: `${count}-osobowa`,
      isFullBench: false
    };
  }
  return {
    count: 0,
    key: "nieustalony",
    label: "Nieustalony",
    isFullBench: false
  };
}

function deriveIpoDecisionTypeInfo(decisionType) {
  const norm = normalizeSearchText(decisionType);
  if (!norm) return { key: null, label: null, visible: false };

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
  return { key: null, label: null, visible: false };
}

function deriveIpoBenchInfo(benchKey) {
  const normalizedKey = normalizeSpace(benchKey);
  const entry = IPO_BENCH_BY_KEY.get(normalizedKey);
  if (!entry) {
    return {
      key: "poza_klasyfikacja_ipo",
      label: "Poza klasyfikacją IPO",
      visible: false
    };
  }
  return {
    key: entry.key,
    label: entry.label,
    visible: true
  };
}

function getDecisionTypeIpoLabelByKey(key) {
  const normalizedKey = normalizeSpace(key);
  const fromState = (state.decisionTypes || []).find((entry) => entry.key === normalizedKey);
  if (fromState?.label) return fromState.label;
  return IPO_DECISION_TYPE_BY_KEY.get(normalizedKey)?.label || normalizedKey;
}

function getBenchLabelByKey(key) {
  const normalizedKey = normalizeSpace(key);
  if (!normalizedKey) return "";
  const fromState = (state.benchSizes || []).find((entry) => entry.key === normalizedKey);
  if (fromState?.label) return fromState.label;
  if (IPO_BENCH_BY_KEY.has(normalizedKey)) return IPO_BENCH_BY_KEY.get(normalizedKey).label;
  if (normalizedKey === "poza_klasyfikacja_ipo") return "Poza klasyfikacją IPO";
  return normalizedKey.replaceAll("_", " ");
}

const state = {
  loaded: false,
  cases: [],
  paragraphIndex: [],
  sections: [],
  decisionTypes: [],
  benchSizes: [],
  years: [],
  query: "",
  currentResults: [],
  currentHits: 0,
  currentPage: 1,
  currentMode: "idle",
  currentParsedQuery: null,
  caseByDocumentId: new Map(),
  expandedCases: new Set(),
  expandedSimilarCases: new Set(),
  selectedHits: new Set(),
  currentFilters: null,
  similarityIndex: new Map(),
  similarityMeta: {
    loaded: false,
    available: false,
    sourceUrl: null,
    datasetHash: null,
    message: ""
  },
  similarityRequestSeq: 0,
  validationErrorsCount: 0,
  datasetMeta: {
    sourceName: null,
    hash: null,
    generatedAt: null,
    caseCount: 0,
    normalizationVersion: NORMALIZATION_VERSION
  },
  caseFolder: {
    cases: {},
    paragraphs: {},
    notes: ""
  },
  savedQueries: [],
  uiPrefs: {
    filtersOpen: false,
    activePreset: null,
    viewMode: "expert",
    paragraphDisplayMode: "collapsed"
  },
  currentFileReader: null,
  pendingUrlState: null,
  urlStateApplied: false,
  applyingUrlState: false,
  activeViewerCaseKey: null,
  activeViewerTocTarget: null,
  activeViewerToc: [],
  viewerKeywordQuery: "",
  viewerKeywordParsed: {
    source: "",
    hasQuery: false,
    rpn: [],
    textOperands: [],
    allTerms: [],
    highlightTerms: []
  },
  viewerKeywordError: "",
  hitTextOverrides: new Map(),
  viewerParagraphOverrides: new Map()
};

const el = {};

function byId(id) {
  return document.getElementById(id);
}

function getPageDashboardMode() {
  const mode = normalizeSearchText(document.body?.dataset?.dashboardMode || "expert");
  return mode === "student" ? "student" : "expert";
}

function dashboardHrefForMode(mode) {
  return mode === "student" ? "dashboard-student.html" : "dashboard-expert.html";
}

function syncModeToggleUi() {
  const currentMode = getPageDashboardMode();
  if (el.modeSwitchStudent) {
    el.modeSwitchStudent.href = dashboardHrefForMode("student");
    el.modeSwitchStudent.classList.toggle("active", currentMode === "student");
  }
  if (el.modeSwitchExpert) {
    el.modeSwitchExpert.href = dashboardHrefForMode("expert");
    el.modeSwitchExpert.classList.toggle("active", currentMode === "expert");
  }
}

function inferSimilarityUrlCandidates(sourceName) {
  const normalized = normalizeSearchText(sourceName || "");
  if (normalized.includes("sample50")) return [SIMILARITY_SAMPLE50_URL];
  if (normalized.includes("sample200")) return [SIMILARITY_SAMPLE200_URL];
  if (normalized.includes("full_bench")) return [SIMILARITY_FULL_BENCH_URL, SIMILARITY_DATA_URL];
  if (normalized.includes("tk_cases.jsonl")) return [SIMILARITY_DATA_URL];
  return [SIMILARITY_DATA_URL, SIMILARITY_FULL_BENCH_URL, SIMILARITY_SAMPLE200_URL, SIMILARITY_SAMPLE50_URL];
}

function resetSimilarityState() {
  state.similarityIndex = new Map();
  state.similarityMeta = {
    loaded: false,
    available: false,
    sourceUrl: null,
    datasetHash: null,
    message: ""
  };
}

function renderSimilarityStatus() {
  if (!el.similarityStatus) return;
  el.similarityStatus.hidden = true;
  el.similarityStatus.textContent = "";
  el.similarityStatus.classList.remove("warn", "ok");
}

function isValidSimilarityPayload(payload) {
  if (!payload || typeof payload !== "object") return false;
  if (normalizeSpace(payload.schema_version) !== SIMILARITY_SCHEMA_VERSION) return false;
  if (!payload.by_document_id || typeof payload.by_document_id !== "object") return false;
  return true;
}

function parseSimilarityIndex(payload) {
  const index = new Map();
  for (const [documentId, list] of Object.entries(payload.by_document_id || {})) {
    if (!Array.isArray(list)) continue;
    index.set(
      normalizeSpace(documentId),
      list
        .map((entry) => ({
          document_id: normalizeSpace(entry.document_id),
          case_signature: normalizeSpace(entry.case_signature),
          decision_date_iso: normalizeSpace(entry.decision_date_iso) || "",
          decision_type_ipo_label: normalizeSpace(entry.decision_type_ipo_label) || "",
          score: Number(entry.score) || 0,
          components: {
            lexical: Number(entry.components?.lexical) || 0,
            citations: Number(entry.components?.citations) || 0,
            metadata: Number(entry.components?.metadata) || 0,
            dotyczy_bigrams: Number(entry.components?.dotyczy_bigrams) || 0
          },
          reasons: Array.isArray(entry.reasons) ? entry.reasons.map((item) => normalizeSpace(item)).filter(Boolean) : [],
          source_url: normalizeSpace(entry.source_url) || ""
        }))
        .filter((entry) => entry.document_id && entry.case_signature)
    );
  }
  return index;
}

async function loadSimilarityForCurrentDataset() {
  state.similarityRequestSeq += 1;
  const requestSeq = state.similarityRequestSeq;
  const expectedDatasetHash = normalizeSpace(state.datasetMeta?.hash);
  resetSimilarityState();
  renderSimilarityStatus();

  const candidates = inferSimilarityUrlCandidates(state.datasetMeta?.sourceName || "");
  let firstMismatchInfo = null;

  for (const url of candidates) {
    try {
      const response = await fetch(url, { cache: "no-store" });
      if (!response.ok) continue;
      const payload = await response.json();
      if (!isValidSimilarityPayload(payload)) continue;

      const payloadHash = normalizeSpace(payload.dataset_hash);
      const datasetHash = normalizeSpace(state.datasetMeta?.hash);
      if (!payloadHash || !datasetHash || payloadHash !== datasetHash) {
        firstMismatchInfo = `Podobne orzeczenia niedostępne: hash pliku ${url} nie pasuje do aktualnego zbioru.`;
        continue;
      }

      if (requestSeq !== state.similarityRequestSeq || expectedDatasetHash !== normalizeSpace(state.datasetMeta?.hash)) {
        return;
      }

      state.similarityIndex = parseSimilarityIndex(payload);
      state.similarityMeta = {
        loaded: true,
        available: true,
        sourceUrl: url,
        datasetHash: payloadHash,
        message: `Podobne orzeczenia aktywne (top 5, źródło: ${url.replace(/^data\//, "")}).`
      };
      renderSimilarityStatus();

      if (state.loaded) {
        renderResults(state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
        if (state.activeViewerCaseKey) {
          const active = findCaseByKey(state.activeViewerCaseKey);
          if (active) renderCaseViewer(active);
        }
      }
      return;
    } catch {
      continue;
    }
  }

  if (requestSeq !== state.similarityRequestSeq || expectedDatasetHash !== normalizeSpace(state.datasetMeta?.hash)) {
    return;
  }

  state.similarityMeta = {
    loaded: true,
    available: false,
    sourceUrl: null,
    datasetHash: null,
    message: firstMismatchInfo || "Podobne orzeczenia: brak precomputed pliku dla aktualnie załadowanego zbioru."
  };
  renderSimilarityStatus();
}

function findCaseByDocumentId(documentId) {
  return state.caseByDocumentId.get(normalizeSpace(documentId)) || null;
}

function getSimilarCasesForCase(caseItem) {
  if (!state.similarityMeta.available) return [];
  const documentId = normalizeSpace(caseItem?.document_id);
  if (!documentId) return [];
  const list = state.similarityIndex.get(documentId);
  return Array.isArray(list) ? list.slice(0, 5) : [];
}

function getParagraphDisplayMode() {
  return state.uiPrefs.paragraphDisplayMode === "expanded" ? "expanded" : "collapsed";
}

function isParagraphExpandedByDefault() {
  return getParagraphDisplayMode() === "expanded";
}

function renderParagraphDisplayControl() {
  const mode = getParagraphDisplayMode();
  const isCollapsed = mode === "collapsed";
  const syncPair = (collapsedBtn, expandedBtn) => {
    if (collapsedBtn) {
      collapsedBtn.classList.toggle("active", isCollapsed);
      collapsedBtn.setAttribute("aria-pressed", String(isCollapsed));
    }
    if (expandedBtn) {
      expandedBtn.classList.toggle("active", !isCollapsed);
      expandedBtn.setAttribute("aria-pressed", String(!isCollapsed));
    }
  };
  syncPair(el.paragraphModeCollapsedBtn, el.paragraphModeExpandedBtn);
  syncPair(el.viewerParagraphModeCollapsedBtn, el.viewerParagraphModeExpandedBtn);
}

function setParagraphDisplayMode(mode, options = {}) {
  const nextMode = mode === "expanded" ? "expanded" : "collapsed";
  if (state.uiPrefs.paragraphDisplayMode === nextMode && !options.forceRerender) {
    renderParagraphDisplayControl();
    return;
  }

  state.uiPrefs.paragraphDisplayMode = nextMode;
  state.hitTextOverrides.clear();
  state.viewerParagraphOverrides.clear();
  saveUiPrefs();
  renderParagraphDisplayControl();

  const parsedFallback = state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] };
  if (state.loaded) {
    renderResults(parsedFallback);
  }
  if (state.activeViewerCaseKey) {
    const activeCase = findCaseByKey(state.activeViewerCaseKey);
    if (activeCase) {
      renderCaseViewer(activeCase);
    }
  }
}

function isHitTextExpanded(hitId) {
  if (state.hitTextOverrides.has(hitId)) {
    return Boolean(state.hitTextOverrides.get(hitId));
  }
  return isParagraphExpandedByDefault();
}

function toggleHitTextExpanded(hitId) {
  const defaultExpanded = isParagraphExpandedByDefault();
  const nextExpanded = !isHitTextExpanded(hitId);
  if (nextExpanded === defaultExpanded) {
    state.hitTextOverrides.delete(hitId);
  } else {
    state.hitTextOverrides.set(hitId, nextExpanded);
  }
}

function makeViewerParagraphKey(caseItem, paragraphId) {
  return `${caseKey(caseItem)}::${String(paragraphId || "")}`;
}

function isViewerParagraphExpanded(caseItem, paragraphId) {
  const key = makeViewerParagraphKey(caseItem, paragraphId);
  if (state.viewerParagraphOverrides.has(key)) {
    return Boolean(state.viewerParagraphOverrides.get(key));
  }
  return isParagraphExpandedByDefault();
}

function toggleViewerParagraphExpanded(caseItem, paragraphId) {
  const key = makeViewerParagraphKey(caseItem, paragraphId);
  const defaultExpanded = isParagraphExpandedByDefault();
  const nextExpanded = !isViewerParagraphExpanded(caseItem, paragraphId);
  if (nextExpanded === defaultExpanded) {
    state.viewerParagraphOverrides.delete(key);
  } else {
    state.viewerParagraphOverrides.set(key, nextExpanded);
  }
}

function normalizeSpace(value) {
  return String(value ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeSearchText(value) {
  return normalizeSpace(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function normalizeLegalCitationText(value) {
  return normalizeSearchText(value)
    .replace(/\bart\.?\s*(\d+[a-z]?)/g, "art$1")
    .replace(/\s*§\s*/g, " § ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function fmtNumber(value) {
  return new Intl.NumberFormat("pl-PL").format(value || 0);
}

function parseDateIso(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function formatDate(value) {
  if (!value) return "brak daty";
  const d = parseDateIso(value);
  if (!d) return value;
  return d.toLocaleDateString("pl-PL", {
    year: "numeric",
    month: "short",
    day: "numeric"
  });
}

function plural(value, one, few, many) {
  const n = Math.abs(Number(value) || 0);
  const nMod100 = n % 100;
  const nMod10 = n % 10;
  if (n === 1) return one;
  if (nMod10 >= 2 && nMod10 <= 4 && !(nMod100 >= 12 && nMod100 <= 14)) return few;
  return many;
}

function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(",")}]`;
  }
  const keys = Object.keys(value).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(",")}}`;
}

function safeJsonParse(text, fallback) {
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

function toDomId(value, fallback = "id") {
  const slug = normalizeSearchText(value)
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+/, "")
    .replace(/-+$/, "");
  return slug || fallback;
}

function isTypingTarget(target) {
  if (!target) return false;
  const tag = target.tagName ? target.tagName.toLowerCase() : "";
  if (tag === "input" || tag === "textarea" || tag === "select") return true;
  return Boolean(target.isContentEditable);
}

function caseKey(caseItem) {
  return String(caseItem?.document_id || caseItem?.case_signature || "");
}

function parseAbsoluteUrl(value) {
  const href = normalizeSpace(value);
  if (!href) return null;
  try {
    return new URL(href, window.location.href);
  } catch {
    return null;
  }
}

function canonicalIpoCaseUrl(sourceUrl, caseId, documentId) {
  const caseValue = normalizeSpace(caseId);
  const documentValue = normalizeSpace(documentId);
  if (caseValue && documentValue) {
    return `https://ipo.trybunal.gov.pl/ipo/Sprawa?cid=1&dokument=${encodeURIComponent(documentValue)}&sprawa=${encodeURIComponent(caseValue)}`;
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

function openSourceLink(urlValue) {
  const targetUrl = parseAbsoluteUrl(urlValue);
  if (!targetUrl) return false;

  window.open(targetUrl.href, "_blank", "noopener,noreferrer");
  return true;
}

function handleOpenSourceLinkClick(event) {
  const link = event.target.closest("a[data-action='open-ipo-source']");
  if (!link) return false;

  const href = link.dataset.sourceUrl || link.getAttribute("href");
  if (!href) return false;

  event.preventDefault();
  return openSourceLink(href);
}

function makeHitId(caseItem, hit) {
  return `${caseKey(caseItem)}::${hit.paragraph_id || hit.paragraph_index}`;
}

async function sha256Hex(text) {
  try {
    if (!globalThis.crypto?.subtle) return "";
    const encoded = new TextEncoder().encode(String(text || ""));
    const digest = await globalThis.crypto.subtle.digest("SHA-256", encoded);
    return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  } catch {
    return "";
  }
}

function normalizeRawSection(text) {
  const norm = normalizeSearchText(text);
  if (!norm) return "inne";
  if (norm.includes("komparycja")) return "komparycja";
  if (norm.includes("tenor")) return "tenor";
  if (norm.includes("orzeka")) return "orzeka";
  if (norm.includes("postanawia") || norm.includes("umorzyc postepowanie")) return "postanawia";
  if (norm.includes("zdanie odrebne")) return "zdanie_odrebne";
  if (norm.includes("uzasadnienie")) {
    if (norm.includes("historycz")) return "uzasadnienie_historyczne";
    if (norm.includes("rozpraw") || norm.includes("posiedzen") || norm.includes("przed rozpraw")) return "uzasadnienie_postepowanie";
    if (norm.includes("prawne")) return "uzasadnienie_prawne";
    return "uzasadnienie_ogolne";
  }
  return "inne";
}

function romanPrefix(text) {
  const match = normalizeSpace(text).match(/^([IVXLCDM]+)(?:\b|\s*[-–:])/i);
  return match ? match[1].toUpperCase() : null;
}

function looksLikeTenorParagraph(text, paragraphNumber, paragraphIndex) {
  const norm = normalizeSearchText(text);
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

    const currentNorm = normalizeSearchText(current.text || "");
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
      const nextNorm = normalizeSearchText(nextText);
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
      merged.push({
        ...current,
        text: `Zdanie odrębne ${parts.join(" ")}`.replace(/\s+/g, " ").trim()
      });
      i = lookahead - 1;
      continue;
    }

    merged.push(current);
  }
  return merged;
}

function buildRomanMap(tableOfContents) {
  const map = new Map();
  for (const item of tableOfContents || []) {
    const title = normalizeSpace(item?.text);
    if (!title) continue;
    const prefix = romanPrefix(title);
    if (!prefix) continue;
    const key = normalizeRawSection(title.replace(/^([IVXLCDM]+)\s*[-–:]?\s*/i, ""));
    if (key !== "inne") map.set(prefix, key);
  }
  return map;
}

function normalizeRawDecision(raw) {
  const caseId = raw.ids?.case_id || raw.case_id || null;
  const documentId = raw.ids?.document_id || raw.document_id || null;
  const toc = Array.isArray(raw.table_of_contents) ? raw.table_of_contents : [];
  const romanMap = buildRomanMap(toc);
  const paragraphsRaw = [];
  const judgeNames = (raw.judges || raw.judge_names || []).map((j) => normalizeSpace(j?.name || j)).filter(Boolean);
  const benchInfo = deriveBenchInfo(judgeNames);
  const ipoBenchInfo = deriveIpoBenchInfo(benchInfo.key);
  const decisionTypeRaw = normalizeSpace(raw.decision_type) || "Nieustalony typ";
  const decisionTypeIpo = deriveIpoDecisionTypeInfo(decisionTypeRaw);

  for (const [idx, p] of (raw.paragraphs || []).entries()) {
    const text = normalizeSpace(p.text);
    if (!text) continue;

    let key = normalizeRawSection(p.section || "");
    let confidence = key === "inne" ? 0.35 : 0.95;

    if (key === "inne") {
      const roman = romanPrefix(p.section || "");
      if (roman && romanMap.has(roman)) {
        key = romanMap.get(roman);
        confidence = 0.88;
      }
    }

    if (key === "inne" && looksLikeTenorParagraph(text, p.paragraph_number, idx + 1)) {
      key = "tenor";
      confidence = 0.72;
    }

    paragraphsRaw.push({
      paragraph_id: `${raw.ids?.document_id || "doc"}-${idx + 1}`,
      paragraph_index: p.paragraph_index || idx + 1,
      paragraph_number: normalizeSpace(p.paragraph_number) || null,
      section_key: key,
      section_label: SECTION_META[key]?.label || SECTION_META.inne.label,
      section_confidence: Number(confidence.toFixed(2)),
      section_raw: normalizeSpace(p.section) || null,
      text
    });
  }

  const paragraphs = mergeSplitDissentHeadings(paragraphsRaw);

  const year = (() => {
    const iso = normalizeSpace(raw.decision_date_iso || "");
    if (iso) return Number(iso.slice(0, 4));
    const fromRaw = normalizeSpace(raw.decision_date || "").match(/(19|20)\d{2}/);
    return fromRaw ? Number(fromRaw[0]) : null;
  })();

  return {
    case_id: caseId,
    document_id: documentId,
    case_signature: normalizeSpace(raw.case_signature) || "bez sygnatury",
    decision_type: decisionTypeRaw,
    decision_type_ipo_key: decisionTypeIpo.key,
    decision_type_ipo_label: decisionTypeIpo.label,
    decision_type_ipo_visible: decisionTypeIpo.visible,
    decision_date_raw: normalizeSpace(raw.decision_date) || null,
    decision_date_iso: normalizeSpace(raw.decision_date_iso) || null,
    year,
    topic: normalizeSpace(raw.topic) || null,
    proceeding_intro: normalizeSpace(raw.proceeding_intro || raw.proceedingIntro) || null,
    source_url: canonicalIpoCaseUrl(raw.source_url, caseId, documentId),
    download_url: canonicalIpoDownloadUrl(raw.download?.href || raw.download_url, documentId),
    publication_entries: Array.isArray(raw.publication_entries) ? raw.publication_entries : [],
    metadata: raw.metadata || {},
    judge_names: judgeNames,
    judge_count: benchInfo.count,
    bench_size_key: benchInfo.key,
    bench_size_label: benchInfo.label,
    bench_size_ipo_key: ipoBenchInfo.key,
    bench_size_ipo_label: ipoBenchInfo.label,
    bench_size_ipo_visible: ipoBenchInfo.visible,
    is_full_bench: benchInfo.isFullBench,
    paragraph_count: paragraphs.length,
    table_of_contents: toc,
    normalization_version: NORMALIZATION_VERSION,
    paragraphs
  };
}

function parseDatasetText(text) {
  const source = String(text ?? "");
  const trimmed = source.trim();
  if (!trimmed) return [];

  if (trimmed.startsWith("[")) {
    const parsed = JSON.parse(trimmed);
    if (!Array.isArray(parsed)) {
      throw new Error("JSON musi zawierać tablicę rekordów.");
    }
    return parsed;
  }

  if (trimmed.startsWith("{")) {
    if (!trimmed.includes("\n")) {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) return parsed;
      return [parsed];
    }
  }

  const rows = [];
  const lines = source.split(/\r?\n/);
  for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
    const line = lines[lineIndex];
    if (!line.trim()) continue;
    try {
      rows.push(JSON.parse(line));
    } catch {
      throw new Error(`Błąd formatu JSONL w wierszu ${lineIndex + 1}.`);
    }
  }
  return rows;
}

function validateCaseShape(caseItem) {
  let errors = 0;
  if (!caseItem.case_signature) errors += 1;
  if (!Array.isArray(caseItem.paragraphs) || !caseItem.paragraphs.length) errors += 1;
  for (const paragraph of caseItem.paragraphs || []) {
    if (!paragraph.text) errors += 1;
    if (!paragraph.section_key) errors += 1;
  }
  return errors;
}

function sanitizeCase(caseItem, index = 0) {
  const caseId = caseItem.case_id || caseItem.ids?.case_id || null;
  const documentId = caseItem.document_id || caseItem.ids?.document_id || `doc-${index + 1}`;
  const judgeNames = (caseItem.judge_names || caseItem.judges || []).map((j) => normalizeSpace(j?.name || j)).filter(Boolean);
  const benchInfo = deriveBenchInfo(judgeNames);
  const decisionTypeRaw = normalizeSpace(caseItem.decision_type) || "Nieustalony typ";
  const decisionTypeIpo = (normalizeSpace(caseItem.decision_type_ipo_key) && normalizeSpace(caseItem.decision_type_ipo_label))
    ? {
        key: normalizeSpace(caseItem.decision_type_ipo_key),
        label: normalizeSpace(caseItem.decision_type_ipo_label),
        visible: typeof caseItem.decision_type_ipo_visible === "boolean"
          ? caseItem.decision_type_ipo_visible
          : Boolean(normalizeSpace(caseItem.decision_type_ipo_key))
      }
    : deriveIpoDecisionTypeInfo(decisionTypeRaw);
  const benchKeyRaw = normalizeSpace(caseItem.bench_size_key) || benchInfo.key;
  const benchIpo = (normalizeSpace(caseItem.bench_size_ipo_key) && normalizeSpace(caseItem.bench_size_ipo_label))
    ? {
        key: normalizeSpace(caseItem.bench_size_ipo_key),
        label: normalizeSpace(caseItem.bench_size_ipo_label),
        visible: typeof caseItem.bench_size_ipo_visible === "boolean"
          ? caseItem.bench_size_ipo_visible
          : IPO_BENCH_BY_KEY.has(normalizeSpace(caseItem.bench_size_ipo_key))
      }
    : deriveIpoBenchInfo(benchKeyRaw);
  const safeParagraphsRaw = (caseItem.paragraphs || [])
    .map((paragraph, paragraphIndex) => {
      const text = normalizeSpace(paragraph.text);
      if (!text) return null;

      let key = paragraph.section_key || normalizeRawSection(paragraph.section_raw || paragraph.section || "");
      let confidence = Number(paragraph.section_confidence);
      if (!Number.isFinite(confidence)) confidence = key === "inne" ? 0.35 : 0.95;

      if (key === "inne" && looksLikeTenorParagraph(text, paragraph.paragraph_number, paragraphIndex + 1)) {
        key = "tenor";
        confidence = 0.72;
      }

      return {
        paragraph_id: paragraph.paragraph_id || `${caseItem.document_id || "doc"}-${paragraphIndex + 1}`,
        paragraph_index: paragraph.paragraph_index || paragraphIndex + 1,
        paragraph_number: normalizeSpace(paragraph.paragraph_number) || null,
        section_key: key,
        section_label: paragraph.section_label || SECTION_META[key]?.label || SECTION_META.inne.label,
        section_confidence: Number(confidence.toFixed(2)),
        section_raw: normalizeSpace(paragraph.section_raw || paragraph.section) || null,
        text
      };
    })
    .filter(Boolean);

  const safeParagraphs = mergeSplitDissentHeadings(safeParagraphsRaw);

  const year = Number.isFinite(caseItem.year) ? caseItem.year : (() => {
    const iso = normalizeSpace(caseItem.decision_date_iso || "");
    if (/^\d{4}-\d{2}-\d{2}$/.test(iso)) return Number(iso.slice(0, 4));
    const match = normalizeSpace(caseItem.decision_date_raw || caseItem.decision_date || "").match(/(19|20)\d{2}/);
    return match ? Number(match[0]) : null;
  })();

  return {
    case_id: caseId,
    document_id: documentId,
    case_signature: normalizeSpace(caseItem.case_signature) || `bez sygnatury-${index + 1}`,
    decision_type: decisionTypeRaw,
    decision_type_ipo_key: decisionTypeIpo.key,
    decision_type_ipo_label: decisionTypeIpo.label,
    decision_type_ipo_visible: decisionTypeIpo.visible,
    decision_date_raw: normalizeSpace(caseItem.decision_date_raw || caseItem.decision_date) || null,
    decision_date_iso: normalizeSpace(caseItem.decision_date_iso) || null,
    year,
    topic: normalizeSpace(caseItem.topic) || null,
    proceeding_intro: normalizeSpace(caseItem.proceeding_intro || caseItem.proceedingIntro) || null,
    source_url: canonicalIpoCaseUrl(caseItem.source_url, caseId, documentId),
    download_url: canonicalIpoDownloadUrl(caseItem.download_url || caseItem.download?.href, documentId),
    publication_entries: Array.isArray(caseItem.publication_entries) ? caseItem.publication_entries : [],
    metadata: caseItem.metadata || {},
    judge_names: judgeNames,
    judge_count: Number.isFinite(caseItem.judge_count) ? Number(caseItem.judge_count) : benchInfo.count,
    bench_size_key: benchKeyRaw,
    bench_size_label: normalizeSpace(caseItem.bench_size_label) || benchInfo.label,
    bench_size_ipo_key: benchIpo.key,
    bench_size_ipo_label: benchIpo.label,
    bench_size_ipo_visible: benchIpo.visible,
    is_full_bench: typeof caseItem.is_full_bench === "boolean" ? caseItem.is_full_bench : benchInfo.isFullBench,
    paragraph_count: safeParagraphs.length,
    table_of_contents: Array.isArray(caseItem.table_of_contents) ? caseItem.table_of_contents : [],
    normalization_version: caseItem.normalization_version || NORMALIZATION_VERSION,
    dataset_hash: normalizeSpace(caseItem.dataset_hash) || null,
    dataset_generated_at: normalizeSpace(caseItem.dataset_generated_at) || null,
    paragraphs: safeParagraphs
  };
}

function normalizeAndValidateRows(rows) {
  const sanitized = [];
  let validationErrors = 0;

  for (const [index, row] of (rows || []).entries()) {
    if (!row || typeof row !== "object") {
      validationErrors += 1;
      continue;
    }

    const looksNormalized = Array.isArray(row.paragraphs)
      && (!row.paragraphs.length || Object.prototype.hasOwnProperty.call(row.paragraphs[0], "section_key"));

    const normalized = looksNormalized ? sanitizeCase(row, index) : sanitizeCase(normalizeRawDecision(row), index);
    validationErrors += validateCaseShape(normalized);

    if (!normalized.paragraphs.length) {
      validationErrors += 1;
      continue;
    }
    sanitized.push(normalized);
  }

  return {
    cases: sanitized,
    validationErrors
  };
}

function cacheElements() {
  el.modeSwitchStudent = byId("modeSwitchStudent");
  el.modeSwitchExpert = byId("modeSwitchExpert");

  el.loadSampleBtn = byId("loadSampleBtn");
  el.loadFullBenchDatasetBtn = byId("loadFullBenchDatasetBtn");
  el.fileInput = byId("fileInput");
  el.dropZone = byId("dropZone");
  el.datasetStatus = byId("datasetStatus");
  el.provenanceBanner = byId("provenanceBanner");
  el.cancelLoadBtn = byId("cancelLoadBtn");

  el.searchForm = byId("searchForm");
  el.searchInput = byId("searchInput");
  el.searchBtn = byId("searchBtn");
  el.queryError = byId("queryError");
  el.quickPresets = byId("quickPresets");
  el.paragraphDisplayControl = byId("paragraphDisplayControl");
  el.paragraphModeCollapsedBtn = byId("paragraphModeCollapsedBtn");
  el.paragraphModeExpandedBtn = byId("paragraphModeExpandedBtn");
  el.viewerParagraphModeCollapsedBtn = byId("viewerParagraphModeCollapsedBtn");
  el.viewerParagraphModeExpandedBtn = byId("viewerParagraphModeExpandedBtn");
  el.filtersToggle = byId("filtersToggle");
  el.filtersPanel = byId("filtersPanel");

  el.sectionFilters = byId("sectionFilters");
  el.typeFilters = byId("typeFilters");
  el.benchSizeFilters = byId("benchSizeFilters");
  el.yearFrom = byId("yearFrom");
  el.yearTo = byId("yearTo");
  el.judgeFilter = byId("judgeFilter");
  el.signatureFilter = byId("signatureFilter");

  el.statCases = byId("statCases");
  el.statCasesSub = byId("statCasesSub");
  el.statParagraphs = byId("statParagraphs");
  el.statParagraphsSub = byId("statParagraphsSub");
  el.statTypes = byId("statTypes");
  el.statTypesSub = byId("statTypesSub");
  el.statYears = byId("statYears");
  el.statYearsSub = byId("statYearsSub");

  el.resultsHeader = byId("resultsHeader");
  el.resultsSummary = byId("resultsSummary");
  el.similarityStatus = byId("similarityStatus");
  el.activeFilters = byId("activeFilters");
  el.emptyState = byId("emptyState");
  el.resultsList = byId("resultsList");
  el.pagination = byId("pagination");

  el.sidebar = byId("sidebar");
  el.analyticsSections = byId("analyticsSections");
  el.analyticsTypes = byId("analyticsTypes");
  el.analyticsYears = byId("analyticsYears");
  el.analyticsTerms = byId("analyticsTerms");

  el.folderSummary = byId("folderSummary");
  el.folderCasesList = byId("folderCasesList");
  el.folderParagraphsList = byId("folderParagraphsList");
  el.folderNotes = byId("folderNotes");
  el.timelineList = byId("timelineList");
  el.compareBtn = byId("compareBtn");
  el.compareView = byId("compareView");
  el.compareSlots = byId("compareSlots");
  el.judgmentViewer = byId("judgmentViewer");
  el.judgmentViewerTitle = byId("judgmentViewerTitle");
  el.judgmentViewerMeta = byId("judgmentViewerMeta");
  el.judgmentViewerSourceLink = byId("judgmentViewerSourceLink");
  el.judgmentViewerCloseBtn = byId("judgmentViewerCloseBtn");
  el.viewerKeywordForm = byId("viewerKeywordForm");
  el.viewerKeywordInput = byId("viewerKeywordInput");
  el.viewerKeywordApplyBtn = byId("viewerKeywordApplyBtn");
  el.viewerKeywordClearBtn = byId("viewerKeywordClearBtn");
  el.viewerKeywordSummary = byId("viewerKeywordSummary");
  el.viewerKeywordError = byId("viewerKeywordError");
  el.judgmentViewerToc = byId("judgmentViewerToc");
  el.judgmentViewerContent = byId("judgmentViewerContent");
  el.judgmentViewerBackdrop = byId("judgmentViewerBackdrop");

  el.exportBtn = byId("exportBtn");
  el.quoteExportBtn = byId("quoteExportBtn");
  el.dossierExportBtn = byId("dossierExportBtn");
  el.matrixExportBtn = byId("matrixExportBtn");
  el.clearBtn = byId("clearBtn");
  el.selectedCitationsPanel = byId("selectedCitationsPanel");
  el.selectedCitationsCount = byId("selectedCitationsCount");
  el.selectedCitationsList = byId("selectedCitationsList");
  el.selectedCitationsClearBtn = byId("selectedCitationsClearBtn");
}

function setDatasetStatus(message, kind = "info") {
  el.datasetStatus.textContent = message;
  const palette = {
    info: "#dbeafe",
    success: "#bbf7d0",
    warn: "#fde68a",
    error: "#fecdd3"
  };
  el.datasetStatus.style.color = palette[kind] || palette.info;
}

function setQueryError(message) {
  const text = normalizeSpace(message);
  el.queryError.hidden = !text;
  el.queryError.textContent = text;
}

function setSearchEnabled(enabled) {
  const setDisabled = (node) => {
    if (node) node.disabled = !enabled;
  };
  el.searchForm.classList.toggle("disabled", !enabled);
  el.searchInput.disabled = !enabled;
  el.searchBtn.disabled = !enabled;
  setDisabled(el.paragraphModeCollapsedBtn);
  setDisabled(el.paragraphModeExpandedBtn);
  setDisabled(el.viewerParagraphModeCollapsedBtn);
  setDisabled(el.viewerParagraphModeExpandedBtn);
  el.filtersToggle.disabled = !enabled;
  el.yearFrom.disabled = !enabled;
  el.yearTo.disabled = !enabled;
  el.judgeFilter.disabled = !enabled;
  el.signatureFilter.disabled = !enabled;
  setDisabled(el.exportBtn);
  setDisabled(el.quoteExportBtn);
  setDisabled(el.dossierExportBtn);
  setDisabled(el.matrixExportBtn);
  setDisabled(el.clearBtn);
  setDisabled(el.selectedCitationsClearBtn);

  for (const input of document.querySelectorAll("#sectionFilters input, #typeFilters input, #benchSizeFilters input")) {
    input.disabled = !enabled;
  }

  for (const preset of document.querySelectorAll(".preset-chip")) {
    preset.disabled = !enabled;
  }
}

function setLoadingControls(loading) {
  if (el.cancelLoadBtn) el.cancelLoadBtn.hidden = !loading;
  if (el.loadSampleBtn) el.loadSampleBtn.disabled = loading;
  if (el.loadFullBenchDatasetBtn) el.loadFullBenchDatasetBtn.disabled = loading;
}

function renderProvenanceBanner() {
  const sourceName = state.datasetMeta.sourceName || "nieznane źródło";
  const hash = state.datasetMeta.hash || "brak";
  const generatedAt = state.datasetMeta.generatedAt
    ? formatDate(state.datasetMeta.generatedAt)
    : "brak daty";

  if (!state.loaded) {
    el.provenanceBanner.hidden = true;
    el.provenanceBanner.innerHTML = "";
    return;
  }

  el.provenanceBanner.hidden = false;
  el.provenanceBanner.innerHTML = `
    <strong>Provenance:</strong>
    plik: ${escapeHtml(sourceName)} •
    hash: <code>${escapeHtml(hash.slice(0, 16))}${hash.length > 16 ? "…" : ""}</code> •
    spraw: ${fmtNumber(state.datasetMeta.caseCount)} •
    wygenerowano: ${escapeHtml(generatedAt)}
  `;
}

function buildParagraphIndexSync(cases) {
  const sectionSet = new Set();
  const typeSet = new Set();
  const yearSet = new Set();
  const paragraphIndex = [];

  for (const [caseIndex, caseItem] of cases.entries()) {
    const decisionTypeIpo = (normalizeSpace(caseItem.decision_type_ipo_key) && normalizeSpace(caseItem.decision_type_ipo_label))
      ? {
          key: normalizeSpace(caseItem.decision_type_ipo_key),
          label: normalizeSpace(caseItem.decision_type_ipo_label),
          visible: typeof caseItem.decision_type_ipo_visible === "boolean"
            ? caseItem.decision_type_ipo_visible
            : Boolean(normalizeSpace(caseItem.decision_type_ipo_key))
        }
      : deriveIpoDecisionTypeInfo(caseItem.decision_type);
    if (decisionTypeIpo.visible && decisionTypeIpo.key) {
      typeSet.add(decisionTypeIpo.key);
    }
    if (caseItem.year) yearSet.add(caseItem.year);

    const signatureNorm = normalizeSearchText(caseItem.case_signature || "");
    const typeNorm = normalizeSearchText(`${decisionTypeIpo.label || ""} ${caseItem.decision_type || ""}`);
    const topicNorm = normalizeSearchText(caseItem.topic || "");
    const judgeNorm = (caseItem.judge_names || []).map((name) => normalizeSearchText(name));

    for (const paragraph of caseItem.paragraphs || []) {
      const sectionKey = paragraph.section_key || "inne";
      sectionSet.add(sectionKey);
      paragraphIndex.push({
        caseIndex,
        caseSignatureNorm: signatureNorm,
        decisionTypeNorm: typeNorm,
        topicNorm,
        judgeNorm,
        year: caseItem.year || null,
        sectionKey,
        sectionLabel: paragraph.section_label,
        paragraph,
        textNorm: normalizeSearchText(paragraph.text),
        textLegal: normalizeLegalCitationText(paragraph.text)
      });
    }
  }

  return {
    paragraphIndex,
    sections: [...sectionSet].sort((a, b) => {
      const al = SECTION_META[a]?.label || a;
      const bl = SECTION_META[b]?.label || b;
      return al.localeCompare(bl, "pl");
    }),
    decisionTypes: IPO_DECISION_TYPES
      .filter((entry) => typeSet.has(entry.key))
      .concat(IPO_DECISION_TYPES.filter((entry) => !typeSet.has(entry.key))),
    benchSizes: [...IPO_BENCH_SIZES],
    years: [...yearSet].sort((a, b) => a - b)
  };
}

function indexDatasetCases(cases) {
  if (typeof Worker === "undefined") {
    return Promise.resolve(buildParagraphIndexSync(cases));
  }

  return new Promise((resolve) => {
    const worker = new Worker("assets/search-worker.js");
    let resolved = false;

    worker.onmessage = (event) => {
      const payload = event?.data || {};
      if (payload.type === "index-progress") {
        setDatasetStatus(
          `Indeksowanie danych: ${fmtNumber(payload.processedCases || 0)} / ${fmtNumber(payload.totalCases || 0)} spraw...`,
          "info"
        );
        return;
      }

      if (payload.type === "indexed") {
        if (resolved) return;
        resolved = true;
        worker.terminate();
        resolve({
          paragraphIndex: payload.paragraphIndex || [],
          sections: payload.sections || [],
          decisionTypes: payload.decisionTypes || [],
          benchSizes: payload.benchSizes || [],
          years: payload.years || []
        });
      }
    };

    worker.onerror = () => {
      if (resolved) return;
      resolved = true;
      worker.terminate();
      resolve(buildParagraphIndexSync(cases));
    };

    worker.postMessage({ type: "index", cases });
  });
}

function renderFilterOptions() {
  el.sectionFilters.innerHTML = state.sections
    .map((key) => {
      const label = SECTION_META[key]?.label || key;
      return `<label class="checkbox-row"><input type="checkbox" data-filter="section" value="${escapeHtml(key)}"> <span>${escapeHtml(label)}</span></label>`;
    })
    .join("");

  const typeCounts = state.cases.reduce((acc, caseItem) => {
    const key = normalizeSpace(caseItem.decision_type_ipo_key)
      || deriveIpoDecisionTypeInfo(caseItem.decision_type).key;
    const visible = typeof caseItem.decision_type_ipo_visible === "boolean"
      ? caseItem.decision_type_ipo_visible
      : Boolean(key && IPO_DECISION_TYPE_BY_KEY.has(key));
    if (!visible || !key) return acc;
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});

  el.typeFilters.innerHTML = state.decisionTypes
    .map((type) => {
      const count = typeCounts[type.key] || 0;
      return `<label class="checkbox-row"><input type="checkbox" data-filter="type" value="${escapeHtml(type.key)}"> <span>${escapeHtml(type.label)} (${fmtNumber(count)})</span></label>`;
    })
    .join("");

  const benchCounts = state.cases.reduce((acc, caseItem) => {
    const derivedBench = deriveBenchInfo(caseItem.judge_names || []);
    const ipo = (normalizeSpace(caseItem.bench_size_ipo_key) && normalizeSpace(caseItem.bench_size_ipo_label))
      ? {
          key: normalizeSpace(caseItem.bench_size_ipo_key),
          visible: typeof caseItem.bench_size_ipo_visible === "boolean"
            ? caseItem.bench_size_ipo_visible
            : IPO_BENCH_BY_KEY.has(normalizeSpace(caseItem.bench_size_ipo_key))
        }
      : deriveIpoBenchInfo(caseItem.bench_size_key || derivedBench.key);
    if (!ipo.visible) return acc;
    const key = ipo.key;
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});

  el.benchSizeFilters.innerHTML = state.benchSizes
    .map((bench) => {
      const count = benchCounts[bench.key] || 0;
      return `<label class="checkbox-row"><input type="checkbox" data-filter="bench" value="${escapeHtml(bench.key)}"> <span>${escapeHtml(bench.label)} (${fmtNumber(count)})</span></label>`;
    })
    .join("");

  if (state.years.length) {
    el.yearFrom.placeholder = String(state.years[0]);
    el.yearTo.placeholder = String(state.years[state.years.length - 1]);
  }
}

function updateDatasetStats() {
  const totalCases = state.cases.length;
  const totalParagraphs = state.paragraphIndex.length;
  const datedCases = state.cases.filter((caseItem) => Number.isFinite(caseItem.year)).length;
  const fullBenchCases = state.cases.filter((caseItem) => Boolean(caseItem.is_full_bench)).length;
  const avgParagraphsPerCase = totalCases ? (totalParagraphs / totalCases) : 0;

  el.statCases.textContent = fmtNumber(totalCases);
  el.statParagraphs.textContent = fmtNumber(totalParagraphs);
  el.statTypes.textContent = fmtNumber(state.decisionTypes.length);
  el.statYears.textContent = state.years.length
    ? `${state.years[0]}–${state.years[state.years.length - 1]}`
    : "-";

  if (el.statCasesSub) {
    el.statCasesSub.textContent = `${fmtNumber(fullBenchCases)} w pełnym składzie`;
  }
  if (el.statParagraphsSub) {
    el.statParagraphsSub.textContent = `średnio ${avgParagraphsPerCase.toFixed(1)} na sprawę`;
  }
  if (el.statTypesSub) {
    el.statTypesSub.textContent = `IPO: ${fmtNumber(state.decisionTypes.length)} kategorie`;
  }
  if (el.statYearsSub) {
    el.statYearsSub.textContent = `${fmtNumber(datedCases)} datowanych spraw`;
  }
}

function buildSectionAliasMap() {
  const map = new Map();
  for (const [key, meta] of Object.entries(SECTION_META)) {
    map.set(normalizeSearchText(key), key);
    map.set(normalizeSearchText(meta.label), key);
  }
  map.set("holding", "tenor");
  map.set("sentencja", "tenor");
  map.set("dissent", "zdanie_odrebne");
  map.set("uzasadnienie prawne", "uzasadnienie_prawne");
  return map;
}

const SECTION_ALIAS_MAP = buildSectionAliasMap();

function resolveDecisionTypeFilterValue(value) {
  const normalized = normalizeSearchText(value);
  if (!normalized) return null;

  const byKey = IPO_DECISION_TYPES.find((entry) => normalizeSearchText(entry.key) === normalized);
  if (byKey) return byKey.key;

  const byLabel = IPO_DECISION_TYPES.find((entry) => normalizeSearchText(entry.label) === normalized);
  if (byLabel) return byLabel.key;

  return deriveIpoDecisionTypeInfo(value).key || null;
}

function resolveBenchFilterValue(value) {
  const normalized = normalizeSearchText(value);
  if (!normalized) return null;

  const byKey = IPO_BENCH_SIZES.find((entry) => normalizeSearchText(entry.key) === normalized);
  if (byKey) return byKey.key;

  const byLabel = IPO_BENCH_SIZES.find((entry) => normalizeSearchText(entry.label) === normalized);
  if (byLabel) return byLabel.key;

  if (normalized.includes("pelny")) return "pelny_sklad";
  if (normalized.includes("piecio")) return "piecioosobowa";
  if (normalized.includes("trojo")) return "trojosobowa";
  return null;
}

function resolveSectionQueryValue(value) {
  const normalized = normalizeSearchText(value);
  if (!normalized) return [];

  if (SECTION_ALIAS_MAP.has(normalized)) {
    return [SECTION_ALIAS_MAP.get(normalized)];
  }

  if (normalized === "holding only" || normalized === "holding") {
    return ["tenor", "orzeka", "postanawia"];
  }
  if (normalized === "reasoning only") {
    return ["uzasadnienie_prawne"];
  }
  if (normalized === "dissent only") {
    return ["zdanie_odrebne"];
  }

  const parts = normalized.split(",").map((entry) => entry.trim()).filter(Boolean);
  if (parts.length > 1) {
    return [...new Set(parts.flatMap((entry) => resolveSectionQueryValue(entry)))];
  }

  const fuzzy = Object.keys(SECTION_META).filter((key) => {
    const keyNorm = normalizeSearchText(key);
    const labelNorm = normalizeSearchText(SECTION_META[key].label);
    return keyNorm.includes(normalized) || labelNorm.includes(normalized);
  });
  return [...new Set(fuzzy)];
}

function readQuotedToken(source, startIndex) {
  let i = startIndex + 1;
  let value = "";
  while (i < source.length) {
    const char = source[i];
    if (char === "\\" && source[i + 1] === '"') {
      value += '"';
      i += 2;
      continue;
    }
    if (char === '"') {
      return {
        value,
        nextIndex: i + 1,
        quoted: true
      };
    }
    value += char;
    i += 1;
  }
  throw new Error("Niezamknięty cudzysłów w zapytaniu.");
}

function readBareToken(source, startIndex) {
  let i = startIndex;
  let value = "";
  while (i < source.length) {
    const char = source[i];
    if (/\s/.test(char) || char === "(" || char === ")") break;
    value += char;
    i += 1;
  }
  return {
    value,
    nextIndex: i,
    quoted: false
  };
}

function buildTextOperand(value, quoted = false) {
  const raw = normalizeSpace(value);
  const norm = normalizeSearchText(raw);
  const legal = normalizeLegalCitationText(raw);
  if (!norm) {
    throw new Error("Puste wyrażenie tekstowe w zapytaniu.");
  }
  return {
    kind: "text",
    raw,
    norm,
    legal,
    quoted
  };
}

function buildYearOperand(raw) {
  const value = normalizeSpace(raw);
  if (/^\d{4}$/.test(value)) {
    return {
      kind: "field",
      field: "year",
      mode: "exact",
      year: Number(value),
      raw: value
    };
  }

  const rangeMatch = value.match(/^(\d{4})\s*-\s*(\d{4})$/);
  if (rangeMatch) {
    const from = Number(rangeMatch[1]);
    const to = Number(rangeMatch[2]);
    return {
      kind: "field",
      field: "year",
      mode: "range",
      from: Math.min(from, to),
      to: Math.max(from, to),
      raw: value
    };
  }

  throw new Error("Operator rok: wymaga wartości 4-cyfrowej (np. rok:2025) lub zakresu (np. rok:2020-2025).");
}

function buildFieldOperand(field, value) {
  const raw = normalizeSpace(value);
  if (!raw) {
    throw new Error(`Brak wartości dla operatora pola: ${field}.`);
  }

  if (field === "year") {
    return buildYearOperand(raw);
  }

  if (field === "section") {
    const sectionValues = resolveSectionQueryValue(raw);
    if (!sectionValues.length) {
      throw new Error(`Nieznana wartość dla sekcja: ${raw}`);
    }
    return {
      kind: "field",
      field,
      raw,
      sectionValues
    };
  }

  if (field === "decisionType") {
    const mappedKey = resolveDecisionTypeFilterValue(raw);
    const mappedLabel = mappedKey ? getDecisionTypeIpoLabelByKey(mappedKey) : "";
    const decisionTypeNorms = [...new Set(
      [mappedLabel, mappedKey || "", raw.replaceAll("_", " ")]
        .map((entry) => normalizeSearchText(entry))
        .filter(Boolean)
    )];
    return {
      kind: "field",
      field,
      raw,
      norm: normalizeSearchText(raw),
      decisionTypeNorms
    };
  }

  return {
    kind: "field",
    field,
    raw,
    norm: normalizeSearchText(raw)
  };
}

function tokenizeExpression(source) {
  const tokens = [];
  let i = 0;

  while (i < source.length) {
    const char = source[i];
    if (/\s/.test(char)) {
      i += 1;
      continue;
    }

    if (char === "(") {
      tokens.push({ type: "LPAREN" });
      i += 1;
      continue;
    }

    if (char === ")") {
      tokens.push({ type: "RPAREN" });
      i += 1;
      continue;
    }

    const fieldMatch = source.slice(i).match(/^([A-Za-ząćęłńóśźżĄĆĘŁŃÓŚŹŻ]+):/u);
    if (fieldMatch) {
      const rawField = fieldMatch[1];
      const canonical = FIELD_OPERATOR_MAP[normalizeSearchText(rawField)];
      if (!canonical) {
        throw new Error(`Nieobsługiwany operator pola: ${rawField}:`);
      }

      i += fieldMatch[0].length;
      while (i < source.length && /\s/.test(source[i])) i += 1;
      if (i >= source.length) {
        throw new Error(`Brak wartości po operatorze ${rawField}:`);
      }

      const parsedToken = source[i] === '"'
        ? readQuotedToken(source, i)
        : readBareToken(source, i);
      i = parsedToken.nextIndex;

      tokens.push({
        type: "OPERAND",
        operand: buildFieldOperand(canonical, parsedToken.value)
      });
      continue;
    }

    if (char === '"') {
      const parsedToken = readQuotedToken(source, i);
      i = parsedToken.nextIndex;
      tokens.push({
        type: "OPERAND",
        operand: buildTextOperand(parsedToken.value, true)
      });
      continue;
    }

    const parsedToken = readBareToken(source, i);
    i = parsedToken.nextIndex;

    const tokenValue = normalizeSpace(parsedToken.value);
    if (!tokenValue) continue;

    const upper = tokenValue.toUpperCase();
    if (upper === "AND" || upper === "OR" || upper === "NOT") {
      tokens.push({ type: "OP", op: upper });
      continue;
    }

    if (tokenValue.includes(":")) {
      const unknownField = tokenValue.split(":", 1)[0];
      throw new Error(`Nieznany operator pola: ${unknownField}:`);
    }

    tokens.push({
      type: "OPERAND",
      operand: buildTextOperand(tokenValue, false)
    });
  }

  return tokens;
}

function tokenStartsOperand(token) {
  return token.type === "OPERAND" || token.type === "LPAREN" || (token.type === "OP" && token.op === "NOT");
}

function tokenEndsOperand(token) {
  return token.type === "OPERAND" || token.type === "RPAREN";
}

function insertImplicitAnd(tokens) {
  const expanded = [];
  for (const token of tokens) {
    const previous = expanded[expanded.length - 1];
    if (previous && tokenEndsOperand(previous) && tokenStartsOperand(token)) {
      expanded.push({ type: "OP", op: "AND" });
    }
    expanded.push(token);
  }
  return expanded;
}

function operatorPrecedence(op) {
  if (op === "NOT") return 3;
  if (op === "AND") return 2;
  return 1;
}

function operatorAssociativity(op) {
  if (op === "NOT") return "right";
  return "left";
}

function toRpn(tokens) {
  const output = [];
  const stack = [];

  for (const token of tokens) {
    if (token.type === "OPERAND") {
      output.push(token);
      continue;
    }

    if (token.type === "LPAREN") {
      stack.push(token);
      continue;
    }

    if (token.type === "RPAREN") {
      let foundLParen = false;
      while (stack.length) {
        const top = stack.pop();
        if (top.type === "LPAREN") {
          foundLParen = true;
          break;
        }
        output.push(top);
      }
      if (!foundLParen) {
        throw new Error("Niezgodne nawiasy w zapytaniu.");
      }
      continue;
    }

    if (token.type === "OP") {
      while (stack.length) {
        const top = stack[stack.length - 1];
        if (top.type !== "OP") break;

        const currentPrec = operatorPrecedence(token.op);
        const topPrec = operatorPrecedence(top.op);
        const assoc = operatorAssociativity(token.op);

        const shouldPop = assoc === "left"
          ? currentPrec <= topPrec
          : currentPrec < topPrec;

        if (!shouldPop) break;
        output.push(stack.pop());
      }
      stack.push(token);
    }
  }

  while (stack.length) {
    const top = stack.pop();
    if (top.type === "LPAREN" || top.type === "RPAREN") {
      throw new Error("Niezgodne nawiasy w zapytaniu.");
    }
    output.push(top);
  }

  return output;
}

function validateRpn(rpn) {
  let depth = 0;
  for (const token of rpn) {
    if (token.type === "OPERAND") {
      depth += 1;
      continue;
    }

    if (token.type === "OP" && token.op === "NOT") {
      if (depth < 1) {
        throw new Error("Nieprawidłowe użycie operatora NOT.");
      }
      continue;
    }

    if (token.type === "OP" && (token.op === "AND" || token.op === "OR")) {
      if (depth < 2) {
        throw new Error(`Nieprawidłowe użycie operatora ${token.op}.`);
      }
      depth -= 1;
    }
  }

  if (depth !== 1) {
    throw new Error("Nieprawidłowa składnia zapytania.");
  }
}

function parseQuery(query) {
  const source = normalizeSpace(query);
  const canonicalSource = source.replace(/\bart\.?\s*(\d+[a-z]?)/gi, "art$1");
  if (!source) {
    return {
      source,
      hasQuery: false,
      rpn: [],
      textOperands: [],
      allTerms: [],
      highlightTerms: []
    };
  }

  const tokens = tokenizeExpression(canonicalSource);
  if (!tokens.length) {
    return {
      source,
      hasQuery: false,
      rpn: [],
      textOperands: [],
      allTerms: [],
      highlightTerms: []
    };
  }

  const expanded = insertImplicitAnd(tokens);
  const rpn = toRpn(expanded);
  validateRpn(rpn);

  const textOperands = rpn
    .filter((token) => token.type === "OPERAND" && token.operand.kind === "text")
    .map((token) => token.operand);

  return {
    source,
    hasQuery: true,
    rpn,
    textOperands,
    allTerms: [...new Set(textOperands.map((operand) => operand.norm).filter(Boolean))],
    highlightTerms: [...new Set(textOperands.map((operand) => operand.raw).filter(Boolean))]
  };
}

function collectFilters() {
  const sections = [...document.querySelectorAll('input[data-filter="section"]:checked')].map((input) => input.value);
  const types = [...document.querySelectorAll('input[data-filter="type"]:checked')].map((input) => input.value);
  const benches = [...document.querySelectorAll('input[data-filter="bench"]:checked')].map((input) => input.value);

  const yearFrom = Number.parseInt(el.yearFrom.value, 10);
  const yearTo = Number.parseInt(el.yearTo.value, 10);

  return {
    sections,
    types,
    benches,
    yearFrom: Number.isFinite(yearFrom) ? yearFrom : null,
    yearTo: Number.isFinite(yearTo) ? yearTo : null,
    judge: normalizeSearchText(el.judgeFilter.value),
    signature: normalizeSearchText(el.signatureFilter.value)
  };
}

function casePassesFilters(caseItem, filters) {
  if (filters.types.length) {
    const typeKey = normalizeSpace(caseItem.decision_type_ipo_key)
      || deriveIpoDecisionTypeInfo(caseItem.decision_type).key;
    if (!filters.types.includes(typeKey)) {
      return false;
    }
  }
  if (filters.yearFrom && (!caseItem.year || caseItem.year < filters.yearFrom)) {
    return false;
  }
  if (filters.yearTo && (!caseItem.year || caseItem.year > filters.yearTo)) {
    return false;
  }
  if (filters.judge) {
    const judgeNorm = (caseItem.judge_names || []).map((name) => normalizeSearchText(name));
    if (!judgeNorm.some((name) => name.includes(filters.judge))) {
      return false;
    }
  }
  if (filters.signature) {
    const signatureNorm = normalizeSearchText(caseItem.case_signature || "");
    if (!signatureNorm.includes(filters.signature)) {
      return false;
    }
  }
  if (filters.benches.length) {
    const derivedBench = deriveBenchInfo(caseItem.judge_names || []);
    const benchIpo = (normalizeSpace(caseItem.bench_size_ipo_key) && normalizeSpace(caseItem.bench_size_ipo_label))
      ? {
          key: normalizeSpace(caseItem.bench_size_ipo_key),
          visible: typeof caseItem.bench_size_ipo_visible === "boolean"
            ? caseItem.bench_size_ipo_visible
            : IPO_BENCH_BY_KEY.has(normalizeSpace(caseItem.bench_size_ipo_key))
        }
      : deriveIpoBenchInfo(caseItem.bench_size_key || derivedBench.key);
    if (!benchIpo.visible || !filters.benches.includes(benchIpo.key)) {
      return false;
    }
  }
  return true;
}

function evaluateFieldOperand(operand, entry) {
  switch (operand.field) {
    case "signature":
      return entry.caseSignatureNorm.includes(operand.norm);
    case "judge":
      return entry.judgeNorm.some((name) => name.includes(operand.norm));
    case "decisionType":
      if (Array.isArray(operand.decisionTypeNorms) && operand.decisionTypeNorms.length) {
        return operand.decisionTypeNorms.some((norm) => entry.decisionTypeNorm.includes(norm));
      }
      return entry.decisionTypeNorm.includes(operand.norm);
    case "thesis":
      return entry.topicNorm.includes(operand.norm);
    case "section":
      return operand.sectionValues.includes(entry.sectionKey);
    case "year":
      if (!entry.year) return false;
      if (operand.mode === "exact") return entry.year === operand.year;
      if (operand.mode === "range") return entry.year >= operand.from && entry.year <= operand.to;
      return false;
    default:
      return false;
  }
}

function evaluateOperand(operand, entry) {
  if (operand.kind === "text") {
    return entry.textNorm.includes(operand.norm) || entry.textLegal.includes(operand.legal);
  }
  return evaluateFieldOperand(operand, entry);
}

function evaluateQuery(entry, parsedQuery) {
  if (!parsedQuery?.hasQuery) return true;
  const stack = [];

  for (const token of parsedQuery.rpn) {
    if (token.type === "OPERAND") {
      stack.push(evaluateOperand(token.operand, entry));
      continue;
    }

    if (token.type === "OP" && token.op === "NOT") {
      const val = stack.pop();
      stack.push(!val);
      continue;
    }

    const right = stack.pop();
    const left = stack.pop();
    if (token.op === "AND") {
      stack.push(Boolean(left && right));
    } else {
      stack.push(Boolean(left || right));
    }
  }

  return stack.length ? Boolean(stack[0]) : true;
}

function countOccurrences(text, term) {
  if (!text || !term) return 0;
  let count = 0;
  let idx = text.indexOf(term);
  while (idx >= 0) {
    count += 1;
    idx = text.indexOf(term, idx + term.length);
  }
  return count;
}

function computeScore(entry, parsedQuery) {
  if (!parsedQuery?.textOperands?.length) {
    const sectionWeight = SECTION_SCORE_WEIGHT[entry.sectionKey] || 1;
    return {
      score: sectionWeight,
      explain: `sekcja=${sectionWeight.toFixed(2)}`
    };
  }

  let tf = 0;
  let phraseBonus = 0;

  for (const operand of parsedQuery.textOperands) {
    const term = operand.legal || operand.norm;
    const occurrences = countOccurrences(entry.textLegal, term);
    tf += occurrences;
    if (operand.quoted && occurrences > 0) {
      phraseBonus += 1.4;
    }
  }

  const sectionWeight = SECTION_SCORE_WEIGHT[entry.sectionKey] || 1;
  const base = Math.max(tf, 1) * sectionWeight;
  const score = Number((base + phraseBonus).toFixed(3));

  return {
    score,
    explain: `tf=${tf} • sekcja=${sectionWeight.toFixed(2)}${phraseBonus ? ` • fraza=+${phraseBonus.toFixed(1)}` : ""}`
  };
}

function buildSnippet(text, terms, maxLen = 360) {
  const source = normalizeSpace(text);
  if (!source) return "";
  if (!terms.length) {
    return source.length <= maxLen ? source : `${source.slice(0, maxLen).trimEnd()}...`;
  }
  if (source.length <= maxLen) return source;

  const lower = normalizeLegalCitationText(source);
  let firstIdx = -1;
  for (const term of terms) {
    const idx = lower.indexOf(term);
    if (idx >= 0 && (firstIdx === -1 || idx < firstIdx)) {
      firstIdx = idx;
    }
  }

  if (firstIdx === -1) {
    return source.slice(0, maxLen).trimEnd() + (source.length > maxLen ? "..." : "");
  }

  const start = Math.max(0, firstIdx - 120);
  const end = Math.min(source.length, start + maxLen);
  const prefix = start > 0 ? "..." : "";
  const suffix = end < source.length ? "..." : "";
  return prefix + source.slice(start, end).trim() + suffix;
}

function isHeadingLikeResultParagraph(text) {
  const source = normalizeSpace(text);
  if (!source) return false;
  if (source.length > 140) return false;

  const norm = normalizeSearchText(source);
  const wordCount = source.split(/\s+/).filter(Boolean).length;
  if (wordCount > 12) return false;
  if (/\bart\.?\s*\d/i.test(source)) return false;

  if (/^(\d+|[ivxlcdm]+)[\.\)]\s+/i.test(source) && /[.:]$/.test(source)) return true;
  if (/^§\s*\d+/.test(source) && /[.:]$/.test(source)) return true;
  if (/^[\p{L}\p{N}\s\-–,]+[:.]$/u.test(source) && wordCount <= 8) return true;

  return /^(przedmiot|stan faktyczny|ocena|uzasadnienie|konkluzja|wniosek)\b/.test(norm) && /[.:]$/.test(source);
}

function mergeHeadingHitsForResults(hits) {
  const merged = [];
  for (let i = 0; i < hits.length; i += 1) {
    const current = hits[i];
    const next = hits[i + 1];

    const canMerge = Boolean(
      current
      && next
      && current.section_key === next.section_key
      && Number(next.paragraph_index) === Number(current.paragraph_index) + 1
      && isHeadingLikeResultParagraph(current.text)
      && !isHeadingLikeResultParagraph(next.text)
    );

    if (!canMerge) {
      merged.push(current);
      continue;
    }

    const headingText = normalizeSpace(current.text);
    const snippetBody = normalizeSpace(next.snippet || next.text);
    merged.push({
      ...next,
      merged_heading: headingText,
      merged_text: `${headingText}\n${normalizeSpace(next.text)}`,
      merged_snippet: `${headingText}\n${snippetBody}`
    });
    i += 1;
  }
  return merged;
}

function buildNormalizedHighlightMap(text) {
  const source = String(text ?? "").replace(/\u00a0/g, " ");
  let normalized = "";
  const map = [];

  for (let i = 0; i < source.length; i += 1) {
    const lowered = source[i].toLowerCase();
    const stripped = lowered.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
    for (const ch of stripped) {
      normalized += ch;
      map.push(i);
    }
  }

  return { source, normalized, map };
}

function isNormalizedWordChar(char) {
  return /[a-z0-9]/.test(char || "");
}

function collectHighlightRanges(sourceText, parsedQuery) {
  if (!parsedQuery?.textOperands?.length) return [];

  const { source, normalized, map } = buildNormalizedHighlightMap(sourceText);
  if (!source || !normalized) return [];

  const patterns = [...new Set(
    parsedQuery.textOperands
      .flatMap((operand) => [normalizeSearchText(operand.raw || ""), operand.norm])
      .filter((term) => term && term.length >= 3)
  )]
    .sort((a, b) => b.length - a.length);

  const ranges = [];
  for (const pattern of patterns) {
    const singleWord = !/\s/.test(pattern);
    let idx = normalized.indexOf(pattern);
    while (idx >= 0) {
      let startNorm = idx;
      let endNorm = idx + pattern.length;

      if (singleWord) {
        while (startNorm > 0 && isNormalizedWordChar(normalized[startNorm - 1])) startNorm -= 1;
        while (endNorm < normalized.length && isNormalizedWordChar(normalized[endNorm])) endNorm += 1;
      }

      const start = map[startNorm];
      const end = map[endNorm - 1] + 1;
      if (Number.isInteger(start) && Number.isInteger(end) && end > start) {
        ranges.push([start, end]);
      }

      idx = normalized.indexOf(pattern, idx + Math.max(1, pattern.length));
    }
  }

  if (!ranges.length) return [];
  ranges.sort((a, b) => a[0] - b[0] || a[1] - b[1]);

  const merged = [ranges[0]];
  for (let i = 1; i < ranges.length; i += 1) {
    const current = ranges[i];
    const prev = merged[merged.length - 1];
    if (current[0] <= prev[1]) {
      prev[1] = Math.max(prev[1], current[1]);
    } else {
      merged.push(current);
    }
  }

  return merged;
}

function highlight(text, parsedQuery) {
  const source = String(text ?? "");
  const ranges = collectHighlightRanges(source, parsedQuery);
  if (!ranges.length) return escapeHtml(source);

  let html = "";
  let cursor = 0;
  for (const [start, end] of ranges) {
    html += escapeHtml(source.slice(cursor, start));
    html += `<mark>${escapeHtml(source.slice(start, end))}</mark>`;
    cursor = end;
  }
  html += escapeHtml(source.slice(cursor));
  return html;
}

function parseQueryWithHandling(rawQuery) {
  try {
    const parsed = parseQuery(rawQuery);
    setQueryError("");
    return parsed;
  } catch (error) {
    setQueryError(error.message || "Błąd składni zapytania.");
    return null;
  }
}

function parseViewerKeywordQuery(rawQuery) {
  const query = normalizeSpace(rawQuery);
  if (!query) {
    return {
      ok: true,
      parsed: {
        source: "",
        hasQuery: false,
        rpn: [],
        textOperands: [],
        allTerms: [],
        highlightTerms: []
      },
      error: ""
    };
  }

  try {
    return {
      ok: true,
      parsed: parseQuery(query),
      error: ""
    };
  } catch (error) {
    return {
      ok: false,
      parsed: null,
      error: error?.message || "Nieprawidłowa składnia filtra."
    };
  }
}

function mergeHighlightQueries(...queries) {
  const mergedOperands = [];
  const seen = new Set();

  for (const query of queries) {
    for (const operand of query?.textOperands || []) {
      const key = `${operand.norm || ""}::${operand.raw || ""}::${operand.quoted ? "1" : "0"}`;
      if (seen.has(key)) continue;
      seen.add(key);
      mergedOperands.push(operand);
    }
  }

  return {
    textOperands: mergedOperands
  };
}

function buildQueryEvaluationEntry(caseItem, paragraph) {
  const decisionTypeIpoLabel = normalizeSpace(caseItem.decision_type_ipo_label)
    || deriveIpoDecisionTypeInfo(caseItem.decision_type).label
    || "";
  return {
    caseSignatureNorm: normalizeSearchText(caseItem.case_signature || ""),
    decisionTypeNorm: normalizeSearchText(`${decisionTypeIpoLabel} ${caseItem.decision_type || ""}`),
    topicNorm: normalizeSearchText(caseItem.topic || ""),
    judgeNorm: (caseItem.judge_names || []).map((name) => normalizeSearchText(name)),
    year: caseItem.year || null,
    sectionKey: paragraph.section_key || "inne",
    textNorm: normalizeSearchText(paragraph.text || ""),
    textLegal: normalizeLegalCitationText(paragraph.text || "")
  };
}

function paragraphMatchesViewerFilter(caseItem, paragraph, parsedQuery) {
  if (!parsedQuery?.hasQuery) return true;
  const entry = buildQueryEvaluationEntry(caseItem, paragraph);
  return evaluateQuery(entry, parsedQuery);
}

function runSearch(options = {}) {
  if (!state.loaded) return;

  const forceBrowse = Boolean(options.forceBrowse);
  const query = normalizeSpace(el.searchInput.value);
  const parsedQuery = parseQueryWithHandling(query);
  const filters = collectFilters();

  state.currentFilters = filters;
  state.currentPage = 1;
  state.query = query;
  state.currentParsedQuery = parsedQuery;
  state.hitTextOverrides.clear();

  if (!parsedQuery && query) {
    state.currentMode = "search";
    state.currentResults = [];
    state.currentHits = 0;
    renderResults({ hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
    renderActiveFilters(filters);
    renderSidebar({ hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
    updateUrlState(filters);
    return;
  }

  const hasQuery = Boolean(parsedQuery?.hasQuery);
  const hasParagraphFilter = filters.sections.length > 0;

  if (!hasQuery && !hasParagraphFilter && forceBrowse) {
    state.currentMode = "browse";
  } else if (!hasQuery && !hasParagraphFilter) {
    state.currentMode = "browse";
  } else {
    state.currentMode = "search";
  }

  if (state.currentMode === "browse") {
    const groups = [];
    for (const [caseIndex, caseItem] of state.cases.entries()) {
      if (!casePassesFilters(caseItem, filters)) continue;
      groups.push({ caseItem, caseIndex, hits: [], hitCount: 0, topScore: 0 });
    }
    state.currentResults = groups;
    state.currentHits = 0;
  } else {
    const grouped = new Map();
    let totalHits = 0;

    for (const entry of state.paragraphIndex) {
      const caseItem = state.cases[entry.caseIndex];
      if (!casePassesFilters(caseItem, filters)) continue;
      if (filters.sections.length && !filters.sections.includes(entry.sectionKey)) continue;
      if (!evaluateQuery(entry, parsedQuery)) continue;

      const scoring = computeScore(entry, parsedQuery);
      const snippetTerms = (parsedQuery?.textOperands || [])
        .map((operand) => operand.legal || operand.norm)
        .filter(Boolean);
      const hit = {
        paragraph_id: entry.paragraph.paragraph_id,
        paragraph_index: entry.paragraph.paragraph_index,
        paragraph_number: entry.paragraph.paragraph_number,
        section_key: entry.sectionKey,
        section_label: entry.sectionLabel,
        section_confidence: entry.paragraph.section_confidence,
        text: entry.paragraph.text,
        snippet: buildSnippet(entry.paragraph.text, snippetTerms, 420),
        score: scoring.score,
        score_explain: scoring.explain
      };

      if (!grouped.has(entry.caseIndex)) {
        grouped.set(entry.caseIndex, {
          caseItem,
          caseIndex: entry.caseIndex,
          hits: [],
          hitCount: 0,
          topScore: 0
        });
      }

      const bucket = grouped.get(entry.caseIndex);
      bucket.hits.push(hit);
      bucket.hitCount += 1;
      bucket.topScore = Math.max(bucket.topScore, scoring.score);
      totalHits += 1;
    }

    const results = [...grouped.values()]
      .map((group) => {
        group.hits.sort((a, b) => b.score - a.score || a.paragraph_index - b.paragraph_index || String(a.paragraph_id).localeCompare(String(b.paragraph_id)));
        return group;
      })
      .sort((a, b) => {
        if (b.topScore !== a.topScore) return b.topScore - a.topScore;
        const da = parseDateIso(a.caseItem.decision_date_iso)?.getTime() || 0;
        const db = parseDateIso(b.caseItem.decision_date_iso)?.getTime() || 0;
        if (db !== da) return db - da;
        return String(a.caseItem.case_signature).localeCompare(String(b.caseItem.case_signature), "pl");
      });

    state.currentResults = results;
    state.currentHits = totalHits;
  }

  renderResults(parsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
  renderActiveFilters(filters);
  renderSidebar(parsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
  renderCaseFolder();
  updateUrlState(filters);
  saveQueryHistory(query);
}

function renderActiveFilters(filters) {
  const chips = [];
  if (filters.sections.length) chips.push(`Sekcje: ${filters.sections.length}`);
  if (filters.types.length) {
    const labels = filters.types.map((key) => getDecisionTypeIpoLabelByKey(key)).filter(Boolean);
    chips.push(`Typy: ${labels.join(", ")}`);
  }
  if (filters.benches.length) {
    const labels = filters.benches.map((key) => getBenchLabelByKey(key)).filter(Boolean);
    chips.push(`Skład: ${labels.join(", ")}`);
  }
  if (filters.yearFrom || filters.yearTo) chips.push(`Rok: ${filters.yearFrom || "*"}–${filters.yearTo || "*"}`);
  if (filters.judge) chips.push(`Sędzia: ${filters.judge}`);
  if (filters.signature) chips.push(`Sygnatura: ${filters.signature}`);
  if (state.query) chips.push(`Zapytanie: ${state.query}`);

  el.activeFilters.innerHTML = chips.map((chip) => `<span class="filter-chip">${escapeHtml(chip)}</span>`).join("");
}

function formatResultSummary(caseCount, hitCount, mode) {
  if (!caseCount) return "Brak wyników dla podanych kryteriów.";
  if (mode === "browse") {
    return `${fmtNumber(caseCount)} ${plural(caseCount, "sprawa", "sprawy", "spraw")} w trybie przeglądania`;
  }
  return `${fmtNumber(hitCount)} ${plural(hitCount, "trafienie", "trafienia", "trafień")} w ${fmtNumber(caseCount)} ${plural(caseCount, "sprawie", "sprawach", "sprawach")}`;
}

function isCasePinned(caseItem) {
  return Boolean(state.caseFolder.cases[caseKey(caseItem)]);
}

function isParagraphPinned(caseItem, hit) {
  return Boolean(state.caseFolder.paragraphs[makeHitId(caseItem, hit)]);
}

function formatSimilarityScore(score) {
  const normalized = Number(score) || 0;
  return normalized.toFixed(3);
}

function renderSimilarCasesList(caseItem, similarEntries, options = {}) {
  if (!state.similarityMeta.available) return "";
  const sectionId = normalizeSpace(options.sectionId || "");
  const sectionIdAttr = sectionId ? ` id="${escapeHtml(sectionId)}"` : "";

  const expanded = false;

  if (!similarEntries.length) {
    return `
      <section class="viewer-similar-block"${sectionIdAttr}>
        <header class="viewer-similar-head">
          <h4>Podobne orzeczenia</h4>
          <span>0 pozycji</span>
        </header>
        <p class="similar-cases-empty">Brak podobnych orzeczeń powyżej progu podobieństwa.</p>
      </section>
    `;
  }

  const entriesHtml = similarEntries
    .map((entry) => {
      const targetCase = findCaseByDocumentId(entry.document_id);
      const reasonItems = (entry.reasons || []).slice(0, getPageDashboardMode() === "student" ? 2 : 3);
      return `
        <article class="similar-case-item">
          <div class="similar-case-head">
            <p class="similar-case-signature">${escapeHtml(entry.case_signature)}</p>
            <span class="similar-case-score">${escapeHtml(formatSimilarityScore(entry.score))}</span>
          </div>
          <p class="similar-case-meta">${escapeHtml(entry.decision_type_ipo_label || "—")} • ${escapeHtml(formatDate(entry.decision_date_iso || ""))}</p>
          ${reasonItems.length ? `<ul class="similar-case-reasons">${reasonItems.map((reason) => `<li>${escapeHtml(reason)}</li>`).join("")}</ul>` : ""}
          <div class="similar-case-actions">
            <button type="button" class="mini-btn" data-action="viewer-open-similar-case" data-target-document-id="${escapeHtml(entry.document_id)}" data-source-url="${escapeHtml(entry.source_url || "")}">${targetCase ? "Otwórz podgląd" : "Otwórz w IPO"}</button>
          </div>
        </article>
      `;
    })
    .join("");

  return `
    <section class="viewer-similar-block"${sectionIdAttr}>
      <header class="viewer-similar-head">
        <h4>Podobne orzeczenia</h4>
        <div class="viewer-similar-controls">
          <span>${fmtNumber(similarEntries.length)} pozycji</span>
          <button
            type="button"
            class="mini-btn viewer-similar-toggle"
            data-action="viewer-toggle-similar"
            aria-expanded="${expanded ? "true" : "false"}"
          >${expanded ? "Zwiń" : "Rozwiń"}</button>
        </div>
      </header>
      <div class="similar-cases-list" ${expanded ? "" : "hidden"}>
        ${entriesHtml}
      </div>
    </section>
  `;
}

function renderResults(parsedQuery) {
  const caseCount = state.currentResults.length;
  const totalPages = Math.max(1, Math.ceil(caseCount / PAGE_SIZE));
  state.currentPage = Math.min(state.currentPage, totalPages);

  const start = (state.currentPage - 1) * PAGE_SIZE;
  const end = start + PAGE_SIZE;
  const pageItems = state.currentResults.slice(start, end);

  el.resultsHeader.hidden = false;
  renderSimilarityStatus();
  el.emptyState.hidden = caseCount > 0;
  el.resultsList.innerHTML = "";

  if (!caseCount) {
    el.resultsSummary.textContent = "Brak wyników dla podanych kryteriów.";
    renderPagination(totalPages);
    updateActionButtons();
    renderSelectedCitationsPanel();
    return;
  }

  el.resultsSummary.textContent = formatResultSummary(caseCount, state.currentHits, state.currentMode);

  for (const group of pageItems) {
    const caseItem = group.caseItem;
    const caseId = caseKey(caseItem);
    const expanded = state.expandedCases.has(caseId);
    const mergedHits = mergeHeadingHitsForResults(group.hits);

    const hitsToShow = state.currentMode === "search"
      ? (expanded ? mergedHits : mergedHits.slice(0, 3))
      : [];

    const hitsHtml = hitsToShow
      .map((hit) => {
        const color = SECTION_META[hit.section_key]?.color || SECTION_META.inne.color;
        const numberLabel = hit.paragraph_number || `§ ${hit.paragraph_index}`;
        const hitId = makeHitId(caseItem, hit);
        const selected = state.selectedHits.has(hitId);
        const pinned = isParagraphPinned(caseItem, hit);
        const displaySnippet = hit.merged_snippet || hit.snippet;
        const displayText = hit.merged_text || hit.text;
        const mergedHeading = hit.merged_heading || "";
        const hasLongText = normalizeSpace(displayText).length > normalizeSpace(displaySnippet).length + 8;
        const isTextExpanded = isHitTextExpanded(hitId);
        return `
          <li class="hit-item ${selected ? "selected" : ""}">
            <div class="hit-head">
              <span class="section-chip" style="background:${escapeHtml(color)}">${escapeHtml(hit.section_label)}</span>
              <span class="hit-number">${escapeHtml(numberLabel)}</span>
            </div>
            <p class="hit-text hit-text-preview" ${(isTextExpanded && hasLongText) ? "hidden" : ""}>${highlight(displaySnippet, parsedQuery)}</p>
            ${hasLongText ? `<p class="hit-text hit-text-full" ${isTextExpanded ? "" : "hidden"}>${highlight(displayText, parsedQuery)}</p>` : ""}
            <p class="rank-explain">Ranking: ${escapeHtml(hit.score_explain || "-")}</p>
            <div class="hit-actions">
              <button type="button" class="mini-btn" data-action="toggle-hit-select" data-case-index="${group.caseIndex}" data-hit-id="${escapeHtml(hitId)}">${selected ? "Odznacz cytat" : "Wybierz cytat"}</button>
              <button type="button" class="mini-btn" data-action="open-case-view" data-case-index="${group.caseIndex}" data-hit-id="${escapeHtml(hitId)}">Pełny wyrok</button>
              <button type="button" class="mini-btn" data-action="copy-citation" data-case-index="${group.caseIndex}" data-hit-id="${escapeHtml(hitId)}">Cytuj</button>
              <button type="button" class="mini-btn" data-action="copy-paragraph" data-case-index="${group.caseIndex}" data-hit-id="${escapeHtml(hitId)}" data-merged-heading="${escapeHtml(mergedHeading)}">Kopiuj akapit</button>
              <button type="button" class="mini-btn" data-action="copy-url" data-case-index="${group.caseIndex}" data-hit-id="${escapeHtml(hitId)}">Kopiuj URL</button>
              <button type="button" class="mini-btn" data-action="pin-paragraph" data-case-index="${group.caseIndex}" data-hit-id="${escapeHtml(hitId)}">${pinned ? "Odepnij akapit" : "Przypnij akapit"}</button>
              ${hasLongText ? `<button type="button" class="mini-btn" data-action="toggle-hit-text" data-case-index="${group.caseIndex}" data-hit-id="${escapeHtml(hitId)}">${isTextExpanded ? "Zwiń akapit" : "Rozwiń akapit"}</button>` : ""}
            </div>
          </li>
        `;
      })
      .join("");

    const hitListHtml = state.currentMode === "search"
      ? `<ul class="hit-list">${hitsHtml}</ul>`
      : "";

    const toggleHtml = state.currentMode === "search" && mergedHits.length > 3
      ? `<button class="toggle-hits" type="button" data-action="toggle-case-expand" data-case-key="${escapeHtml(caseId)}">${expanded ? "Pokaż mniej" : `Pokaż wszystkie trafienia (${mergedHits.length})`}</button>`
      : "";

    const judges = (caseItem.judge_names || []).slice(0, 5).join(", ");
    const benchInfo = deriveBenchInfo(caseItem.judge_names || []);
    const benchIpoLabel = normalizeSpace(caseItem.bench_size_ipo_label) || deriveIpoBenchInfo(caseItem.bench_size_key || benchInfo.key).label;
    const benchIpoVisible = typeof caseItem.bench_size_ipo_visible === "boolean"
      ? caseItem.bench_size_ipo_visible
      : IPO_BENCH_BY_KEY.has(normalizeSpace(caseItem.bench_size_ipo_key) || normalizeSpace(caseItem.bench_size_key));
    const benchLabel = benchIpoVisible ? benchIpoLabel : (caseItem.bench_size_label || benchInfo.label);
    const isFullBench = typeof caseItem.is_full_bench === "boolean" ? caseItem.is_full_bench : benchInfo.isFullBench;
    const isBenchLabelFullBench = /pełny\s+skład/i.test(benchLabel || "");
    const showBenchLabelPill = !(isFullBench && isBenchLabelFullBench);

    const card = document.createElement("article");
    card.className = `case-card${isFullBench ? " case-card-full-bench" : ""}`;
    card.innerHTML = `
      <header class="case-head">
        <div class="case-title-row">
          <h3 class="case-title">${escapeHtml(caseItem.case_signature)}</h3>
          <span class="case-date">${escapeHtml(formatDate(caseItem.decision_date_iso || caseItem.decision_date_raw))}</span>
        </div>
        <div class="case-meta">
          <span class="meta-pill">${escapeHtml(caseItem.decision_type)}</span>
          <span class="meta-pill">${fmtNumber(caseItem.paragraph_count)} akapitów</span>
          ${showBenchLabelPill ? `<span class="meta-pill">${escapeHtml(benchLabel)}</span>` : ""}
          ${isFullBench ? `<span class="meta-pill full-bench-pill">Pełny skład</span>` : ""}
          ${caseItem.year ? `<span class="meta-pill">${caseItem.year}</span>` : ""}
        </div>
        ${caseItem.topic ? `<p class="case-topic"><strong>Dotyczy:</strong> ${escapeHtml(caseItem.topic)}</p>` : ""}
        ${judges ? `<p class="case-topic"><strong>Skład:</strong> ${escapeHtml(judges)}</p>` : ""}
      </header>
      ${hitListHtml}
      <footer class="case-foot">
        <div class="case-foot-left">
          ${toggleHtml}
          <button type="button" class="mini-btn" data-action="open-case-view" data-case-index="${group.caseIndex}">Podgląd pełnego wyroku</button>
          <button type="button" class="mini-btn" data-action="pin-case" data-case-index="${group.caseIndex}">${isCasePinned(caseItem) ? "Odepnij sprawę" : "Przypnij sprawę"}</button>
        </div>
        <div>
          ${caseItem.source_url ? `<a href="${escapeHtml(caseItem.source_url)}" data-action="open-ipo-source" data-source-url="${escapeHtml(caseItem.source_url)}" target="_blank" rel="noopener noreferrer">Otwórz w IPO</a>` : ""}
        </div>
      </footer>
    `;

    el.resultsList.appendChild(card);
  }

  renderPagination(totalPages);
  updateActionButtons();
  renderSelectedCitationsPanel();
}

function renderPagination(totalPages) {
  if (totalPages <= 1) {
    el.pagination.hidden = true;
    el.pagination.innerHTML = "";
    return;
  }

  const currentPage = Math.max(1, Math.min(state.currentPage, totalPages));
  const pageSet = new Set([1, totalPages, currentPage, currentPage - 1, currentPage + 1, currentPage - 2, currentPage + 2]);

  if (currentPage <= 4) {
    for (let page = 1; page <= Math.min(6, totalPages); page += 1) pageSet.add(page);
  }
  if (currentPage >= totalPages - 3) {
    for (let page = Math.max(1, totalPages - 5); page <= totalPages; page += 1) pageSet.add(page);
  }

  const pages = [...pageSet]
    .filter((page) => Number.isFinite(page) && page >= 1 && page <= totalPages)
    .sort((a, b) => a - b);

  const parts = [];
  const navButton = (label, page, disabled, extraClass = "") => (
    `<button type="button" class="page-btn page-nav ${extraClass}" ${disabled ? "disabled aria-disabled='true'" : `data-page='${page}'`} aria-label="${label}">${label}</button>`
  );

  parts.push(navButton("«", 1, currentPage === 1, "page-first"));
  parts.push(navButton("‹", currentPage - 1, currentPage === 1, "page-prev"));

  let previousPage = 0;
  for (const page of pages) {
    if (previousPage && page - previousPage > 1) {
      parts.push("<span class='page-ellipsis' aria-hidden='true'>…</span>");
    }
    parts.push(`<button type="button" class="page-btn ${page === currentPage ? "active" : ""}" data-page="${page}">${page}</button>`);
    previousPage = page;
  }

  parts.push(navButton("›", currentPage + 1, currentPage === totalPages, "page-next"));
  parts.push(navButton("»", totalPages, currentPage === totalPages, "page-last"));
  parts.push(`<span class="page-meta" aria-live="polite">strona ${currentPage} / ${totalPages}</span>`);

  el.pagination.hidden = false;
  el.pagination.innerHTML = parts.join("");
}

function countBy(items, keyGetter) {
  const map = new Map();
  for (const item of items) {
    const key = keyGetter(item);
    if (!key) continue;
    map.set(key, (map.get(key) || 0) + 1);
  }
  return [...map.entries()].sort((a, b) => b[1] - a[1]);
}

function renderBarList(container, rows, colorFn) {
  if (!rows.length) {
    container.innerHTML = "<p class='case-topic'>brak danych</p>";
    return;
  }
  const max = rows[0][1] || 1;
  const total = rows.reduce((sum, row) => sum + Number(row[1] || 0), 0);
  container.innerHTML = rows
    .slice(0, 10)
    .map(([label, value]) => {
      const pct = Math.max(6, Math.round((value / max) * 100));
      const color = colorFn(label);
      const share = total ? ((value / total) * 100).toFixed(1).replace(".", ",") : "0,0";
      return `
        <div class="bar-item">
          <div class="bar-meta-row">
            <span class="bar-label" title="${escapeHtml(label)}">${escapeHtml(label)}</span>
            <strong class="bar-value">${fmtNumber(value)}</strong>
          </div>
          <div class="bar-track">
            <div class="bar-fill" style="width:${pct}%;background:${escapeHtml(color)}"></div>
          </div>
          <span class="bar-share">${share}%</span>
        </div>
      `;
    })
    .join("");
}

function sortedEntriesFromMap(map) {
  return [...map.entries()].sort((a, b) => b[1] - a[1] || String(a[0]).localeCompare(String(b[0]), "pl"));
}

function collectTermsFromText(text, excludedTermsSet = null) {
  const tokens = normalizeSearchText(text)
    .split(/[^\p{L}\p{N}_-]+/u)
    .filter((token) => token.length >= 4 && !STOPWORDS.has(token));

  if (!excludedTermsSet || !excludedTermsSet.size) return tokens;
  return tokens.filter((token) => !excludedTermsSet.has(token));
}

function buildResultsSidebarStats(parsedQuery) {
  const groups = state.currentResults;
  if (!groups.length) {
    return {
      sections: [],
      types: [],
      years: [],
      terms: [],
      empty: true
    };
  }

  if (state.currentMode === "search") {
    const hitRows = groups.flatMap((group) => group.hits.map((hit) => ({ hit, caseItem: group.caseItem })));
    const sections = countBy(hitRows, (row) => row.hit.section_label);
    const types = countBy(groups, (group) => (
      normalizeSpace(group.caseItem.decision_type_ipo_label)
      || deriveIpoDecisionTypeInfo(group.caseItem.decision_type).label
      || group.caseItem.decision_type
    ));
    const years = countBy(groups, (group) => (group.caseItem.year ? String(group.caseItem.year) : "brak daty"));

    const termFreq = new Map();
    const excludedTerms = new Set(parsedQuery?.allTerms || []);
    for (const row of hitRows) {
      const words = collectTermsFromText(row.hit.text, excludedTerms);
      for (const word of words) {
        termFreq.set(word, (termFreq.get(word) || 0) + 1);
      }
    }

    return {
      sections,
      types,
      years,
      terms: sortedEntriesFromMap(termFreq).slice(0, 24),
      empty: false
    };
  }

  const sections = countBy(
    groups.flatMap((group) => group.caseItem.paragraphs || []),
    (paragraph) => paragraph.section_label
  );
  const types = countBy(groups, (group) => (
    normalizeSpace(group.caseItem.decision_type_ipo_label)
    || deriveIpoDecisionTypeInfo(group.caseItem.decision_type).label
    || group.caseItem.decision_type
  ));
  const years = countBy(groups, (group) => (group.caseItem.year ? String(group.caseItem.year) : "brak daty"));

  return {
    sections,
    types,
    years,
    terms: [],
    empty: false,
    browseMode: true
  };
}

function renderSidebarTerms(stats) {
  if (!el.analyticsTerms) return;

  if (stats.browseMode) {
    el.analyticsTerms.innerHTML = "<span class='term-pill'>Tryb przeglądania (bez zapytania)</span>";
    return;
  }

  if (!stats.terms.length) {
    el.analyticsTerms.innerHTML = "<span class='term-pill'>brak danych</span>";
    return;
  }

  el.analyticsTerms.innerHTML = stats.terms
    .map(([word, count]) => `<span class="term-pill" title="${count}">${escapeHtml(word)}</span>`)
    .join("");
}

function renderSidebar(parsedQuery) {
  if (!state.loaded) {
    el.sidebar.hidden = true;
    return;
  }

  if (!state.currentResults.length) {
    el.sidebar.hidden = true;
    return;
  }

  el.sidebar.hidden = false;
  const stats = buildResultsSidebarStats(parsedQuery);

  renderBarList(el.analyticsSections, stats.sections, (label) => {
    const key = Object.keys(SECTION_META).find((k) => SECTION_META[k].label === label);
    return SECTION_META[key]?.color || "#94a3b8";
  });
  renderBarList(el.analyticsTypes, stats.types, () => "#bf0d2e");
  renderBarList(el.analyticsYears, stats.years, () => "#334155");
  renderSidebarTerms(stats);

  if (stats.empty) {
    el.analyticsTerms.innerHTML = "<span class='term-pill'>Brak wyników wyszukiwania</span>";
  }
}

function saveCaseFolder() {
  localStorage.setItem(STORAGE_KEYS.caseFolder, JSON.stringify(state.caseFolder));
}

function saveUiPrefs() {
  localStorage.setItem(STORAGE_KEYS.uiPrefs, JSON.stringify(state.uiPrefs));
}

function saveQueryHistory(query) {
  const normalized = normalizeSpace(query);
  if (!normalized) return;

  const existing = state.savedQueries.filter((entry) => entry !== normalized);
  state.savedQueries = [normalized, ...existing].slice(0, 25);
  localStorage.setItem(STORAGE_KEYS.savedQueries, JSON.stringify(state.savedQueries));
}

function togglePinCase(caseItem) {
  const key = caseKey(caseItem);
  if (!key) return;

  if (state.caseFolder.cases[key]) {
    delete state.caseFolder.cases[key];
  } else {
    state.caseFolder.cases[key] = {
      case_key: key,
      document_id: caseItem.document_id,
      case_signature: caseItem.case_signature,
      decision_date_iso: caseItem.decision_date_iso,
      decision_date_raw: caseItem.decision_date_raw,
      decision_type: caseItem.decision_type,
      topic: caseItem.topic,
      source_url: caseItem.source_url,
      paragraph_count: caseItem.paragraph_count
    };
  }

  saveCaseFolder();
  renderCaseFolder();
  renderResults(state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
}

function togglePinParagraph(caseItem, hit) {
  const hitId = makeHitId(caseItem, hit);
  if (state.caseFolder.paragraphs[hitId]) {
    delete state.caseFolder.paragraphs[hitId];
  } else {
    state.caseFolder.paragraphs[hitId] = {
      hit_id: hitId,
      case_key: caseKey(caseItem),
      case_signature: caseItem.case_signature,
      document_id: caseItem.document_id,
      decision_date_iso: caseItem.decision_date_iso,
      decision_type: caseItem.decision_type,
      paragraph_id: hit.paragraph_id,
      paragraph_index: hit.paragraph_index,
      paragraph_number: hit.paragraph_number,
      section_key: hit.section_key,
      section_label: hit.section_label,
      source_url: caseItem.source_url,
      text: hit.text
    };
  }

  saveCaseFolder();
  renderCaseFolder();
  renderResults(state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
}

function extractPublicationCitation(caseItem) {
  const raw = (caseItem.publication_entries || [])
    .map((entry) => normalizeSpace(entry?.text || entry))
    .find(Boolean);

  if (!raw) return "";
  const clean = raw.replace(/\.$/, "");

  const letterYearMatch = clean.match(/^OTK\s+ZU\s+([A-Z])\/(\d{4}),\s*poz\.\s*(\d+)/i);
  if (letterYearMatch) {
    return `OTK ZU ${letterYearMatch[2]}, nr ${letterYearMatch[1]}, poz. ${letterYearMatch[3]}`;
  }

  const standardMatch = clean.match(/^OTK\s+ZU\s+(\d{4}),\s*nr\s*([^,]+),\s*poz\.\s*(\d+)/i);
  if (standardMatch) {
    return `OTK ZU ${standardMatch[1]}, nr ${normalizeSpace(standardMatch[2])}, poz. ${standardMatch[3]}`;
  }

  return clean;
}

function formatDecisionKindForCitation(decisionType) {
  const norm = normalizeSearchText(decisionType || "");
  if (norm.includes("wyrok")) return "wyrok TK";
  if (norm.includes("postanow")) return "postanowienie TK";
  return "orzeczenie TK";
}

function formatCitationDate(caseItem) {
  const date = parseDateIso(caseItem.decision_date_iso || caseItem.decision_date_raw);
  if (date) {
    return `${date.toLocaleDateString("pl-PL", { day: "numeric", month: "long", year: "numeric" })} r.`;
  }

  const raw = normalizeSpace(caseItem.decision_date_raw || "");
  if (!raw) return "brak daty";
  return /\br\.$/i.test(raw) ? raw : `${raw} r.`;
}

function normalizeCitationSignature(signature) {
  return normalizeSpace(signature)
    .replace(/\s*\*+\s*$/u, "")
    .trim();
}

function buildCaseCitation(caseItem) {
  const prefix = `${formatDecisionKindForCitation(caseItem.decision_type)} z dnia ${formatCitationDate(caseItem)}`;
  const signature = normalizeCitationSignature(caseItem.case_signature || "");
  const publication = extractPublicationCitation(caseItem);
  return [prefix, signature, publication].filter(Boolean).join(", ");
}

function buildParagraphCitation(caseItem, hit) {
  const paragraphLabel = hit.paragraph_number || `§ ${hit.paragraph_index}`;
  return `${buildCaseCitation(caseItem)}, ${hit.section_label}, ${paragraphLabel}`;
}

function renderCaseFolder() {
  const caseEntries = Object.values(state.caseFolder.cases);
  const paragraphEntries = Object.values(state.caseFolder.paragraphs)
    .sort((a, b) => (a.case_signature || "").localeCompare(b.case_signature || "", "pl") || (a.paragraph_index || 0) - (b.paragraph_index || 0));

  el.folderSummary.textContent = `${fmtNumber(caseEntries.length)} ${plural(caseEntries.length, "sprawa", "sprawy", "spraw")}, ${fmtNumber(paragraphEntries.length)} ${plural(paragraphEntries.length, "akapit", "akapity", "akapitów")}.`;

  el.folderCasesList.innerHTML = caseEntries.length
    ? caseEntries
      .map((entry) => `
        <article class="folder-item">
          <strong>${escapeHtml(entry.case_signature)}</strong><br>
          <span>${escapeHtml(entry.decision_type || "-")} • ${escapeHtml(formatDate(entry.decision_date_iso || entry.decision_date_raw))}</span>
          <div class="folder-item-actions">
            <button type="button" class="mini-btn" data-action="remove-folder-case" data-case-key="${escapeHtml(entry.case_key)}">Usuń</button>
          </div>
        </article>
      `)
      .join("")
    : "<p class='case-topic'>Brak przypiętych spraw.</p>";

  el.folderParagraphsList.innerHTML = paragraphEntries.length
    ? paragraphEntries
      .slice(0, 12)
      .map((entry) => `
        <article class="folder-item">
          <strong>${escapeHtml(entry.case_signature)}</strong> • ${escapeHtml(entry.section_label)} • ${escapeHtml(entry.paragraph_number || `§ ${entry.paragraph_index}`)}
          <div class="folder-item-actions">
            <button type="button" class="mini-btn" data-action="remove-folder-paragraph" data-hit-id="${escapeHtml(entry.hit_id)}">Usuń</button>
          </div>
        </article>
      `)
      .join("")
    : "<p class='case-topic'>Brak przypiętych akapitów.</p>";

  const timeline = caseEntries
    .slice()
    .sort((a, b) => {
      const da = parseDateIso(a.decision_date_iso)?.getTime() || 0;
      const db = parseDateIso(b.decision_date_iso)?.getTime() || 0;
      return da - db;
    });

  el.timelineList.innerHTML = timeline.length
    ? timeline
      .map((entry) => `<div class="timeline-item"><strong>${escapeHtml(formatDate(entry.decision_date_iso || entry.decision_date_raw))}</strong> — ${escapeHtml(entry.case_signature)}</div>`)
      .join("")
    : "<p class='case-topic'>Brak danych chronologicznych.</p>";

  el.compareBtn.disabled = caseEntries.length < 2;
  updateActionButtons();
  renderCompareView();
}

function findCaseByKey(key) {
  return state.cases.find((caseItem) => caseKey(caseItem) === key) || null;
}

function buildViewerSections(caseItem) {
  const sections = [];
  const sectionMap = new Map();

  for (const paragraph of caseItem.paragraphs || []) {
    const key = paragraph.section_key || "inne";
    if (!sectionMap.has(key)) {
      const section = {
        key,
        label: paragraph.section_label || SECTION_META[key]?.label || SECTION_META.inne.label,
        id: `viewer-section-${toDomId(`${caseItem.document_id || caseItem.case_signature}-${key}-${sections.length + 1}`, `sekcja-${sections.length + 1}`)}`,
        paragraphs: []
      };
      sections.push(section);
      sectionMap.set(key, section);
    }

    sectionMap.get(key).paragraphs.push(paragraph);
  }

  return sections;
}

function viewerParagraphDomId(paragraph, sectionId = "sekcja") {
  return `viewer-paragraph-${toDomId(paragraph.paragraph_id || `${sectionId}-${paragraph.paragraph_index}`)}`;
}

function buildSectionTocEntry(section, label, sectionKey) {
  const first = section.paragraphs[0];
  const last = section.paragraphs[section.paragraphs.length - 1];
  const firstLabel = first?.paragraph_number || (first ? `§ ${first.paragraph_index}` : "");
  const lastLabel = last?.paragraph_number || (last ? `§ ${last.paragraph_index}` : "");
  const range = firstLabel && lastLabel ? (firstLabel === lastLabel ? firstLabel : `${firstLabel}–${lastLabel}`) : "";

  return {
    label,
    sectionKey,
    targetId: section.id,
    count: section.paragraphs.length,
    range,
    level: 1
  };
}

function extractDissentJudgeName(text) {
  const cleaned = normalizeSpace(text);
  if (!cleaned) return "sędzia TK";

  const judgeMatch = cleaned.match(/^Zdanie odrębne sędziego TK\s+(.+?)(?=\s+do\b|\s+z dnia\b|,|$)/i);
  if (judgeMatch?.[1]) {
    return normalizeSpace(judgeMatch[1]);
  }

  const fallbackMatch = cleaned.match(/^Zdanie odrębne\s+(.+?)(?=\s+do\b|\s+z dnia\b|,|$)/i);
  if (fallbackMatch?.[1]) {
    return normalizeSpace(fallbackMatch[1]);
  }

  return cleaned.length > 72 ? `${cleaned.slice(0, 69)}...` : cleaned;
}

function buildDissentJudgeTocEntries(section) {
  if (!section || section.key !== "zdanie_odrebne") return [];

  const items = [];
  for (const paragraph of section.paragraphs) {
    const norm = normalizeSearchText(paragraph.text || "");
    if (!norm.startsWith("zdanie odrebne sedziego tk")) continue;

    const paragraphLabel = paragraph.paragraph_number || `§ ${paragraph.paragraph_index}`;
    items.push({
      label: extractDissentJudgeName(paragraph.text),
      sectionKey: section.key,
      targetId: viewerParagraphDomId(paragraph, section.id),
      count: 0,
      range: paragraphLabel,
      meta: `ak. ${paragraphLabel}`,
      level: 2
    });
  }

  return items;
}

function buildViewerToc(caseItem, sections) {
  const toc = [];
  const seenTargets = new Set();
  const sectionIndex = new Map(sections.map((section) => [section.key, section]));

  const appendSection = (section, label, sectionKey) => {
    if (!section || seenTargets.has(section.id)) return;
    seenTargets.add(section.id);
    toc.push(buildSectionTocEntry(section, label, sectionKey));
    toc.push(...buildDissentJudgeTocEntries(section));
  };

  for (const item of caseItem.table_of_contents || []) {
    const rawText = normalizeSpace(item?.text || "");
    const key = normalizeSpace(item?.key || "") || normalizeRawSection(rawText);
    const targetSection = sectionIndex.get(key);
    appendSection(targetSection, rawText || targetSection?.label || "", key);
  }

  for (const section of sections) {
    appendSection(section, section.label, section.key);
  }

  return toc;
}

function viewerParagraphToHit(paragraph) {
  return {
    paragraph_id: paragraph.paragraph_id,
    paragraph_index: paragraph.paragraph_index,
    paragraph_number: paragraph.paragraph_number,
    section_key: paragraph.section_key,
    section_label: paragraph.section_label,
    text: paragraph.text
  };
}

function renderViewerToc(toc) {
  if (!toc.length) {
    el.judgmentViewerToc.innerHTML = "<p class='case-topic'>Brak spisu treści.</p>";
    return;
  }

  let topLevelIndex = 0;
  el.judgmentViewerToc.innerHTML = toc
    .map((item) => {
      const activeClass = state.activeViewerTocTarget === item.targetId ? " active" : "";
      const isSubItem = item.level === 2;
      const subClass = isSubItem ? " sub" : "";
      if (!isSubItem) topLevelIndex += 1;

      const labelPrefix = isSubItem ? "- " : `${topLevelIndex}. `;
      const metaText = item.meta
        || (item.count > 0
          ? `${fmtNumber(item.count)} ak.${item.range ? ` • ${item.range}` : ""}`
          : (item.range || ""));

      return `
        <button type="button" class="viewer-toc-link${activeClass}${subClass}" data-action="viewer-goto-section" data-target-id="${escapeHtml(item.targetId)}">
          <span class="viewer-toc-label">${labelPrefix}${escapeHtml(item.label)}</span>
          ${metaText ? `<span class="viewer-toc-meta">${escapeHtml(metaText)}</span>` : ""}
        </button>
      `;
    })
    .join("");
}

function buildJudgmentIdentitySection(caseItem) {
  const targetId = `viewer-identity-${toDomId(caseKey(caseItem) || caseItem.case_signature || "wyrok")}`;
  const typeLabel = normalizeSpace(caseItem.decision_type || "Orzeczenie").toUpperCase();
  const dateLabel = normalizeSpace(caseItem.decision_date_raw) || formatDate(caseItem.decision_date_iso || caseItem.decision_date_raw);
  const signatureLabel = normalizeSpace(caseItem.case_signature || "");
  const proceedingIntro = normalizeSpace(caseItem.proceeding_intro || "");
  const judges = (caseItem.judge_names || []).filter(Boolean);
  const publication = (caseItem.publication_entries || [])
    .map((entry) => normalizeSpace(entry?.text || entry))
    .find(Boolean);

  const judgesHtml = judges.length
    ? `
      <p class="viewer-identity-subtitle">Trybunał Konstytucyjny w składzie:</p>
      <ul class="viewer-identity-judges">
        ${judges.map((name) => `<li>${escapeHtml(name)}</li>`).join("")}
      </ul>
    `
    : "";

  const publicationHtml = publication
    ? `<p class="viewer-identity-publication"><strong>Publikacja:</strong> ${escapeHtml(publication)}</p>`
    : "";

  const proceedingHtml = proceedingIntro
    ? `<p class="viewer-identity-proceeding">${escapeHtml(proceedingIntro)}</p>`
    : "";

  return {
    targetId,
    tocItem: {
      label: "Komparycja",
      targetId,
      count: 0,
      range: "",
      meta: "typ • data • sygnatura • skład"
    },
    html: `
      <section class="viewer-identity" id="${escapeHtml(targetId)}">
        <p class="viewer-identity-kicker">Komparycja</p>
        <h3 class="viewer-identity-type">${escapeHtml(typeLabel)}</h3>
        <p class="viewer-identity-date">z dnia ${escapeHtml(dateLabel)}</p>
        <p class="viewer-identity-signature">Sygn. akt ${escapeHtml(signatureLabel)}</p>
        <p class="viewer-identity-state">W IMIENIU RZECZYPOSPOLITEJ POLSKIEJ</p>
        ${judgesHtml}
        ${proceedingHtml}
        ${publicationHtml}
      </section>
    `
  };
}

function renderViewerKeywordUi(totalParagraphs, visibleParagraphs) {
  if (el.viewerKeywordInput) {
    el.viewerKeywordInput.value = state.viewerKeywordQuery || "";
  }

  if (el.viewerKeywordClearBtn) {
    el.viewerKeywordClearBtn.disabled = !normalizeSpace(state.viewerKeywordQuery);
  }

  if (el.viewerKeywordError) {
    const errorText = normalizeSpace(state.viewerKeywordError || "");
    el.viewerKeywordError.hidden = !errorText;
    el.viewerKeywordError.textContent = errorText;
  }

  if (!el.viewerKeywordSummary) return;

  if (state.viewerKeywordError) {
    el.viewerKeywordSummary.textContent = `Filtr nieaktywny przez błąd składni. Wyświetlono ${fmtNumber(totalParagraphs)} akapitów.`;
    return;
  }

  if (normalizeSpace(state.viewerKeywordQuery)) {
    el.viewerKeywordSummary.textContent = `Filtr aktywny: ${fmtNumber(visibleParagraphs)} z ${fmtNumber(totalParagraphs)} akapitów.`;
    return;
  }

  el.viewerKeywordSummary.textContent = `Wyświetlono wszystkie ${fmtNumber(totalParagraphs)} ${plural(totalParagraphs, "akapit", "akapity", "akapitów")}.`;
}

function applyViewerKeywordFilter(rawQuery, options = {}) {
  const rerender = options.rerender !== false;
  const parsedResult = parseViewerKeywordQuery(rawQuery);
  state.viewerKeywordQuery = normalizeSpace(rawQuery);
  state.viewerKeywordError = parsedResult.error || "";
  state.viewerKeywordParsed = parsedResult.parsed || {
    source: "",
    hasQuery: false,
    rpn: [],
    textOperands: [],
    allTerms: [],
    highlightTerms: []
  };

  if (!rerender || !state.activeViewerCaseKey) return parsedResult.ok;
  const activeCase = findCaseByKey(state.activeViewerCaseKey);
  if (activeCase) {
    renderCaseViewer(activeCase);
  }
  return parsedResult.ok;
}

function renderCaseViewer(caseItem, options = {}) {
  if (!caseItem) return;

  const focusParagraphId = options.focusParagraphId || null;
  const allSections = buildViewerSections(caseItem);
  const totalParagraphs = allSections.reduce((sum, section) => sum + (section.paragraphs?.length || 0), 0);
  const viewerFilterQuery = normalizeSpace(state.viewerKeywordQuery);
  const viewerFilterParsed = state.viewerKeywordError ? null : state.viewerKeywordParsed;

  const sections = allSections
    .map((section) => {
      const paragraphs = (section.paragraphs || []).filter((paragraph) => {
        if (focusParagraphId && paragraph.paragraph_id === focusParagraphId) return true;
        if (!viewerFilterQuery || !viewerFilterParsed?.hasQuery) return true;
        return paragraphMatchesViewerFilter(caseItem, paragraph, viewerFilterParsed);
      });

      return {
        ...section,
        paragraphs
      };
    })
    .filter((section) => section.paragraphs.length > 0);

  const visibleParagraphs = sections.reduce((sum, section) => sum + section.paragraphs.length, 0);
  const toc = buildViewerToc(caseItem, sections);
  const identity = buildJudgmentIdentitySection(caseItem);
  const similarEntries = getSimilarCasesForCase(caseItem);
  const similarTargetId = `viewer-similar-${toDomId(caseKey(caseItem) || caseItem.case_signature || "wyrok")}`;
  const similarHtml = renderSimilarCasesList(caseItem, similarEntries, { sectionId: similarTargetId });
  const similarToc = similarHtml
    ? [{
      label: "Podobne orzeczenia",
      targetId: similarTargetId,
      count: 0,
      range: "",
      meta: `${fmtNumber(similarEntries.length)} pozycji`
    }]
    : [];
  const tocWithIdentity = [identity.tocItem, ...toc, ...similarToc];
  const focusTargetId = focusParagraphId ? `viewer-paragraph-${toDomId(focusParagraphId)}` : null;
  const highlightQuery = mergeHighlightQueries(state.currentParsedQuery, viewerFilterParsed);
  const viewerSnippetTerms = (highlightQuery?.textOperands || [])
    .map((operand) => operand.legal || operand.norm)
    .filter(Boolean);

  if (focusTargetId && tocWithIdentity.some((item) => item.targetId === focusTargetId)) {
    state.activeViewerTocTarget = focusTargetId;
  }

  state.activeViewerToc = tocWithIdentity;
  if (!tocWithIdentity.some((item) => item.targetId === state.activeViewerTocTarget)) {
    state.activeViewerTocTarget = tocWithIdentity[0]?.targetId || null;
  }

  el.judgmentViewer.hidden = false;
  if (el.judgmentViewerBackdrop) {
    el.judgmentViewerBackdrop.hidden = false;
  }
  document.body.classList.add("viewer-open");

  el.judgmentViewerTitle.textContent = `${caseItem.case_signature} — pełna treść`;
  el.judgmentViewerMeta.textContent = `${caseItem.decision_type} • ${formatDate(caseItem.decision_date_iso || caseItem.decision_date_raw)} • ${fmtNumber(visibleParagraphs)} / ${fmtNumber(totalParagraphs)} akapitów`;

  if (caseItem.source_url) {
    el.judgmentViewerSourceLink.hidden = false;
    el.judgmentViewerSourceLink.href = caseItem.source_url;
    el.judgmentViewerSourceLink.dataset.sourceUrl = caseItem.source_url;
  } else {
    el.judgmentViewerSourceLink.hidden = true;
    el.judgmentViewerSourceLink.removeAttribute("href");
    delete el.judgmentViewerSourceLink.dataset.sourceUrl;
  }

  renderViewerKeywordUi(totalParagraphs, visibleParagraphs);
  renderViewerToc(tocWithIdentity);

  if (!sections.length) {
    el.judgmentViewerContent.innerHTML = `
      ${identity.html}
      <article class="viewer-empty">
        Brak akapitów spełniających filtr. Zmień słowa kluczowe albo kliknij „Wyczyść”.
      </article>
      ${similarHtml}
    `;
    state.activeViewerCaseKey = caseKey(caseItem);
    el.judgmentViewerContent.scrollTop = 0;
    return;
  }

  el.judgmentViewerContent.innerHTML = identity.html + sections
    .map((section) => {
      const sectionBody = section.paragraphs
        .map((paragraph) => {
          const paragraphLabel = paragraph.paragraph_number || `§ ${paragraph.paragraph_index}`;
          const paragraphDomId = viewerParagraphDomId(paragraph, section.id);
          const viewerHit = viewerParagraphToHit(paragraph);
          const pinned = isParagraphPinned(caseItem, viewerHit);
          const caseKeyValue = caseKey(caseItem);
          const citation = `${caseItem.case_signature}, ${section.label}, ${paragraphLabel}`;
          const textFull = paragraph.text || "";
          const textSnippet = buildSnippet(textFull, viewerSnippetTerms, 560);
          const hasLongText = normalizeSpace(textFull).length > normalizeSpace(textSnippet).length + 8;
          const isExpanded = isViewerParagraphExpanded(caseItem, paragraph.paragraph_id);
          return `
            <article class="viewer-paragraph" id="${escapeHtml(paragraphDomId)}">
              <div class="viewer-paragraph-head">
                <span class="viewer-paragraph-num">${escapeHtml(paragraphLabel)}</span>
                <span class="viewer-paragraph-citation">${escapeHtml(citation)}</span>
                <div class="viewer-paragraph-actions">
                  <button type="button" class="mini-btn" data-action="viewer-copy-citation" data-case-key="${escapeHtml(caseKeyValue)}" data-paragraph-id="${escapeHtml(paragraph.paragraph_id)}">Cytuj</button>
                  <button type="button" class="mini-btn" data-action="viewer-copy-paragraph" data-case-key="${escapeHtml(caseKeyValue)}" data-paragraph-id="${escapeHtml(paragraph.paragraph_id)}">Kopiuj akapit</button>
                  <button type="button" class="mini-btn" data-action="viewer-pin-paragraph" data-case-key="${escapeHtml(caseKeyValue)}" data-paragraph-id="${escapeHtml(paragraph.paragraph_id)}">${pinned ? "Odepnij akapit" : "Przypnij akapit"}</button>
                  ${hasLongText ? `<button type="button" class="mini-btn" data-action="viewer-toggle-paragraph" data-case-key="${escapeHtml(caseKeyValue)}" data-paragraph-id="${escapeHtml(paragraph.paragraph_id)}">${isExpanded ? "Zwiń akapit" : "Rozwiń akapit"}</button>` : ""}
                </div>
              </div>
              <p class="viewer-paragraph-text viewer-paragraph-text-preview" ${(isExpanded && hasLongText) ? "hidden" : ""}>${highlight(textSnippet, highlightQuery)}</p>
              ${hasLongText ? `<p class="viewer-paragraph-text viewer-paragraph-text-full" ${isExpanded ? "" : "hidden"}>${highlight(textFull, highlightQuery)}</p>` : ""}
            </article>
          `;
        })
        .join("");

      return `
        <article class="viewer-section" id="${escapeHtml(section.id)}">
          <header class="viewer-section-head">
            <h4>${escapeHtml(section.label)}</h4>
            <span class="viewer-section-count">${fmtNumber(section.paragraphs.length)} akapitów</span>
          </header>
          <div class="viewer-section-body">
            ${sectionBody}
          </div>
        </article>
      `;
    })
    .join("") + similarHtml;

  state.activeViewerCaseKey = caseKey(caseItem);

  if (focusParagraphId) {
    const targetParagraph = document.getElementById(`viewer-paragraph-${toDomId(focusParagraphId)}`);
    if (targetParagraph) {
      targetParagraph.scrollIntoView({ block: "center", behavior: "smooth" });
    }
  } else {
    el.judgmentViewerContent.scrollTop = 0;
  }
}

function closeCaseViewer() {
  el.judgmentViewer.hidden = true;
  if (el.judgmentViewerBackdrop) {
    el.judgmentViewerBackdrop.hidden = true;
  }
  document.body.classList.remove("viewer-open");
  state.activeViewerCaseKey = null;
  state.activeViewerTocTarget = null;
  state.activeViewerToc = [];
  state.viewerParagraphOverrides.clear();
}

function handleViewerTocAction(event) {
  const button = event.target.closest("button[data-action='viewer-goto-section']");
  if (!button) return;

  const targetId = button.dataset.targetId;
  if (!targetId) return;

  const target = document.getElementById(targetId);
  if (!target) return;
  state.activeViewerTocTarget = targetId;
  renderViewerToc(state.activeViewerToc || []);
  target.scrollIntoView({ block: "start", behavior: "smooth" });
}

function findViewerParagraph(caseKeyValue, paragraphId) {
  const caseItem = findCaseByKey(caseKeyValue);
  if (!caseItem) return null;
  const paragraph = (caseItem.paragraphs || []).find((item) => item.paragraph_id === paragraphId);
  if (!paragraph) return null;
  return {
    caseItem,
    paragraph,
    hit: viewerParagraphToHit(paragraph)
  };
}

async function handleViewerContentAction(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) return;

  const action = button.dataset.action;
  if (!action.startsWith("viewer-")) return;

  if (action === "viewer-open-similar-case") {
    const targetDocumentId = normalizeSpace(button.dataset.targetDocumentId);
    const sourceUrl = normalizeSpace(button.dataset.sourceUrl);
    const targetCase = findCaseByDocumentId(targetDocumentId);
    if (targetCase) {
      renderCaseViewer(targetCase);
      return;
    }
    if (sourceUrl) {
      openSourceLink(sourceUrl);
    }
    return;
  }

  if (action === "viewer-toggle-similar") {
    const host = button.closest(".viewer-similar-block");
    const list = host ? host.querySelector(".similar-cases-list") : null;
    const isExpanded = button.getAttribute("aria-expanded") === "true";
    const nextExpanded = !isExpanded;

    button.setAttribute("aria-expanded", nextExpanded ? "true" : "false");
    button.textContent = nextExpanded ? "Zwiń" : "Rozwiń";
    if (list) list.hidden = !nextExpanded;
    return;
  }

  const caseKeyValue = button.dataset.caseKey;
  const paragraphId = button.dataset.paragraphId;
  if (!caseKeyValue || !paragraphId) return;

  const payload = findViewerParagraph(caseKeyValue, paragraphId);
  if (!payload) return;

  if (action === "viewer-copy-paragraph") {
    await copyToClipboard(payload.paragraph.text, "Skopiowano tekst akapitu.");
    return;
  }

  if (action === "viewer-copy-citation") {
    await copyToClipboard(buildCaseCitation(payload.caseItem), "Skopiowano cytowanie.");
    return;
  }

  if (action === "viewer-pin-paragraph") {
    togglePinParagraph(payload.caseItem, payload.hit);
    renderCaseViewer(payload.caseItem, { focusParagraphId: payload.paragraph.paragraph_id });
    return;
  }

  if (action === "viewer-toggle-paragraph") {
    toggleViewerParagraphExpanded(payload.caseItem, payload.paragraph.paragraph_id);
    renderCaseViewer(payload.caseItem, { focusParagraphId: payload.paragraph.paragraph_id });
  }
}

function renderCompareView() {
  if (el.compareView.hidden) return;

  const selectedCases = Object.values(state.caseFolder.cases)
    .slice(0, 3)
    .map((entry) => findCaseByKey(entry.case_key))
    .filter(Boolean);

  if (!selectedCases.length) {
    el.compareSlots.innerHTML = "<p class='case-topic'>Brak przypiętych spraw do porównania.</p>";
    return;
  }

  el.compareSlots.innerHTML = selectedCases
    .map((caseItem) => {
      const sectionRows = Object.entries(caseItem.paragraphs.reduce((acc, paragraph) => {
        acc[paragraph.section_key] = (acc[paragraph.section_key] || 0) + 1;
        return acc;
      }, {}))
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([key, count]) => `${SECTION_META[key]?.label || key}: ${count}`)
        .join(" • ");

      return `
        <article class="compare-slot">
          <h3>${escapeHtml(caseItem.case_signature)}</h3>
          <p><strong>Data:</strong> ${escapeHtml(formatDate(caseItem.decision_date_iso || caseItem.decision_date_raw))}</p>
          <p><strong>Typ:</strong> ${escapeHtml(caseItem.decision_type)}</p>
          <p><strong>Akapity:</strong> ${fmtNumber(caseItem.paragraph_count)}</p>
          <p><strong>Sekcje TOP:</strong> ${escapeHtml(sectionRows || "-")}</p>
        </article>
      `;
    })
    .join("");
}

function toCsvRow(values) {
  return values
    .map((value) => {
      const stringValue = String(value ?? "");
      if (/[",\n]/.test(stringValue)) {
        return `"${stringValue.replace(/"/g, '""')}"`;
      }
      return stringValue;
    })
    .join(",");
}

function downloadBlob(filename, content, type = "text/plain;charset=utf-8") {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function buildExportMetadata() {
  return {
    dataset_hash: state.datasetMeta.hash || "",
    dataset_generated_at: state.datasetMeta.generatedAt || "",
    query_string: state.query || "",
    active_filters: stableStringify(state.currentFilters || {}),
    export_generated_at: new Date().toISOString(),
    tool_version: TOOL_VERSION
  };
}

function updateActionButtons() {
  const hasResults = state.currentResults.length > 0;
  const selectedCount = state.selectedHits.size;
  if (el.exportBtn) el.exportBtn.disabled = !state.loaded || !hasResults;
  if (el.quoteExportBtn) el.quoteExportBtn.disabled = !state.loaded || selectedCount === 0;
  if (el.dossierExportBtn) {
    const hasFolderData = Object.keys(state.caseFolder.cases).length > 0 || Object.keys(state.caseFolder.paragraphs).length > 0;
    el.dossierExportBtn.disabled = !state.loaded || !hasFolderData;
  }
  if (el.matrixExportBtn) {
    el.matrixExportBtn.disabled = !state.loaded || Object.keys(state.caseFolder.cases).length === 0;
  }
  if (el.selectedCitationsClearBtn) {
    el.selectedCitationsClearBtn.disabled = !state.loaded || selectedCount === 0;
  }
}

function exportCsv() {
  if (!state.currentResults.length) return;

  const rows = [];
  const meta = buildExportMetadata();

  rows.push(toCsvRow([
    "dataset_hash",
    "dataset_generated_at",
    "query_string",
    "active_filters",
    "export_generated_at",
    "tool_version",
    "case_signature",
    "document_id",
    "paragraph_index",
    "paragraph_number",
    "section_key",
    "section_label",
    "decision_date",
    "decision_type",
    "source_url",
    "score",
    "score_explain",
    "text"
  ]));

  if (state.currentMode === "search") {
    for (const group of state.currentResults) {
      for (const hit of group.hits) {
        rows.push(toCsvRow([
          meta.dataset_hash,
          meta.dataset_generated_at,
          meta.query_string,
          meta.active_filters,
          meta.export_generated_at,
          meta.tool_version,
          group.caseItem.case_signature,
          group.caseItem.document_id,
          hit.paragraph_index,
          hit.paragraph_number || "",
          hit.section_key,
          hit.section_label,
          group.caseItem.decision_date_iso || group.caseItem.decision_date_raw || "",
          group.caseItem.decision_type,
          group.caseItem.source_url || "",
          hit.score,
          hit.score_explain,
          hit.text
        ]));
      }
    }
  } else {
    for (const group of state.currentResults) {
      rows.push(toCsvRow([
        meta.dataset_hash,
        meta.dataset_generated_at,
        meta.query_string,
        meta.active_filters,
        meta.export_generated_at,
        meta.tool_version,
        group.caseItem.case_signature,
        group.caseItem.document_id,
        "",
        "",
        "",
        "",
        group.caseItem.decision_date_iso || group.caseItem.decision_date_raw || "",
        group.caseItem.decision_type,
        group.caseItem.source_url || "",
        "",
        "",
        group.caseItem.topic || ""
      ]));
    }
  }

  downloadBlob(
    `tk-results-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.csv`,
    `${rows.join("\n")}\n`,
    "text/csv;charset=utf-8"
  );
}

function collectSelectedHitEntries() {
  const selected = [];
  for (const group of state.currentResults) {
    for (const hit of group.hits || []) {
      const hitId = makeHitId(group.caseItem, hit);
      if (!state.selectedHits.has(hitId)) continue;
      selected.push({
        caseIndex: group.caseIndex,
        caseItem: group.caseItem,
        hit,
        hitId
      });
    }
  }
  return selected;
}

function renderSelectedCitationsPanel() {
  if (!el.selectedCitationsPanel || !el.selectedCitationsList || !el.selectedCitationsCount) return;
  const isStudentMode = getPageDashboardMode() === "student";
  if (!isStudentMode) {
    el.selectedCitationsPanel.hidden = true;
    return;
  }

  const selected = collectSelectedHitEntries()
    .sort((a, b) =>
      String(a.caseItem.case_signature || "").localeCompare(String(b.caseItem.case_signature || ""), "pl")
      || (a.hit.paragraph_index || 0) - (b.hit.paragraph_index || 0)
    );

  if (!selected.length) {
    el.selectedCitationsPanel.hidden = true;
    el.selectedCitationsCount.textContent = "0";
    el.selectedCitationsList.innerHTML = "";
    return;
  }

  el.selectedCitationsPanel.hidden = false;
  el.selectedCitationsCount.textContent = `${fmtNumber(selected.length)} ${plural(selected.length, "cytat", "cytaty", "cytatów")}`;
  el.selectedCitationsList.innerHTML = selected
    .slice(0, 16)
    .map(({ caseIndex, caseItem, hit, hitId }) => {
      const numberLabel = hit.paragraph_number || `§ ${hit.paragraph_index}`;
      return `
        <article class="selected-citation-item">
          <p class="selected-citation-meta">${escapeHtml(caseItem.case_signature)} • ${escapeHtml(hit.section_label)} • ${escapeHtml(numberLabel)}</p>
          <p class="selected-citation-text">${highlight(buildSnippet(hit.text || "", [], 220), state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] })}</p>
          <div class="selected-citation-actions">
            <button type="button" class="mini-btn" data-action="open-case-view" data-case-index="${caseIndex}" data-hit-id="${escapeHtml(hitId)}">Podgląd</button>
            <button type="button" class="mini-btn" data-action="toggle-hit-select" data-case-index="${caseIndex}" data-hit-id="${escapeHtml(hitId)}">Usuń</button>
          </div>
        </article>
      `;
    })
    .join("");
}

function findParagraphContext(caseItem, hit) {
  const paragraphs = caseItem.paragraphs || [];
  const index = paragraphs.findIndex((paragraph) => paragraph.paragraph_id === hit.paragraph_id);
  if (index < 0) {
    return {
      context_window: "±1 paragraph",
      prev_id: "",
      prev: "",
      current_id: hit.paragraph_id || "",
      current: hit.text,
      next_id: "",
      next: ""
    };
  }

  return {
    context_window: "±1 paragraph",
    prev_id: paragraphs[index - 1]?.paragraph_id || "",
    prev: paragraphs[index - 1]?.text || "",
    current_id: paragraphs[index]?.paragraph_id || hit.paragraph_id || "",
    current: paragraphs[index]?.text || hit.text,
    next_id: paragraphs[index + 1]?.paragraph_id || "",
    next: paragraphs[index + 1]?.text || ""
  };
}

function exportQuotePackage() {
  const selected = collectSelectedHitEntries();
  if (!selected.length) {
    setDatasetStatus("Pakiet cytatów wymaga wybrania co najmniej jednego akapitu.", "warn");
    return;
  }

  const meta = buildExportMetadata();
  const rows = [];
  rows.push(toCsvRow([
    "dataset_hash",
    "dataset_generated_at",
    "query_string",
    "active_filters",
    "export_generated_at",
    "tool_version",
    "case_signature",
    "document_id",
    "paragraph_index",
    "paragraph_number",
    "section_key",
    "source_url",
    "citation",
    "context_window",
    "previous_paragraph_id",
    "previous_paragraph",
    "quoted_paragraph_id",
    "quoted_paragraph",
    "next_paragraph_id",
    "next_paragraph"
  ]));

  for (const entry of selected) {
    const context = findParagraphContext(entry.caseItem, entry.hit);
    rows.push(toCsvRow([
      meta.dataset_hash,
      meta.dataset_generated_at,
      meta.query_string,
      meta.active_filters,
      meta.export_generated_at,
      meta.tool_version,
      entry.caseItem.case_signature,
      entry.caseItem.document_id,
      entry.hit.paragraph_index,
      entry.hit.paragraph_number || "",
      entry.hit.section_key,
      entry.caseItem.source_url || "",
      buildParagraphCitation(entry.caseItem, entry.hit),
      context.context_window,
      context.prev_id,
      context.prev,
      context.current_id,
      context.current,
      context.next_id,
      context.next
    ]));
  }

  downloadBlob(
    `tk-quote-package-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.csv`,
    `${rows.join("\n")}\n`,
    "text/csv;charset=utf-8"
  );
}

function exportDossier() {
  const payload = {
    generated_at: new Date().toISOString(),
    tool_version: TOOL_VERSION,
    dataset_hash: state.datasetMeta.hash,
    dataset_generated_at: state.datasetMeta.generatedAt,
    query: state.query,
    filters: state.currentFilters,
    notes: state.caseFolder.notes,
    pinned_cases: Object.values(state.caseFolder.cases),
    pinned_paragraphs: Object.values(state.caseFolder.paragraphs)
  };

  downloadBlob(
    `tk-dossier-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.json`,
    `${JSON.stringify(payload, null, 2)}\n`,
    "application/json;charset=utf-8"
  );
}

function deriveIssuesForMatrix() {
  const parsed = state.currentParsedQuery;
  if (parsed?.textOperands?.length) {
    return [...new Set(parsed.textOperands.map((operand) => operand.raw))].slice(0, 16);
  }

  const freq = new Map();
  const textPool = Object.values(state.caseFolder.paragraphs).map((entry) => entry.text || "");
  for (const text of textPool) {
    const words = normalizeSearchText(text)
      .split(/[^\p{L}\p{N}_-]+/u)
      .filter((word) => word.length >= 4 && !STOPWORDS.has(word));
    for (const word of words) {
      freq.set(word, (freq.get(word) || 0) + 1);
    }
  }

  return [...freq.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .map(([term]) => term);
}

function exportArgumentMatrix() {
  const caseEntries = Object.values(state.caseFolder.cases)
    .map((entry) => findCaseByKey(entry.case_key))
    .filter(Boolean);

  if (!caseEntries.length) {
    setDatasetStatus("Macierz argumentów wymaga przypięcia co najmniej jednej sprawy.", "warn");
    return;
  }

  const issues = deriveIssuesForMatrix();
  if (!issues.length) {
    setDatasetStatus("Brak wystarczających danych do zbudowania macierzy argumentów.", "warn");
    return;
  }

  const header = ["issue", ...caseEntries.map((caseItem) => caseItem.case_signature)];
  const rows = [toCsvRow(header)];

  for (const issue of issues) {
    const issueNorm = normalizeLegalCitationText(issue);
    const row = [issue];

    for (const caseItem of caseEntries) {
      const refs = caseItem.paragraphs
        .filter((paragraph) => normalizeLegalCitationText(paragraph.text).includes(issueNorm))
        .slice(0, 4)
        .map((paragraph) => paragraph.paragraph_number || `§ ${paragraph.paragraph_index}`);

      row.push(refs.join(" | "));
    }

    rows.push(toCsvRow(row));
  }

  downloadBlob(
    `tk-argument-matrix-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.csv`,
    `${rows.join("\n")}\n`,
    "text/csv;charset=utf-8"
  );
}

async function copyToClipboard(text, successMessage) {
  try {
    await navigator.clipboard.writeText(String(text || ""));
    setDatasetStatus(successMessage, "success");
  } catch {
    setDatasetStatus("Nie udało się skopiować do schowka.", "error");
  }
}

function readUrlStateFromLocation() {
  const params = new URLSearchParams(window.location.search || "");
  return {
    q: params.get("q") || "",
    sections: (params.get("sections") || "").split(",").map((entry) => entry.trim()).filter(Boolean),
    types: [...new Set(
      (params.get("types") || "")
        .split(",")
        .map((entry) => resolveDecisionTypeFilterValue(entry))
        .filter(Boolean)
    )],
    benches: [...new Set(
      (params.get("benches") || "")
        .split(",")
        .map((entry) => resolveBenchFilterValue(entry))
        .filter(Boolean)
    )],
    year_from: params.get("year_from") || "",
    year_to: params.get("year_to") || "",
    judge: params.get("judge") || "",
    signature: params.get("signature") || ""
  };
}

function applyUrlState(urlState) {
  if (!urlState) return;

  state.applyingUrlState = true;
  el.searchInput.value = urlState.q || "";
  el.yearFrom.value = urlState.year_from || "";
  el.yearTo.value = urlState.year_to || "";
  el.judgeFilter.value = urlState.judge || "";
  el.signatureFilter.value = urlState.signature || "";

  const sectionSet = new Set(urlState.sections || []);
  const typeSet = new Set(urlState.types || []);
  const benchSet = new Set(urlState.benches || []);

  document
    .querySelectorAll('input[data-filter="section"]')
    .forEach((input) => {
      input.checked = sectionSet.has(input.value);
    });

  document
    .querySelectorAll('input[data-filter="type"]')
    .forEach((input) => {
      input.checked = typeSet.has(input.value);
    });

  document
    .querySelectorAll('input[data-filter="bench"]')
    .forEach((input) => {
      input.checked = benchSet.has(input.value);
    });

  state.applyingUrlState = false;
}

function updateUrlState(filters) {
  if (state.applyingUrlState) return;

  const params = new URLSearchParams();
  if (state.query) params.set("q", state.query);
  if (filters.sections.length) params.set("sections", filters.sections.join(","));
  if (filters.types.length) params.set("types", filters.types.join(","));
  if (filters.benches.length) params.set("benches", filters.benches.join(","));
  if (filters.yearFrom) params.set("year_from", String(filters.yearFrom));
  if (filters.yearTo) params.set("year_to", String(filters.yearTo));
  if (filters.judge) params.set("judge", filters.judge);
  if (filters.signature) params.set("signature", filters.signature);

  const query = params.toString();
  const nextUrl = query ? `${window.location.pathname}?${query}` : window.location.pathname;
  window.history.replaceState(null, "", nextUrl);
}

function clearAllFiltersAndQuery() {
  el.searchInput.value = "";
  el.yearFrom.value = "";
  el.yearTo.value = "";
  el.judgeFilter.value = "";
  el.signatureFilter.value = "";

  document
    .querySelectorAll('input[data-filter="section"], input[data-filter="type"], input[data-filter="bench"]')
    .forEach((input) => {
      input.checked = false;
    });

  state.uiPrefs.activePreset = null;
  saveUiPrefs();
  renderPresetSelection();
  runSearch({ forceBrowse: true });
}

function renderPresetSelection() {
  document.querySelectorAll(".preset-chip").forEach((button) => {
    button.classList.toggle("active", button.dataset.preset === state.uiPrefs.activePreset);
  });
}

function setCheckedValues(selector, values) {
  const valueSet = new Set(values);
  document.querySelectorAll(selector).forEach((input) => {
    input.checked = valueSet.has(input.value);
  });
}

function applyQuickPreset(name) {
  const preset = QUICK_PRESETS[name];
  if (!preset) return;

  if (preset.sectionKeys.length) {
    setCheckedValues('input[data-filter="section"]', preset.sectionKeys);
  } else {
    setCheckedValues('input[data-filter="section"]', []);
  }

  if ((preset.typeKeys || []).length) {
    setCheckedValues('input[data-filter="type"]', preset.typeKeys);
  } else {
    setCheckedValues('input[data-filter="type"]', []);
  }

  setCheckedValues('input[data-filter="bench"]', []);

  state.uiPrefs.activePreset = name;
  saveUiPrefs();
  renderPresetSelection();
}

function findHitById(caseIndex, hitId) {
  const group = state.currentResults.find((entry) => entry.caseIndex === caseIndex);
  if (!group) return null;
  const hit = group.hits.find((item) => makeHitId(group.caseItem, item) === hitId);
  if (!hit) return null;
  return {
    group,
    caseItem: group.caseItem,
    hit
  };
}

async function handleResultAction(event) {
  if (handleOpenSourceLinkClick(event)) return;

  const button = event.target.closest("button[data-action]");
  if (!button) return;

  const action = button.dataset.action;
  const caseIndex = Number(button.dataset.caseIndex);

  if (action === "toggle-case-expand") {
    const key = button.dataset.caseKey;
    if (state.expandedCases.has(key)) {
      state.expandedCases.delete(key);
    } else {
      state.expandedCases.add(key);
    }
    renderResults(state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
    return;
  }

  if (action === "open-case-view") {
    if (!Number.isFinite(caseIndex)) return;
    const caseItem = state.cases[caseIndex];
    if (!caseItem) return;

    const hitId = button.dataset.hitId;
    if (hitId) {
      const payload = findHitById(caseIndex, hitId);
      renderCaseViewer(caseItem, { focusParagraphId: payload?.hit?.paragraph_id || null });
    } else {
      renderCaseViewer(caseItem);
    }
    return;
  }

  if (action === "pin-case") {
    if (!Number.isFinite(caseIndex)) return;
    const caseItem = state.cases[caseIndex];
    if (!caseItem) return;
    togglePinCase(caseItem);
    return;
  }

  const hitId = button.dataset.hitId;
  if (!Number.isFinite(caseIndex) || !hitId) return;

  const payload = findHitById(caseIndex, hitId);
  if (!payload) return;

  if (action === "toggle-hit-select") {
    if (state.selectedHits.has(hitId)) {
      state.selectedHits.delete(hitId);
    } else {
      state.selectedHits.add(hitId);
    }
    renderResults(state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
    return;
  }

  if (action === "toggle-hit-text") {
    toggleHitTextExpanded(hitId);
    renderResults(state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
    return;
  }

  if (action === "copy-citation") {
    await copyToClipboard(buildCaseCitation(payload.caseItem), "Skopiowano cytowanie.");
    return;
  }

  if (action === "copy-paragraph") {
    const mergedHeading = normalizeSpace(button.dataset.mergedHeading || "");
    const paragraphText = mergedHeading ? `${mergedHeading}\n${payload.hit.text}` : payload.hit.text;
    await copyToClipboard(paragraphText, "Skopiowano tekst akapitu.");
    return;
  }

  if (action === "copy-url") {
    await copyToClipboard(payload.caseItem.source_url || "", "Skopiowano URL źródła.");
    return;
  }

  if (action === "pin-paragraph") {
    togglePinParagraph(payload.caseItem, payload.hit);
  }
}

function handleFolderPanelAction(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) return;

  if (button.dataset.action === "remove-folder-case") {
    const caseKeyValue = button.dataset.caseKey;
    if (caseKeyValue && state.caseFolder.cases[caseKeyValue]) {
      delete state.caseFolder.cases[caseKeyValue];
      saveCaseFolder();
      renderCaseFolder();
      renderResults(state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
    }
    return;
  }

  if (button.dataset.action === "remove-folder-paragraph") {
    const hitId = button.dataset.hitId;
    if (hitId && state.caseFolder.paragraphs[hitId]) {
      delete state.caseFolder.paragraphs[hitId];
      state.selectedHits.delete(hitId);
      saveCaseFolder();
      renderCaseFolder();
      renderResults(state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
    }
  }
}

function mapLoadError(error) {
  if (!error) return "Nie udało się wczytać danych.";
  if (error.code === "FILE_TOO_LARGE") {
    return `Plik jest zbyt duży (${fmtNumber(Math.round(error.fileSize / (1024 * 1024)))} MB). Limit: ${fmtNumber(Math.round(MAX_FILE_BYTES / (1024 * 1024)))} MB.`;
  }
  if (error.name === "AbortError") {
    return "Wczytywanie pliku zostało anulowane. Możesz wybrać plik ponownie.";
  }
  const msg = String(error.message || "");
  if (msg.includes("JSONL") || msg.includes("JSON")) {
    return "Nieprawidłowy format danych. Użyj poprawnego JSON/JSONL (tablica rekordów albo jeden rekord na linię).";
  }
  if (msg.includes("Niezamknięty cudzysłów") || msg.includes("Nieprawidłowa składnia")) {
    return "Nieprawidłowa składnia zapytania. Sprawdź operatory AND/OR/NOT, nawiasy i cudzysłowy.";
  }
  return "Nie udało się wczytać danych. Sprawdź format pliku i spróbuj ponownie.";
}

function readFileWithProgress(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    state.currentFileReader = reader;

    reader.onerror = () => reject(reader.error || new Error("Błąd odczytu pliku."));
    reader.onabort = () => reject(new DOMException("Aborted", "AbortError"));
    reader.onprogress = (event) => {
      if (!event.lengthComputable) return;
      const percent = Math.round((event.loaded / event.total) * 100);
      setDatasetStatus(`Wczytywanie pliku: ${percent}%`, "info");
    };
    reader.onload = () => resolve(String(reader.result || ""));

    reader.readAsText(file, "utf-8");
  });
}

async function hydrateDataset(rows, options = {}) {
  const { cases, validationErrors } = normalizeAndValidateRows(rows);
  if (!cases.length) {
    throw new Error("Nie znaleziono poprawnych orzeczeń w przesłanym pliku.");
  }

  cases.sort((a, b) => {
    const aTime = parseDateIso(a.decision_date_iso)?.getTime() || 0;
    const bTime = parseDateIso(b.decision_date_iso)?.getTime() || 0;
    if (bTime !== aTime) return bTime - aTime;
    return String(a.case_signature).localeCompare(String(b.case_signature), "pl");
  });

  state.cases = cases;
  state.caseByDocumentId = new Map(cases.map((item) => [normalizeSpace(item.document_id), item]));
  state.validationErrorsCount = validationErrors;
  state.selectedHits.clear();
  state.expandedCases.clear();
  state.expandedSimilarCases.clear();
  resetSimilarityState();
  state.viewerKeywordQuery = "";
  state.viewerKeywordError = "";
  state.viewerKeywordParsed = {
    source: "",
    hasQuery: false,
    rpn: [],
    textOperands: [],
    allTerms: [],
    highlightTerms: []
  };
  closeCaseViewer();
  state.loaded = false;

  setSearchEnabled(false);
  setDatasetStatus("Indeksowanie zbioru...", "info");
  const indexed = await indexDatasetCases(cases);

  state.paragraphIndex = indexed.paragraphIndex;
  state.sections = indexed.sections;
  const indexedTypeKeys = new Set(
    (indexed.decisionTypes || [])
      .map((entry) => (typeof entry === "string" ? normalizeSpace(entry) : normalizeSpace(entry?.key)))
      .filter(Boolean)
  );
  state.decisionTypes = IPO_DECISION_TYPES
    .filter((entry) => indexedTypeKeys.has(entry.key))
    .concat(IPO_DECISION_TYPES.filter((entry) => !indexedTypeKeys.has(entry.key)));
  state.benchSizes = [...IPO_BENCH_SIZES];
  state.years = indexed.years;
  state.loaded = true;

  const hashFromData = cases.find((entry) => entry.dataset_hash)?.dataset_hash || "";
  const generatedAtFromData = cases.find((entry) => entry.dataset_generated_at)?.dataset_generated_at || "";

  state.datasetMeta = {
    sourceName: options.sourceName || "dataset",
    hash: hashFromData || options.sourceHash || "",
    generatedAt: generatedAtFromData || options.generatedAt || new Date().toISOString(),
    caseCount: cases.length,
    normalizationVersion: cases[0]?.normalization_version || NORMALIZATION_VERSION
  };

  renderFilterOptions();
  updateDatasetStats();
  renderProvenanceBanner();
  setSearchEnabled(true);

  if (!state.urlStateApplied) {
    applyUrlState(state.pendingUrlState);
    state.urlStateApplied = true;
  }

  runSearch({ forceBrowse: true });
  void loadSimilarityForCurrentDataset();

  if (validationErrors > 0) {
    setDatasetStatus(`Załadowano ${fmtNumber(cases.length)} spraw. Walidacja wykryła ${fmtNumber(validationErrors)} ostrzeżeń.`, "warn");
  } else {
    setDatasetStatus(`Załadowano ${fmtNumber(cases.length)} spraw.`, "success");
  }
}

async function loadSample() {
  setDatasetStatus("Wczytywanie próbki...", "info");
  const response = await fetch(SAMPLE_DATA_URL, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Nie można pobrać próbki (${response.status}).`);
  }
  const text = await response.text();
  const parsed = parseDatasetText(text);
  const hash = await sha256Hex(text);
  await hydrateDataset(parsed, {
    sourceName: "tk_cases_sample200.jsonl",
    sourceHash: hash
  });
}

async function loadFullBenchDataset() {
  setDatasetStatus("Wczytywanie spraw w pełnym składzie...", "info");
  const response = await fetch(FULL_BENCH_DATA_URL, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Nie można pobrać zbioru pełnego składu (${response.status}).`);
  }
  const text = await response.text();
  const parsed = parseDatasetText(text);
  const hash = await sha256Hex(text);
  await hydrateDataset(parsed, {
    sourceName: "tk_cases_full_bench.jsonl",
    sourceHash: hash
  });
}

async function handleFile(file) {
  if (file.size > MAX_FILE_BYTES) {
    const error = new Error("FILE_TOO_LARGE");
    error.code = "FILE_TOO_LARGE";
    error.fileSize = file.size;
    throw error;
  }

  setLoadingControls(true);
  try {
    const text = await readFileWithProgress(file);
    const parsed = parseDatasetText(text);
    const hash = await sha256Hex(text);
    await hydrateDataset(parsed, {
      sourceName: file.name,
      sourceHash: hash
    });
  } finally {
    setLoadingControls(false);
    state.currentFileReader = null;
  }
}

function clearPresetOnManualFilterChange() {
  if (!state.uiPrefs.activePreset) return;
  state.uiPrefs.activePreset = null;
  saveUiPrefs();
  renderPresetSelection();
}

function setupKeyboardShortcuts() {
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !el.judgmentViewer.hidden) {
      closeCaseViewer();
      return;
    }

    if (isTypingTarget(event.target)) return;

    if (event.key === "/") {
      event.preventDefault();
      el.searchInput.focus();
      return;
    }

    if (event.key.toLowerCase() === "f") {
      event.preventDefault();
      const isHidden = el.filtersPanel.hidden;
      el.filtersPanel.hidden = !isHidden;
      el.filtersToggle.setAttribute("aria-expanded", String(isHidden));
      state.uiPrefs.filtersOpen = isHidden;
      saveUiPrefs();
      return;
    }

    if (event.key.toLowerCase() === "e") {
      event.preventDefault();
      if (!el.exportBtn.disabled) exportCsv();
    }
  });
}

function initInteractions() {
  if (el.loadSampleBtn) {
    el.loadSampleBtn.addEventListener("click", async () => {
      setLoadingControls(true);
      try {
        await loadSample();
      } catch (error) {
        setDatasetStatus(mapLoadError(error), "error");
      } finally {
        setLoadingControls(false);
      }
    });
  }

  if (el.loadFullBenchDatasetBtn) {
    el.loadFullBenchDatasetBtn.addEventListener("click", async () => {
      setLoadingControls(true);
      try {
        await loadFullBenchDataset();
      } catch (error) {
        setDatasetStatus(mapLoadError(error), "error");
      } finally {
        setLoadingControls(false);
      }
    });
  }

  if (el.cancelLoadBtn) {
    el.cancelLoadBtn.addEventListener("click", () => {
      if (state.currentFileReader) {
        state.currentFileReader.abort();
      }
    });
  }

  if (el.fileInput) {
    el.fileInput.addEventListener("change", async (event) => {
      const file = event.target.files?.[0];
      if (!file) return;
      try {
        await handleFile(file);
      } catch (error) {
        setDatasetStatus(mapLoadError(error), "error");
      }
    });
  }

  const onDrop = async (event) => {
    event.preventDefault();
    el.dropZone.classList.remove("drag-over");
    const file = event.dataTransfer?.files?.[0];
    if (!file) return;
    try {
      await handleFile(file);
    } catch (error) {
      setDatasetStatus(mapLoadError(error), "error");
    }
  };

  if (el.dropZone && el.fileInput) {
    el.dropZone.addEventListener("dragover", (event) => {
      event.preventDefault();
      el.dropZone.classList.add("drag-over");
    });
    el.dropZone.addEventListener("dragleave", () => el.dropZone.classList.remove("drag-over"));
    el.dropZone.addEventListener("drop", onDrop);
    el.dropZone.addEventListener("click", () => el.fileInput.click());
    el.dropZone.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        el.fileInput.click();
      }
    });
  }

  el.searchForm.addEventListener("submit", (event) => {
    event.preventDefault();
    runSearch();
  });

  const bindParagraphModeButtons = (collapsedBtn, expandedBtn) => {
    [collapsedBtn, expandedBtn].forEach((button) => {
      if (!button) return;
      button.addEventListener("click", () => {
        setParagraphDisplayMode(button.dataset.paragraphMode || "collapsed", { forceRerender: true });
      });
      button.addEventListener("keydown", (event) => {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
        event.preventDefault();
        const nextMode = event.key === "ArrowRight" ? "expanded" : "collapsed";
        setParagraphDisplayMode(nextMode, { forceRerender: true });
        const focusTarget = nextMode === "expanded" ? expandedBtn : collapsedBtn;
        focusTarget?.focus();
      });
    });
  };

  bindParagraphModeButtons(el.paragraphModeCollapsedBtn, el.paragraphModeExpandedBtn);
  bindParagraphModeButtons(el.viewerParagraphModeCollapsedBtn, el.viewerParagraphModeExpandedBtn);

  [el.modeSwitchStudent, el.modeSwitchExpert].forEach((link) => {
    if (!link) return;
    link.addEventListener("click", () => {
      const targetMode = link.id === "modeSwitchStudent" ? "student" : "expert";
      state.uiPrefs.viewMode = targetMode;
      saveUiPrefs();
    });
  });

  el.filtersToggle.addEventListener("click", () => {
    const isHidden = el.filtersPanel.hidden;
    el.filtersPanel.hidden = !isHidden;
    el.filtersToggle.setAttribute("aria-expanded", String(isHidden));
    state.uiPrefs.filtersOpen = isHidden;
    saveUiPrefs();
  });

  [el.yearFrom, el.yearTo, el.judgeFilter, el.signatureFilter].forEach((input) => {
    input.addEventListener("change", () => {
      clearPresetOnManualFilterChange();
    });
  });

  document.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.dataset.filter === "section" || target.dataset.filter === "type" || target.dataset.filter === "bench") {
      clearPresetOnManualFilterChange();
    }
  });

  el.quickPresets.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-preset]");
    if (!button) return;
    applyQuickPreset(button.dataset.preset);
  });

  el.exportBtn.addEventListener("click", exportCsv);
  if (el.quoteExportBtn) {
    el.quoteExportBtn.addEventListener("click", exportQuotePackage);
  }
  if (el.dossierExportBtn) {
    el.dossierExportBtn.addEventListener("click", exportDossier);
  }
  if (el.matrixExportBtn) {
    el.matrixExportBtn.addEventListener("click", exportArgumentMatrix);
  }
  if (el.selectedCitationsClearBtn) {
    el.selectedCitationsClearBtn.addEventListener("click", () => {
      state.selectedHits.clear();
      renderResults(state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
    });
  }

  el.clearBtn.addEventListener("click", clearAllFiltersAndQuery);

  el.pagination.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-page]");
    if (!button) return;
    state.currentPage = Number(button.dataset.page);
    renderResults(state.currentParsedQuery || { hasQuery: false, textOperands: [], allTerms: [], highlightTerms: [] });
  });

  el.resultsList.addEventListener("click", handleResultAction);
  el.sidebar.addEventListener("click", handleFolderPanelAction);
  el.judgmentViewerSourceLink.addEventListener("click", handleOpenSourceLinkClick);

  el.folderNotes.addEventListener("input", () => {
    state.caseFolder.notes = el.folderNotes.value;
    saveCaseFolder();
  });

  el.compareBtn.addEventListener("click", () => {
    el.compareView.hidden = !el.compareView.hidden;
    renderCompareView();
  });

  el.judgmentViewerCloseBtn.addEventListener("click", closeCaseViewer);
  el.judgmentViewerContent.addEventListener("click", handleViewerContentAction);
  el.judgmentViewerToc.addEventListener("click", handleViewerTocAction);
  if (el.viewerKeywordForm) {
    el.viewerKeywordForm.addEventListener("submit", (event) => {
      event.preventDefault();
      applyViewerKeywordFilter(el.viewerKeywordInput?.value || "");
    });
  }
  if (el.viewerKeywordClearBtn) {
    el.viewerKeywordClearBtn.addEventListener("click", () => {
      applyViewerKeywordFilter("");
      if (el.viewerKeywordInput) {
        el.viewerKeywordInput.focus();
      }
    });
  }
  if (el.judgmentViewerBackdrop) {
    el.judgmentViewerBackdrop.addEventListener("click", closeCaseViewer);
  }

  setupKeyboardShortcuts();
}

function loadPersistedState() {
  state.savedQueries = safeJsonParse(localStorage.getItem(STORAGE_KEYS.savedQueries) || "[]", []);

  const folder = safeJsonParse(localStorage.getItem(STORAGE_KEYS.caseFolder) || "{}", {});
  state.caseFolder = {
    cases: folder.cases || {},
    paragraphs: folder.paragraphs || {},
    notes: folder.notes || ""
  };

  const prefs = safeJsonParse(localStorage.getItem(STORAGE_KEYS.uiPrefs) || "{}", {});
  const pageMode = getPageDashboardMode();
  const hasFiltersPref = Object.prototype.hasOwnProperty.call(prefs, "filtersOpen");
  const defaultFiltersOpen = pageMode === "expert";
  const paragraphDisplayMode = prefs.paragraphDisplayMode === "expanded" ? "expanded" : "collapsed";
  state.uiPrefs = {
    filtersOpen: hasFiltersPref ? Boolean(prefs.filtersOpen) : defaultFiltersOpen,
    activePreset: prefs.activePreset || null,
    viewMode: pageMode,
    paragraphDisplayMode
  };
  saveUiPrefs();
}

function initUiPrefs() {
  el.filtersPanel.hidden = !state.uiPrefs.filtersOpen;
  el.filtersToggle.setAttribute("aria-expanded", String(state.uiPrefs.filtersOpen));
  renderPresetSelection();
  renderParagraphDisplayControl();
  el.folderNotes.value = state.caseFolder.notes || "";
  syncModeToggleUi();
}

function init() {
  cacheElements();
  loadPersistedState();
  state.pendingUrlState = readUrlStateFromLocation();

  setSearchEnabled(false);
  initUiPrefs();
  initInteractions();
  renderCaseFolder();

  // Public default for GitHub Pages: load full-bench dataset on first render.
  setLoadingControls(true);
  loadFullBenchDataset()
    .catch((error) => {
      setDatasetStatus(mapLoadError(error), "error");
    })
    .finally(() => {
      setLoadingControls(false);
    });
}

init();
