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

const IPO_DECISION_TYPES = [
  { key: "postanowienie", label: "Postanowienie" },
  { key: "postanowienie_tymczasowe", label: "Postanowienie Tymczasowe" },
  { key: "rozstrzygniecie", label: "Rozstrzygnięcie" },
  { key: "wyrok", label: "Wyrok" }
];

const IPO_BENCH_SIZES = [
  { key: "pelny_sklad", label: "Pełny skład" },
  { key: "piecioosobowa", label: "Pięcioosobowa" },
  { key: "trojosobowa", label: "Trójosobowa" }
];

function deriveIpoDecisionTypeInfo(decisionType) {
  const norm = normalizeSearchText(decisionType);
  if (!norm) return { key: null, label: null, visible: false };

  if (norm.includes("postanow") && norm.includes("tymczas")) {
    return { key: "postanowienie_tymczasowe", label: "Postanowienie Tymczasowe", visible: true };
  }
  if (norm.includes("rozstrzygn")) {
    return { key: "rozstrzygniecie", label: "Rozstrzygnięcie", visible: true };
  }
  if (norm.includes("wyrok")) {
    return { key: "wyrok", label: "Wyrok", visible: true };
  }
  if (norm.includes("postanow")) {
    return { key: "postanowienie", label: "Postanowienie", visible: true };
  }
  return { key: null, label: null, visible: false };
}

self.onmessage = (event) => {
  const payload = event?.data || {};
  if (payload.type !== "index") return;

  const cases = Array.isArray(payload.cases) ? payload.cases : [];
  const precomputeTextIndex = payload.precomputeTextIndex !== false;
  const paragraphIndex = [];
  const sectionSet = new Set();
  const typeSet = new Set();
  const yearSet = new Set();

  for (const [caseIndex, caseItem] of cases.entries()) {
    if (!caseItem) continue;
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
    const judgeNorm = (caseItem.judge_names || []).map((name) => normalizeSearchText(name)).filter(Boolean);

    for (const paragraph of caseItem.paragraphs || []) {
      const sectionKey = paragraph.section_key || "inne";
      sectionSet.add(sectionKey);
      const paragraphText = paragraph.text || "";

      paragraphIndex.push({
        caseIndex,
        caseSignatureNorm: signatureNorm,
        decisionTypeNorm: typeNorm,
        topicNorm,
        judgeNorm,
        year: caseItem.year || null,
        sectionKey,
        sectionLabel: paragraph.section_label,
        paragraph: {
          paragraph_id: paragraph.paragraph_id,
          paragraph_index: paragraph.paragraph_index,
          paragraph_number: paragraph.paragraph_number,
          section_key: sectionKey,
          section_label: paragraph.section_label,
          section_confidence: paragraph.section_confidence,
          text: paragraphText
        },
        textLegal: precomputeTextIndex ? normalizeLegalCitationText(paragraphText) : null
      });
    }

    if ((caseIndex + 1) % 10 === 0) {
      self.postMessage({
        type: "index-progress",
        processedCases: caseIndex + 1,
        totalCases: cases.length
      });
    }
  }

  self.postMessage({
    type: "indexed",
    paragraphIndex,
    sections: [...sectionSet].sort((a, b) => String(a).localeCompare(String(b), "pl")),
    decisionTypes: IPO_DECISION_TYPES
      .filter((entry) => typeSet.has(entry.key))
      .concat(IPO_DECISION_TYPES.filter((entry) => !typeSet.has(entry.key))),
    benchSizes: [...IPO_BENCH_SIZES],
    years: [...yearSet].sort((a, b) => a - b)
  });
};
