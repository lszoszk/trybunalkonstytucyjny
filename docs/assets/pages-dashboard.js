const PALETTE = [
  "#bf0d2e",
  "#8e0f28",
  "#dc2626",
  "#f97316",
  "#2563eb",
  "#16a34a",
  "#7c3aed",
  "#0ea5e9",
  "#334155",
  "#94a3b8"
];

const UI_PREFS_KEY = "tk_ui_prefs";

if (globalThis.Chart?.defaults) {
  globalThis.Chart.defaults.font.family = '"Sora", "Avenir Next", "Segoe UI", sans-serif';
  globalThis.Chart.defaults.color = "#334155";
  globalThis.Chart.defaults.plugins.legend.labels.boxWidth = 12;
  globalThis.Chart.defaults.plugins.legend.labels.usePointStyle = true;
}

function fmtInt(value) {
  return new Intl.NumberFormat("pl-PL").format(value || 0);
}

function fmtPct(value) {
  return `${(Number(value || 0) * 100).toFixed(2)}%`;
}

function normalizeSpace(value) {
  return String(value ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function kpi(label, value, note = "") {
  return `
    <article class="kpi-card">
      <p class="kpi-label">${label}</p>
      <p class="kpi-value">${value}</p>
      ${note ? `<p class="kpi-note">${note}</p>` : ""}
    </article>
  `;
}

function loadUiPrefs() {
  try {
    return JSON.parse(localStorage.getItem(UI_PREFS_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveViewMode(mode) {
  const prefs = loadUiPrefs();
  prefs.viewMode = mode === "student" ? "student" : "expert";
  localStorage.setItem(UI_PREFS_KEY, JSON.stringify(prefs));
}

function syncModeSwitchUi() {
  const studentLink = document.getElementById("analyticsSwitchStudent");
  const expertLink = document.getElementById("analyticsSwitchExpert");
  if (!studentLink || !expertLink) return;

  const mode = loadUiPrefs().viewMode === "student" ? "student" : "expert";
  studentLink.classList.toggle("active", mode === "student");
  expertLink.classList.toggle("active", mode === "expert");

  [studentLink, expertLink].forEach((link) => {
    link.addEventListener("click", () => {
      saveViewMode(link.dataset.mode || "expert");
    });
  });
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function buildBarChart(canvasId, labels, values, color = "#bf0d2e", horizontal = false) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: labels.map((_, index) => `${PALETTE[index % PALETTE.length]}cc`),
          borderColor: labels.map((_, index) => PALETTE[index % PALETTE.length]),
          borderWidth: 1,
          borderRadius: 8
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: horizontal ? "y" : "x",
      animation: {
        duration: 420
      },
      plugins: {
        legend: { display: false }
      },
      scales: {
        x: {
          beginAtZero: true,
          grid: {
            display: !horizontal,
            color: "rgba(148, 163, 184, 0.28)"
          },
          ticks: {
            color: "#475569",
            font: {
              size: 11
            }
          }
        },
        y: {
          beginAtZero: true,
          grid: {
            display: horizontal ? false : true,
            color: "rgba(148, 163, 184, 0.2)"
          },
          ticks: {
            color: "#475569",
            font: {
              size: 11
            }
          }
        }
      }
    }
  });
}

function buildDoughnut(canvasId, labels, values) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;

  new Chart(ctx, {
    type: "doughnut",
    data: {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: labels.map((_, index) => `${PALETTE[index % PALETTE.length]}cc`),
          borderColor: labels.map((_, index) => PALETTE[index % PALETTE.length]),
          borderWidth: 1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: {
        duration: 420
      },
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            color: "#334155",
            font: {
              size: 11,
              weight: 600
            }
          }
        }
      }
    }
  });
}

function tuplesToLabelsValues(rows, limit = Number.POSITIVE_INFINITY) {
  const truncated = (rows || []).slice(0, limit);
  return {
    labels: truncated.map((row) => row[0]),
    values: truncated.map((row) => row[1])
  };
}

function renderChartFallback(canvasId, message) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const host = canvas.parentElement;
  if (!host) return;

  canvas.style.display = "none";
  let note = host.querySelector(".chart-empty-note");
  if (!note) {
    note = document.createElement("p");
    note.className = "chart-note chart-empty-note";
    host.appendChild(note);
  }
  note.textContent = message;
}

function clearChartFallback(canvasId) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const host = canvas.parentElement;
  if (!host) return;

  canvas.style.display = "";
  const note = host.querySelector(".chart-empty-note");
  if (note) note.remove();
}

function renderBigramsFiltering(payload) {
  const disclaimerEl = document.getElementById("bigramsDisclaimer");
  const listEl = document.getElementById("removedBigramsList");
  if (!disclaimerEl || !listEl) return;

  const filtering = payload?.rankings?.bigrams_filtering || {};
  const removedRows = payload?.rankings?.bigrams_removed || [];
  const removedTotal = Number(filtering.removed_total_count || 0);
  const removedUnique = Number(filtering.removed_unique_count || 0);

  disclaimerEl.textContent = [
    filtering.disclaimer || "Brak dodatkowego filtrowania szumu językowego.",
    removedUnique
      ? `Usunięto ${fmtInt(removedTotal)} wystąpień (${fmtInt(removedUnique)} unikalnych bigramów).`
      : "Nie usunięto żadnych bigramów."
  ].join(" ");

  if (!removedRows.length) {
    listEl.innerHTML = "<li>Brak pozycji na liście usuniętych bigramów.</li>";
    return;
  }

  listEl.innerHTML = removedRows
    .map(([label, count]) => `<li><strong>${escapeHtml(label)}</strong> (${fmtInt(count)})</li>`)
    .join("");
}

function shortName(value, maxLen = 22) {
  const text = String(value || "");
  return text.length > maxLen ? `${text.slice(0, maxLen - 1)}…` : text;
}

function buildJudgeHeatmap(containerId, judges = [], matrix = []) {
  const container = document.getElementById(containerId);
  if (!container) return;

  if (!judges.length || !Array.isArray(matrix) || !matrix.length) {
    container.innerHTML = "<p class='kpi-note'>Brak danych do heatmapy współorzekania.</p>";
    return;
  }

  const maxValue = Math.max(
    1,
    ...matrix.flatMap((row) => row.map((value) => Number(value) || 0))
  );

  const headerCells = judges
    .map((judge) => `<th scope="col" title="${escapeHtml(judge)}">${escapeHtml(shortName(judge))}</th>`)
    .join("");

  const bodyRows = judges
    .map((rowJudge, rowIndex) => {
      const cells = judges
        .map((colJudge, colIndex) => {
          const value = Number(matrix[rowIndex]?.[colIndex]) || 0;
          const ratio = value / maxValue;
          const alpha = value > 0 ? Math.max(0.06, Math.min(0.92, ratio)) : 0.02;
          const bg = `rgba(191, 13, 46, ${alpha})`;
          const textColor = alpha >= 0.45 ? "#ffffff" : "#0f172a";
          const isDiagonal = rowIndex === colIndex;
          const title = isDiagonal
            ? `${rowJudge}: ${fmtInt(value)} spraw z udziałem sędziego`
            : `${rowJudge} + ${colJudge}: ${fmtInt(value)} spraw we wspólnym składzie`;
          return `<td class="heatmap-cell" style="background:${bg};color:${textColor}" title="${escapeHtml(title)}">${fmtInt(value)}</td>`;
        })
        .join("");

      return `
        <tr>
          <th scope="row" title="${escapeHtml(rowJudge)}">${escapeHtml(shortName(rowJudge, 28))}</th>
          ${cells}
        </tr>
      `;
    })
    .join("");

  container.innerHTML = `
    <table class="heatmap-table">
      <thead>
        <tr>
          <th scope="col">Sędzia</th>
          ${headerCells}
        </tr>
      </thead>
      <tbody>
        ${bodyRows}
      </tbody>
    </table>
  `;
}

async function initAnalytics() {
  const metaLine = document.getElementById("metaLine");
  const kpiGrid = document.getElementById("kpiGrid");

  syncModeSwitchUi();

  try {
    const response = await fetch("data/stats.json", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Nie udało się załadować stats.json (${response.status})`);
    }

    const payload = await response.json();
    const summary = payload.summary || {};
    const quality = payload.quality || {};

    const generatedAt = payload.generated_at ? new Date(payload.generated_at).toLocaleString("pl-PL") : "-";
    const hashPreview = String(payload.dataset_hash || "-").slice(0, 16);
    metaLine.textContent = `Korpus online (pełne składy) • Źródło: ${payload.source_file || "-"} • Wygenerowano: ${generatedAt} • Hash: ${hashPreview}`;

    kpiGrid.innerHTML = [
      kpi("Sprawy", fmtInt(summary.total_cases || 0)),
      kpi("Akapity", fmtInt(summary.total_paragraphs || 0)),
      kpi("Śr. akapitów/sprawa", Number(summary.avg_paragraphs_per_case || 0).toFixed(1)),
      kpi("Typy orzeczeń (IPO)", fmtInt(summary.unique_decision_types_ipo || 0)),
      kpi(
        "Zakres lat",
        summary.date_range ? `${summary.date_range.from}–${summary.date_range.to}` : "-",
        `${fmtInt(summary.dated_cases || 0)} datowanych`
      ),
      kpi("Nieznane sekcje", fmtPct(quality.section_unknown_share || 0), "target < 0.5%")
    ].join("");

    const years = tuplesToLabelsValues(payload.series?.cases_by_year || [], 64);
    buildBarChart("casesYearChart", years.labels, years.values, "#334155", false);

    const types = tuplesToLabelsValues(payload.rankings?.decision_types_ipo || payload.rankings?.decision_types || [], 10);
    buildDoughnut("decisionTypeChart", types.labels, types.values);

    const benchesIpo = tuplesToLabelsValues(payload.rankings?.bench_sizes_ipo || [], 5);
    buildBarChart("benchIpoChart", benchesIpo.labels, benchesIpo.values, "#334155", false);

    const sectionsFiltered = (payload.rankings?.sections || [])
      .filter((row) => normalizeSpace(row?.[0]) && Number(row?.[1]) > 0);
    const sections = tuplesToLabelsValues(sectionsFiltered, 12);
    if (sections.labels.length) {
      clearChartFallback("sectionsChart");
      buildBarChart("sectionsChart", sections.labels, sections.values, "#bf0d2e", true);
    } else {
      renderChartFallback("sectionsChart", "Brak danych sekcyjnych.");
    }

    const judges = tuplesToLabelsValues(payload.rankings?.judges || [], 10);
    buildBarChart("judgesChart", judges.labels, judges.values, "#2563eb", true);

    const bigrams = tuplesToLabelsValues(payload.rankings?.bigrams || [], 12);
    const bigramsCanvas = document.getElementById("bigramsChart");
    if (bigramsCanvas) {
      const dynamicHeight = Math.max(360, bigrams.labels.length * 31);
      bigramsCanvas.style.height = `${dynamicHeight}px`;
    }
    buildBarChart(
      "bigramsChart",
      bigrams.labels.map((label) => (label.length > 46 ? `${label.slice(0, 45)}…` : label)),
      bigrams.values,
      "#0f766e",
      true
    );
    renderBigramsFiltering(payload);

    buildJudgeHeatmap("judgeHeatmap", payload.cooccurrence?.judges || [], payload.cooccurrence?.matrix || []);
  } catch (error) {
    metaLine.textContent = error.message || "Błąd ładowania statystyk";
    kpiGrid.innerHTML = "";
    console.error(error);
  }
}

initAnalytics();
