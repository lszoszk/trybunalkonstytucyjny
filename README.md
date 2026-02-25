# trybunalkonstytucyjny

This repository (`trybunalkonstytucyjny`) includes two connected parts:

- **Playwright scraper** for `https://ipo.trybunal.gov.pl/ipo/Szukaj?cid=1`
- **Static legal search dashboard** (inspired by your ECHR dashboard workflow)

The dashboard is focused on **paragraph-level legal retrieval** with section-aware filtering for TK decisions.

Repo URL: `https://github.com/lszoszk/trybunalkonstytucyjny`

## Litigation-grade hardening (implemented)

- Immutable export metadata (`dataset_hash`, generation timestamps, query + filters, tool version)
- Stable citation locators (`case_signature`, `document_id`, `paragraph_index`, `paragraph_number`, `section_key`, `source_url`)
- Quote package export with +/- 1 paragraph context
- Strict fielded + boolean query parser (`sygn:`, `sedzia:`, `typ:`, `sekcja:`, `rok:`, `teza:` + `AND/OR/NOT` + parentheses)
- Legal citation normalization (`art. 32`, `Art 32`, `art32` equivalence)
- Case Folder workflow (pin case/paragraph, notes, compare view, chronology, dossier + argument matrix export)
- URL state persistence (`?q=&sections=&types=&year_from=&year_to=&judge=&signature=`)
- Worker-based indexing, load guardrails (size limit, progress, cancel), schema checks, friendly upload errors
- Large local upload support up to 320 MB with streaming parsing for files over 80 MB (JSONL and JSON arrays)
- Keyboard shortcuts (`/`, `f`, `e`) and print stylesheet

## 1) Install

```bash
npm install
```

If Playwright asks for browser binaries:

```bash
npx playwright install chromium
```

## 2) Scrape decisions

Example (50 decisions):

```bash
npm run scrape:ipo -- --limit 50 --results-per-page 500 --output-prefix sample-50-v2
```

Main raw output:

- `output/playwright/<prefix>.decisions.json`

## 3) Build dashboard data (normalization + stats)

```bash
node scripts/build_tk_dashboard_data.mjs --input output/playwright/sample-50-v2.decisions.json
```

Generated files:

- `docs/data/tk_cases.jsonl`
- `docs/data/tk_cases_sample50.jsonl`
- `docs/data/stats.json`

## 4) Run the dashboard locally

```bash
npm run preview:docs
```

Open:

- Landing: `http://localhost:4173/index.html`
- Dashboard Expert: `http://localhost:4173/dashboard-expert.html`
- Dashboard Student: `http://localhost:4173/dashboard-student.html`
- Analytics: `http://localhost:4173/analytics.html`

## Structure and legal IA

See:

- `LEGAL_STRUCTURE.md`

It documents the recommended legal section taxonomy and paragraph-level segmentation strategy used by this dashboard.

## Key files

- Scraper: `scripts/scrape_ipo_decisions.mjs`
- Data build: `scripts/build_tk_dashboard_data.mjs`
- Landing: `docs/index.html`
- Search app (expert): `docs/dashboard-expert.html`
- Search app (student): `docs/dashboard-student.html`
- Search logic: `docs/assets/search-app.js`
- Analytics page: `docs/analytics.html`
- Analytics logic: `docs/assets/pages-dashboard.js`
