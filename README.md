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
- Optional Expert-mode lemmatization via precomputed shards (`docs/data/lemma_shards/*`) with automatic fallback to classic search
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

Incremental scrape for missing older IPO records (time range `od 1986 roku`) while skipping already-known `document_id` from `tk-all.decisions.json`:

```bash
npm run scrape:ipo:missing-1986
npm run merge:ipo:missing-1986
npm run build:refresh-from-tk-all
```

Equivalent raw CLI (if you need custom limits/paths):

```bash
npm run scrape:ipo -- --limit 5000 --pool-size 5000 --results-per-page 500 --concurrency 3 --time-range "od 1986 roku" --existing-decisions output/playwright/tk-all.decisions.json --output-prefix tk-missing-1986
node scripts/merge_ipo_decisions.mjs --base output/playwright/tk-all.decisions.json --incremental output/playwright/tk-missing-1986.decisions.json --output output/playwright/tk-all.decisions.json
npm run build:refresh-from-tk-all
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

## 3a) Build lemma shards (optional, Expert dashboard)

Install Morfeusz2 first:

```bash
pip install morfeusz2
```

Build shards:

```bash
npm run build:lemma-shards:full
npm run build:lemma-shards:full-bench
npm run build:lemma-shards:sample200
# optional:
npm run build:lemma-shards:all
```

Generated structure:

- `docs/data/lemma_shards/full/manifest.json`
- `docs/data/lemma_shards/full/forms-*.json`
- `docs/data/lemma_shards/full/lemmas-*.json`
- `docs/data/lemma_shards/full/lemma-pos-*.json`
- `docs/data/lemma_shards/full_bench/manifest.json`
- `docs/data/lemma_shards/full_bench/forms-*.json`
- `docs/data/lemma_shards/full_bench/lemmas-*.json`
- `docs/data/lemma_shards/full_bench/lemma-pos-*.json`
- `docs/data/lemma_shards/sample200/...`

Runtime behavior:

- Expert dashboard has an optional `Lematyzacja` checkbox.
- In lemmatization mode, quoted phrases use lemma-aware positional matching (order-preserving), e.g. `"nadużycia prawa"` can match `"nadużycie prawa"`.
- When shards are unavailable (including local uploads without shards, hash mismatch, or shard fetch errors), search automatically falls back to classic mode without blocking the UI.

## 4) Run the dashboard locally

```bash
npm run preview:docs
```

Open:

- Landing: `http://localhost:4173/index.html`
- Dashboard Expert: `http://localhost:4173/dashboard-expert.html`
- Dashboard Student: `http://localhost:4173/dashboard-student.html`
- Analytics: `http://localhost:4173/analytics.html`

## 5) Publish full dataset to Hugging Face (first time)

Install/upload prerequisites:

```bash
pip install huggingface_hub
huggingface-cli login
```

Dry-run (verifies file selection and total payload size only):

```bash
npm run publish:hf:dataset -- --repo-id <your-hf-user>/<dataset-repo> --dry-run
```

Upload full dataset snapshot (`tk_cases`, `full_bench`, `similar_cases`, `lemma_shards/full*`):

```bash
npm run publish:hf:dataset -- --repo-id <your-hf-user>/<dataset-repo>
```

Optional: include sample datasets too:

```bash
npm run publish:hf:dataset -- --repo-id <your-hf-user>/<dataset-repo> --include-samples
```

Keep only core files (`tk_cases.jsonl` + `tk_cases_full_bench.jsonl`) and prune the rest on HF:

```bash
npm run publish:hf:dataset -- --repo-id <your-hf-user>/<dataset-repo> --profile core --prune-remote
```

Update only dataset card/description on HF (no data re-upload):

```bash
npm run publish:hf:dataset -- --repo-id <your-hf-user>/<dataset-repo> --readme-only
```

## Structure and legal IA

See:

- `LEGAL_STRUCTURE.md`

It documents the recommended legal section taxonomy and paragraph-level segmentation strategy used by this dashboard.

## Key files

- Scraper: `scripts/scrape_ipo_decisions.mjs`
- Incremental merge helper: `scripts/merge_ipo_decisions.mjs`
- Data build: `scripts/build_tk_dashboard_data.mjs`
- Landing: `docs/index.html`
- Search app (expert): `docs/dashboard-expert.html`
- Search app (student): `docs/dashboard-student.html`
- Search logic: `docs/assets/search-app.js`
- Analytics page: `docs/analytics.html`
- Analytics logic: `docs/assets/pages-dashboard.js`
