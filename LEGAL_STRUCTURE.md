# TK Dataset Structuring & Segmentation (Legal IA)

## 1) Structuring (recommended legal architecture)

### A. Hierarchy
- Case level (`sprawa`): sygnatura, data, rodzaj orzeczenia, skład, publikacja.
- Document level (`dokument`): individual judgment/postanowienie within a case.
- Section level (legal function): `tenor`, `orzeka`, `postanawia`, `uzasadnienie_*`, `zdanie_odrebne`.
- Paragraph level (search unit): atomic text item with section assignment.

### B. Canonical section taxonomy for TK
- `komparycja`
- `tenor`
- `orzeka`
- `postanawia`
- `uzasadnienie_historyczne`
- `uzasadnienie_postepowanie`
- `uzasadnienie_prawne`
- `uzasadnienie_ogolne`
- `zdanie_odrebne`
- `sentencja_inna`
- `inne`

### C. Why this is legally efficient
- Lawyers usually search in a **functional zone**, not in full text:
  - holding/disposition: `tenor`, `orzeka`, `postanawia`
  - ratio and constitutional test: `uzasadnienie_prawne`
  - procedural background: `uzasadnienie_historyczne`, `uzasadnienie_postepowanie`
  - doctrinal divergence: `zdanie_odrebne`
- This structure improves precision for analytics and legal retrieval by reducing false positives from irrelevant sections.

## 2) Segmentation (recommended search result unit)

### Primary unit
- **Paragraph hit** (`paragraph_id`) tied to:
  - case/document identifiers
  - section key + label
  - paragraph number/index
  - raw text

### Presentation unit
- **Grouped-by-case results**, with paragraph hits underneath.
- Default show top 3 hits per case, then expand.

### Why this is legally efficient
- Preserves doctrinal context (lawyers read in case context).
- Keeps evidence atomic for citation/export (paragraph granularity).
- Supports section-scoped searching and downstream model features.

## 3) Implemented in this repository

- Normalization script: `scripts/build_tk_dashboard_data.mjs`
- Normalized dataset output: `docs/data/tk_cases.jsonl`
- Search app: `docs/index.html`
- Analytics page: `docs/analytics.html`

The pipeline uses table-of-contents anchors where available and maps Roman-numeral section headers to canonical legal sections.
