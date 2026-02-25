#!/usr/bin/env python3
"""
Build lemma shards for dashboard runtime search.

Output structure:
  docs/data/lemma_shards/<dataset_key>/
    manifest.json
    forms-001.json ...
    lemmas-001.json ...
    lemma-pos-001.json ...
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple


SCHEMA_VERSION = "tk-lemma-shards-v2"
DEFAULT_MAX_TERMS_PER_SHARD = 20000
TOKEN_SPLIT_RE = re.compile(r"[^\w-]+", flags=re.UNICODE)
DATE_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ART_RE = re.compile(r"\bart\.?\s*(\d+[a-z]?)", flags=re.IGNORECASE)


def normalize_space(value: object) -> str:
    text = str(value if value is not None else "")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_search_text(value: object) -> str:
    text = normalize_space(value).lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text


def normalize_legal_citation_text(value: object) -> str:
    text = normalize_search_text(value)
    text = ART_RE.sub(r"art\1", text)
    text = re.sub(r"\s*§\s*", " § ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_date_iso(value: object) -> float:
    text = normalize_space(value)
    if not DATE_ISO_RE.match(text):
        return 0.0
    try:
        return datetime.strptime(text, "%Y-%m-%d").timestamp()
    except ValueError:
        return 0.0


def load_jsonl(path: Path) -> List[dict]:
    rows: List[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid JSONL at line {line_no}: {exc}") from exc
            if not isinstance(parsed, dict):
                continue
            rows.append(parsed)
    return rows


def sort_cases_for_runtime(cases: Sequence[dict]) -> List[dict]:
    # Match runtime ordering used before paragraph indexing:
    # date desc, then case signature asc.
    return sorted(
        cases,
        key=lambda item: (
            -parse_date_iso(item.get("decision_date_iso")),
            normalize_space(item.get("case_signature")),
        ),
    )


def iter_paragraph_texts(sorted_cases: Sequence[dict]) -> Iterable[Tuple[int, str]]:
    pid = 0
    for case in sorted_cases:
        paragraphs = case.get("paragraphs") or []
        if not isinstance(paragraphs, list):
            continue
        for paragraph in paragraphs:
            if not isinstance(paragraph, dict):
                continue
            text = normalize_space(paragraph.get("text"))
            yield pid, text
            pid += 1


def tokenize_text_for_lemma(text: str) -> List[str]:
    normalized = normalize_legal_citation_text(text)
    if not normalized:
        return []
    return [token for token in TOKEN_SPLIT_RE.split(normalized) if token]


class MorfeuszLemmatizer:
    def __init__(self) -> None:
        try:
            import morfeusz2  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "Missing dependency 'morfeusz2'. Install with: pip install morfeusz2"
            ) from exc

        self._module = morfeusz2
        self._engine = morfeusz2.Morfeusz()
        self.version = str(getattr(morfeusz2, "__version__", "unknown"))

    def lemmatize_token(self, token: str) -> List[str]:
        normalized_token = normalize_legal_citation_text(token)
        if not normalized_token:
            return []

        lemmas: Set[str] = set()
        try:
            analyses = self._engine.analyse(token)
        except Exception:
            analyses = []

        for item in analyses:
            if not isinstance(item, (list, tuple)) or len(item) < 3:
                continue
            interp = item[2]
            lemma_raw = ""
            if isinstance(interp, dict):
                lemma_raw = str(interp.get("lemma", ""))
            elif isinstance(interp, (list, tuple)) and len(interp) >= 2:
                lemma_raw = str(interp[1])
            if not lemma_raw:
                continue
            lemma_base = lemma_raw.split(":", 1)[0]
            lemma_norm = normalize_legal_citation_text(lemma_base)
            if lemma_norm:
                lemmas.add(lemma_norm)

        if not lemmas:
            lemmas.add(normalized_token)
        return sorted(lemmas)


def split_to_shards(
    mapping: Dict[str, Any],
    prefix: str,
    max_terms_per_shard: int,
) -> Tuple[List[dict], Dict[str, dict]]:
    keys = sorted(mapping.keys())
    shard_meta: List[dict] = []
    shard_payloads: Dict[str, dict] = {}

    shard_index = 0
    for start in range(0, len(keys), max_terms_per_shard):
        shard_index += 1
        chunk = keys[start : start + max_terms_per_shard]
        shard_id = f"{prefix}-{shard_index:03d}"
        file_name = f"{shard_id}.json"

        payload = {term: mapping[term] for term in chunk}
        shard_payloads[file_name] = payload
        shard_meta.append(
            {
                "id": shard_id,
                "url": file_name,
                "from": chunk[0] if chunk else "",
                "to": chunk[-1] if chunk else "",
                "term_count": len(chunk),
            }
        )

    return shard_meta, shard_payloads


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
        handle.write("\n")


def build_lemma_shards(
    input_path: Path,
    outdir: Path,
    dataset_key: str,
    max_terms_per_shard: int,
) -> None:
    rows = load_jsonl(input_path)
    if not rows:
        raise RuntimeError(f"Input JSONL is empty: {input_path}")

    sorted_cases = sort_cases_for_runtime(rows)
    dataset_hash = normalize_space(sorted_cases[0].get("dataset_hash")) if sorted_cases else ""
    normalization_version = normalize_space(sorted_cases[0].get("normalization_version")) if sorted_cases else ""

    lemmatizer = MorfeuszLemmatizer()

    forms_to_lemmas: Dict[str, Set[str]] = defaultdict(set)
    lemma_to_postings: Dict[str, Set[int]] = defaultdict(set)
    lemma_to_positions: Dict[str, Dict[int, List[int]]] = defaultdict(lambda: defaultdict(list))
    paragraph_count = 0

    for pid, text in iter_paragraph_texts(sorted_cases):
        paragraph_count += 1
        for position, token in enumerate(tokenize_text_for_lemma(text)):
            lemmas = sorted(set(lemmatizer.lemmatize_token(token)))
            if not lemmas:
                continue
            forms_to_lemmas[token].update(lemmas)
            for lemma in lemmas:
                lemma_to_postings[lemma].add(pid)
                lemma_to_positions[lemma][pid].append(position)

    forms_map: Dict[str, List[str]] = {
        term: sorted(values) for term, values in forms_to_lemmas.items()
    }
    lemmas_map: Dict[str, List[int]] = {
        lemma: sorted(values) for lemma, values in lemma_to_postings.items()
    }
    lemma_positions_map: Dict[str, List[List[Any]]] = {}
    for lemma, pid_positions in lemma_to_positions.items():
        serialized_rows: List[List[Any]] = []
        for pid in sorted(pid_positions.keys()):
            positions = sorted(set(pid_positions[pid]))
            if not positions:
                continue
            serialized_rows.append([pid, positions])
        if serialized_rows:
            lemma_positions_map[lemma] = serialized_rows

    forms_meta, forms_payloads = split_to_shards(
        mapping=forms_map,
        prefix="forms",
        max_terms_per_shard=max_terms_per_shard,
    )
    lemmas_meta, lemmas_payloads = split_to_shards(
        mapping=lemmas_map,
        prefix="lemmas",
        max_terms_per_shard=max_terms_per_shard,
    )
    lemma_positions_meta, lemma_positions_payloads = split_to_shards(
        mapping=lemma_positions_map,
        prefix="lemma-pos",
        max_terms_per_shard=max_terms_per_shard,
    )

    target_dir = outdir / dataset_key
    target_dir.mkdir(parents=True, exist_ok=True)

    for file_name, payload in forms_payloads.items():
        write_json(target_dir / file_name, payload)
    for file_name, payload in lemmas_payloads.items():
        write_json(target_dir / file_name, payload)
    for file_name, payload in lemma_positions_payloads.items():
        write_json(target_dir / file_name, payload)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "dataset_hash": dataset_hash,
        "normalization_version": normalization_version,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "lemma_engine": {
            "name": "morfeusz2",
            "version": lemmatizer.version,
        },
        "forms_shards": forms_meta,
        "lemmas_shards": lemmas_meta,
        "lemma_positions_shards": lemma_positions_meta,
    }
    write_json(target_dir / "manifest.json", manifest)

    print("Lemma shard build complete.")
    print(f"Input: {input_path}")
    print(f"Output dir: {target_dir}")
    print(f"Cases: {len(sorted_cases)}")
    print(f"Paragraphs: {paragraph_count}")
    print(f"Forms: {len(forms_map)} in {len(forms_meta)} shard(s)")
    print(f"Lemmas: {len(lemmas_map)} in {len(lemmas_meta)} shard(s)")
    print(f"Lemma positions: {len(lemma_positions_map)} in {len(lemma_positions_meta)} shard(s)")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build lemma shards from normalized JSONL")
    parser.add_argument("--input", required=True, help="Path to normalized dataset JSONL")
    parser.add_argument(
        "--outdir",
        default="docs/data/lemma_shards",
        help="Output base directory for shards",
    )
    parser.add_argument(
        "--dataset-key",
        default="full",
        help="Target dataset key directory (e.g. full, full_bench, sample200)",
    )
    parser.add_argument(
        "--max-terms-per-shard",
        type=int,
        default=DEFAULT_MAX_TERMS_PER_SHARD,
        help="Max unique terms per shard file",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    if args.max_terms_per_shard <= 0:
        raise RuntimeError("--max-terms-per-shard must be > 0")

    input_path = Path(args.input).resolve()
    if not input_path.exists():
        raise RuntimeError(f"Input file not found: {input_path}")

    outdir = Path(args.outdir).resolve()
    dataset_key = normalize_space(args.dataset_key) or "full"
    dataset_key = dataset_key.replace(" ", "_")

    build_lemma_shards(
        input_path=input_path,
        outdir=outdir,
        dataset_key=dataset_key,
        max_terms_per_shard=args.max_terms_per_shard,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
