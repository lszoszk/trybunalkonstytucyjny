#!/usr/bin/env python3
"""
Publish the full TK dataset to a Hugging Face dataset repository.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Iterable

from huggingface_hub import CommitOperationDelete, HfApi


FULL_PATTERNS = [
    "tk_cases.jsonl",
    "tk_cases_full_bench.jsonl",
    "similar_cases.json",
    "similar_cases_full_bench.json",
    "stats.json",
    "lemma_shards/full/**/*",
    "lemma_shards/full_bench/**/*",
]

CORE_PATTERNS = [
    "tk_cases.jsonl",
    "tk_cases_full_bench.jsonl",
]

SAMPLE_PATTERNS = [
    "tk_cases_sample50.jsonl",
    "tk_cases_sample200.jsonl",
    "similar_cases_sample50.json",
    "similar_cases_sample200.json",
    "lemma_shards/sample200/**/*",
]

DEFAULT_APP_URL = "https://lszoszk.github.io/trybunalkonstytucyjny/"
DEFAULT_REPO_URL = "https://github.com/lszoszk/trybunalkonstytucyjny"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload the full dataset snapshot to a Hugging Face dataset repo."
    )
    parser.add_argument(
        "--repo-id",
        required=True,
        help="Target dataset repo, e.g. 'your-user/constcourt-full'.",
    )
    parser.add_argument(
        "--data-dir",
        default="docs/data",
        help="Directory that contains tk_cases*.jsonl and lemma_shards/.",
    )
    parser.add_argument(
        "--revision",
        default="main",
        help="Branch/revision to upload to (default: main).",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="HF token. If omitted, uses HF_TOKEN/HUGGINGFACE_TOKEN or cached login.",
    )
    parser.add_argument(
        "--private",
        action="store_true",
        help="Create or keep the target dataset repo private.",
    )
    parser.add_argument(
        "--include-samples",
        action="store_true",
        help="Also upload sample50/sample200 files and sample200 shards.",
    )
    parser.add_argument(
        "--profile",
        choices=["full", "core"],
        default="full",
        help="Dataset profile: 'full' keeps auxiliary files; 'core' keeps only full + full_bench JSONL.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print selected files and exit without uploading.",
    )
    parser.add_argument(
        "--commit-message",
        default=None,
        help="Optional commit message override.",
    )
    parser.add_argument(
        "--app-url",
        default=DEFAULT_APP_URL,
        help="Public URL of the dashboard application.",
    )
    parser.add_argument(
        "--repo-url",
        default=DEFAULT_REPO_URL,
        help="Source code repository URL.",
    )
    parser.add_argument(
        "--readme-only",
        action="store_true",
        help="Upload only README.md (skip data payload upload).",
    )
    parser.add_argument(
        "--prune-remote",
        action="store_true",
        help="Delete files from remote dataset repo that are not part of the current selected set.",
    )
    return parser.parse_args()


def format_bytes(num_bytes: int) -> str:
    size = float(num_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{num_bytes} B"


def select_files(data_dir: Path, patterns: Iterable[str]) -> Dict[str, Path]:
    selected: Dict[str, Path] = {}
    missing_patterns: list[str] = []
    for pattern in patterns:
        matches = [path for path in data_dir.glob(pattern) if path.is_file()]
        if not matches:
            missing_patterns.append(pattern)
            continue
        for path in matches:
            rel = path.relative_to(data_dir).as_posix()
            selected[rel] = path
    if missing_patterns:
        joined = "\n- ".join(missing_patterns)
        raise FileNotFoundError(f"Missing required files for patterns:\n- {joined}")
    return dict(sorted(selected.items(), key=lambda item: item[0]))


def load_stats(stats_path: Path) -> dict:
    if not stats_path.exists():
        raise FileNotFoundError(f"Missing stats file: {stats_path}")
    with stats_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_token(explicit_token: str | None) -> str | None:
    if explicit_token:
        return explicit_token
    return os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_TOKEN")


def build_dataset_card(
    repo_id: str,
    stats: dict,
    selected_files: Dict[str, Path],
    total_bytes: int,
    include_samples: bool,
    app_url: str,
    repo_url: str,
) -> str:
    summary = stats.get("summary", {})
    date_range = summary.get("date_range", {})

    total_cases = summary.get("total_cases", "n/a")
    total_paragraphs = summary.get("total_paragraphs", "n/a")
    from_year = date_range.get("from", "n/a")
    to_year = date_range.get("to", "n/a")
    generated_at = stats.get("dataset_generated_at") or stats.get("generated_at", "n/a")
    dataset_hash = stats.get("dataset_hash", "n/a")
    normalization_version = stats.get("normalization_version", "n/a")
    app_base = app_url.rstrip("/") + "/"

    file_lines = [
        f"- `data/{rel_path}` ({format_bytes(path.stat().st_size)})"
        for rel_path, path in selected_files.items()
    ]
    file_listing = "\n".join(file_lines)
    sample_note = "yes" if include_samples else "no"
    profile_name = "core" if set(selected_files.keys()) == set(CORE_PATTERNS) else "full"
    contains_lines: list[str] = []

    if "tk_cases.jsonl" in selected_files:
        contains_lines.append(
            "- `data/tk_cases.jsonl`: full normalized corpus of Polish Constitutional Tribunal decisions (metadata + paragraph-level text units)."
        )
    if "tk_cases_full_bench.jsonl" in selected_files:
        contains_lines.append("- `data/tk_cases_full_bench.jsonl`: subset of decisions resolved in full bench.")
    if any(name.startswith("similar_cases") for name in selected_files):
        contains_lines.append("- `data/similar_cases*.json`: precomputed nearest-case links for retrieval and exploration.")
    if "stats.json" in selected_files:
        contains_lines.append("- `data/stats.json`: aggregate counts, rankings, date coverage, and dataset fingerprint metadata.")
    if any(name.startswith("lemma_shards/") for name in selected_files):
        contains_lines.append("- `data/lemma_shards/*`: lemma/form/POS shards used for lemma-aware search in the expert dashboard.")
    contains_section = "\n".join(contains_lines) if contains_lines else "- Core files only."

    return f"""---
language:
- pl
license: other
pretty_name: Trybunal Konstytucyjny Full Corpus
tags:
- legal
- constitutional-law
- polish
- retrieval
---

# Trybunal Konstytucyjny Full Corpus

This dataset repository stores a full snapshot of normalized decisions used by the `trybunalkonstytucyjny` dashboard.

## Snapshot metadata

- HF dataset repo: `{repo_id}`
- Source repository: `{repo_url}`
- Active profile: `{profile_name}`
- Total files uploaded: {len(selected_files)}
- Total payload size: {format_bytes(total_bytes)}
- Cases: {total_cases}
- Paragraphs: {total_paragraphs}
- Date range: {from_year}-{to_year}
- Dataset hash: `{dataset_hash}`
- Dataset generated at: `{generated_at}`
- Normalization version: `{normalization_version}`
- Includes sample datasets: {sample_note}

## What this dataset contains

{contains_section}

## Application

- Live app (landing): {app_base}
- Expert search dashboard: {app_base}docs/dashboard-expert.html
- Analytics dashboard: {app_base}docs/analytics.html

## Files

{file_listing}

## Notes

- Main training/evaluation source: `data/tk_cases.jsonl`
- Full-bench subset: `data/tk_cases_full_bench.jsonl`
- Lemma shards are under `data/lemma_shards/full*`
"""


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        raise SystemExit(f"Data directory does not exist: {data_dir}")

    patterns = list(FULL_PATTERNS if args.profile == "full" else CORE_PATTERNS)
    if args.include_samples:
        patterns.extend(SAMPLE_PATTERNS)

    selected_files = select_files(data_dir, patterns)
    total_bytes = sum(path.stat().st_size for path in selected_files.values())

    stats = load_stats(data_dir / "stats.json")
    dataset_hash = str(stats.get("dataset_hash", "")).strip()
    commit_message = args.commit_message or (
        f"Upload dataset snapshot {dataset_hash[:12]}" if dataset_hash else "Upload dataset snapshot"
    )

    dataset_card = build_dataset_card(
        repo_id=args.repo_id,
        stats=stats,
        selected_files=selected_files,
        total_bytes=total_bytes,
        include_samples=args.include_samples,
        app_url=args.app_url,
        repo_url=args.repo_url,
    )

    print(f"Selected files: {len(selected_files)}")
    print(f"Selected size: {format_bytes(total_bytes)}")
    for rel_path, path in selected_files.items():
        print(f"- {rel_path} ({format_bytes(path.stat().st_size)})")

    if args.dry_run:
        print("\nDry run only. Nothing uploaded.")
        return 0

    token = resolve_token(args.token)
    api = HfApi()

    try:
        whoami = api.whoami(token=token)
    except Exception as exc:  # pragma: no cover - auth path depends on runtime
        raise SystemExit(
            "Hugging Face authentication required. "
            "Run `huggingface-cli login` or provide `--token` / `HF_TOKEN`."
        ) from exc

    user = whoami.get("name") or whoami.get("fullname") or "unknown-user"
    print(f"\nAuthenticated as: {user}")

    print(f"Ensuring dataset repo exists: {args.repo_id}")
    api.create_repo(
        repo_id=args.repo_id,
        repo_type="dataset",
        token=token,
        private=args.private,
        exist_ok=True,
    )

    print("Uploading dataset card (README.md)...")
    api.upload_file(
        repo_id=args.repo_id,
        repo_type="dataset",
        token=token,
        revision=args.revision,
        path_in_repo="README.md",
        path_or_fileobj=dataset_card.encode("utf-8"),
        commit_message=commit_message,
    )

    if not args.readme_only:
        print("Uploading dataset payload to data/ ...")
        api.upload_folder(
            repo_id=args.repo_id,
            repo_type="dataset",
            token=token,
            revision=args.revision,
            folder_path=data_dir,
            path_in_repo="data",
            allow_patterns=list(selected_files.keys()),
            ignore_patterns=[".DS_Store", "**/.DS_Store"],
            commit_message=commit_message,
        )

    if args.prune_remote:
        print("Pruning remote files not in selected set ...")
        keep_paths = {"README.md", ".gitattributes"}
        keep_paths.update(f"data/{rel}" for rel in selected_files)

        remote_files = api.list_repo_files(
            repo_id=args.repo_id,
            repo_type="dataset",
            token=token,
            revision=args.revision,
        )
        delete_paths = [
            path for path in remote_files if path.startswith("data/") and path not in keep_paths
        ]
        if delete_paths:
            api.create_commit(
                repo_id=args.repo_id,
                repo_type="dataset",
                token=token,
                revision=args.revision,
                commit_message=f"Prune dataset repo ({args.profile} profile)",
                operations=[CommitOperationDelete(path_in_repo=path) for path in delete_paths],
            )
            print(f"Deleted remote files: {len(delete_paths)}")
        else:
            print("No remote files to prune.")

    suffix = " (README only)" if args.readme_only else ""
    print(f"\nDone{suffix}: https://huggingface.co/datasets/{args.repo_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
