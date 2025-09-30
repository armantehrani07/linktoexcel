#!/usr/bin/env python3
"""
Export CSV and JSON files from a directory into a single Excel workbook,
one worksheet per source file (splits large files across sheets).
"""

from __future__ import annotations

import argparse
import glob as globlib
import json
import math
import os
import re
import sys
from typing import Iterator, List, Optional, Sequence, Tuple

import pandas as pd

EXCEL_MAX_ROWS = 1_048_576
EXCEL_SHEETNAME_MAXLEN = 31


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate CSV/JSON files into a single Excel workbook with one sheet per file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input-dir", "-i", default=".", help="Directory to search for input files")
    parser.add_argument("--glob", default="*.csv,*.json,*.ndjson,*.jsonl", help="Comma-separated glob patterns")
    parser.add_argument("--exclude", default="", help="Comma-separated glob patterns to exclude")
    parser.add_argument("--recursive", "-r", action="store_true", help="Recursively search subdirectories")
    parser.add_argument("--output", "-o", default="export.xlsx", help="Path to the output Excel file")
    parser.add_argument("--engine", choices=["openpyxl", "xlsxwriter"], default="openpyxl", help="Excel writer engine")
    parser.add_argument("--max-rows-per-sheet", type=int, default=EXCEL_MAX_ROWS,
                        help="Split across sheets if exceeded")
    parser.add_argument("--sheet-prefix", default="", help="Optional prefix for all sheet names")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be written without creating a file")
    parser.add_argument("--verbose", "-v", action="count", default=0, help="Increase verbosity (repeat for more)")
    return parser.parse_args(argv)


def debug_print(verbose: int, level: int, message: str) -> None:
    if verbose >= level:
        print(message, file=sys.stderr)


def split_comma_patterns(patterns: str) -> List[str]:
    if not patterns:
        return []
    return [p.strip() for p in patterns.split(",") if p.strip()]


def find_files(base_dir: str, include_globs: Sequence[str], exclude_globs: Sequence[str],
               recursive: bool, verbose: int) -> List[str]:
    base_dir = os.path.abspath(base_dir)
    files: List[str] = []
    for pattern in include_globs:
        full_pattern = os.path.join(base_dir, "**" if recursive else "", pattern)
        for path in globlib.glob(full_pattern, recursive=True):
            if os.path.isfile(path):
                files.append(os.path.abspath(path))

    # De-duplicate
    seen, unique_files = set(), []
    for f in files:
        if f not in seen:
            seen.add(f)
            unique_files.append(f)

    # Exclude
    if exclude_globs:
        exclude_set: set[str] = set()
        for ex in exclude_globs:
            ex_pattern = os.path.join(base_dir, "**" if recursive else "", ex)
            for path in globlib.glob(ex_pattern, recursive=True):
                if os.path.isfile(path):
                    exclude_set.add(os.path.abspath(path))
        unique_files = [p for p in unique_files if p not in exclude_set]

    debug_print(verbose, 1, f"Discovered {len(unique_files)} files")
    if verbose >= 2:
        for p in unique_files:
            debug_print(verbose, 2, f"  - {p}")
    return unique_files


def sanitize_sheet_name(name: str) -> str:
    # Excel sheet name rules: <= 31 chars, no : \ / ? * [ ]
    invalid = r"[:\\/\?\*\[\]]"
    sanitized = re.sub(invalid, "_", name).strip() or "Sheet"
    return sanitized[:EXCEL_SHEETNAME_MAXLEN]


def ensure_unique_name(base: str, existing: set[str]) -> str:
    name, counter = base, 1
    while name in existing:
        suffix = f"_{counter}"
        cutoff = EXCEL_SHEETNAME_MAXLEN - len(suffix)
        name = (base[:cutoff] if len(base) > cutoff else base) + suffix
        counter += 1
    existing.add(name)
    return name


def read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False)


def read_json_any(path: str) -> pd.DataFrame:
    lower = path.lower()
    if lower.endswith(".jsonl") or lower.endswith(".ndjson"):
        records: List[dict] = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return pd.json_normalize(records)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return pd.json_normalize(data)
    if isinstance(data, dict):
        list_keys = [k for k, v in data.items() if isinstance(v, list)]
        for k in list_keys:
            if all(isinstance(x, dict) for x in data[k]):
                return pd.json_normalize(data[k])
        return pd.json_normalize(data)
    return pd.DataFrame({"value": [data]})


def dataframe_chunks(df: pd.DataFrame, chunk_size: int) -> Iterator[pd.DataFrame]:
    if len(df) <= chunk_size:
        yield df
        return
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size]


def infer_sheet_base_name(file_path: str, sheet_prefix: str) -> str:
    stem = os.path.splitext(os.path.basename(file_path))[0]
    base = sanitize_sheet_name(stem)
    return sanitize_sheet_name(f"{sheet_prefix}{base}") if sheet_prefix else base


def read_to_dataframe(file_path: str) -> pd.DataFrame:
    lower = file_path.lower()
    if lower.endswith(".csv"):
        return read_csv(file_path)
    if lower.endswith(".json") or lower.endswith(".jsonl") or lower.endswith(".ndjson"):
        return read_json_any(file_path)
    raise ValueError(f"Unsupported file type: {file_path}")


def export_files_to_excel(files: Sequence[str], output_path: str, engine: str,
                          max_rows_per_sheet: int, sheet_prefix: str,
                          verbose: int, dry_run: bool) -> None:
    if not files:
        raise SystemExit("No input files found. Adjust --input-dir/--glob or check your data.")

    existing_sheet_names: set[str] = set()
    plan: List[Tuple[str, str, int]] = []  # (sheet_name, source, rows)

    dataframes: List[Tuple[str, pd.DataFrame, str]] = []
    for file_path in files:
        try:
            df = read_to_dataframe(file_path)
        except Exception as exc:
            debug_print(verbose, 1, f"Skipping {file_path}: {exc}")
            continue
        base_name = infer_sheet_base_name(file_path, sheet_prefix)
        dataframes.append((base_name, df, file_path))

    for base_name, df, src in dataframes:
        total_rows = len(df)
        if total_rows == 0:
            plan.append((ensure_unique_name(base_name, existing_sheet_names), src, 0))
            continue
        limit = max(EXCEL_MAX_ROWS if max_rows_per_sheet < 1 else max_rows_per_sheet, 1)
        num_chunks = math.ceil(total_rows / limit)
        for idx, chunk in enumerate(dataframe_chunks(df, limit), start=1):
            suffix = f"_{idx}" if num_chunks > 1 else ""
            sheet = ensure_unique_name(sanitize_sheet_name(base_name + suffix), existing_sheet_names)
            plan.append((sheet, src, len(chunk)))

    debug_print(verbose, 1, f"Preparing to write {len(plan)} sheet(s) to {output_path}")
    if verbose >= 2:
        for sheet, src, nrows in plan:
            debug_print(verbose, 2, f"  - {sheet} <- {src} ({nrows} rows)")

    if dry_run:
        print(f"[dry-run] Would write {len(plan)} sheets to {output_path}")
        return

    os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)
    with pd.ExcelWriter(output_path, engine=engine) as writer:  # type: ignore[arg-type]
        existing_sheet_names.clear()
        for base_name, df, _src in dataframes:
            total_rows = len(df)
            if total_rows == 0:
                sheet = ensure_unique_name(sanitize_sheet_name(base_name), existing_sheet_names)
                pd.DataFrame().to_excel(writer, sheet_name=sheet, index=False)
                continue
            limit = max(EXCEL_MAX_ROWS if max_rows_per_sheet < 1 else max_rows_per_sheet, 1)
            num_chunks = math.ceil(total_rows / limit)
            for idx, chunk in enumerate(dataframe_chunks(df, limit), start=1):
                suffix = f"_{idx}" if num_chunks > 1 else ""
                sheet = ensure_unique_name(sanitize_sheet_name(base_name + suffix), existing_sheet_names)
                chunk.to_excel(writer, sheet_name=sheet, index=False)

    debug_print(verbose, 1, f"Wrote Excel file: {output_path}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    include_patterns = split_comma_patterns(args.glob)
    exclude_patterns = split_comma_patterns(args.exclude)
    files = find_files(args.input_dir, include_patterns, exclude_patterns, args.recursive, args.verbose)
    try:
        export_files_to_excel(
            files=files,
            output_path=args.output,
            engine=args.engine,
            max_rows_per_sheet=args.max_rows_per_sheet,
            sheet_prefix=args.sheet_prefix,
            verbose=args.verbose,
            dry_run=args.dry_run,
        )
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
