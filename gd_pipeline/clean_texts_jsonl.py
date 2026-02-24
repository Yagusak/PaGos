from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import PipelineConfig
from .io_utils import iter_jsonl
from .text_quality import is_probably_garbage_text


def clean_texts_jsonl_file(path: Path, make_backup: bool = True) -> dict[str, int]:
    records_by_bill: dict[str, dict[str, Any]] = {}
    rows_read = 0
    malformed = 0

    for record in iter_jsonl(path):
        rows_read += 1
        bill_id = str(record.get("bill_id") or "").strip()
        if not bill_id:
            malformed += 1
            continue
        records_by_bill[bill_id] = record

    dropped_side_a = 0
    dropped_side_b = 0
    dropped_records = 0
    output_records: list[dict[str, Any]] = []

    for bill_id, record in records_by_bill.items():
        cleaned: dict[str, Any] = {"bill_id": bill_id}

        if "text_a" in record:
            text_a = record.get("text_a")
            if isinstance(text_a, str) and not is_probably_garbage_text(text_a):
                cleaned["text_a"] = text_a
            else:
                dropped_side_a += 1

        if "text_b" in record:
            text_b = record.get("text_b")
            if isinstance(text_b, str) and not is_probably_garbage_text(text_b):
                cleaned["text_b"] = text_b
            else:
                dropped_side_b += 1

        if "text_a" not in cleaned and "text_b" not in cleaned:
            dropped_records += 1
            continue

        output_records.append(cleaned)

    backup_path: Path | None = None
    if make_backup:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = path.with_suffix(path.suffix + f".bak_{stamp}")
        shutil.copy2(path, backup_path)

    fd, tmp_name = tempfile.mkstemp(prefix="texts_repair_", suffix=".jsonl.tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp_fp:
            for record in output_records:
                tmp_fp.write(json.dumps(record, ensure_ascii=False) + "\n")
            tmp_fp.flush()
            os.fsync(tmp_fp.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)

    return {
        "rows_read": rows_read,
        "unique_bills": len(records_by_bill),
        "written_records": len(output_records),
        "dropped_side_a": dropped_side_a,
        "dropped_side_b": dropped_side_b,
        "dropped_records": dropped_records,
        "malformed": malformed,
        "backup_created": 1 if backup_path else 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean poisoned text payloads from texts.jsonl.")
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts"))
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    config = PipelineConfig(output_dir=args.output_dir)
    path = config.texts_jsonl
    if not path.exists():
        raise SystemExit(f"texts.jsonl not found: {path.as_posix()}")

    stats = clean_texts_jsonl_file(path=path, make_backup=not args.no_backup)
    print(
        "clean_texts_jsonl:"
        f" rows_read={stats['rows_read']}"
        f" unique_bills={stats['unique_bills']}"
        f" written_records={stats['written_records']}"
        f" dropped_side_a={stats['dropped_side_a']}"
        f" dropped_side_b={stats['dropped_side_b']}"
        f" dropped_records={stats['dropped_records']}"
        f" malformed={stats['malformed']}"
        f" backup_created={stats['backup_created']}"
    )


if __name__ == "__main__":
    main()

