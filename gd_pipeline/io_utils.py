from __future__ import annotations

import csv
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator


LOGGER = logging.getLogger(__name__)


def append_csv_row(path: Path, fieldnames: Iterable[str], row: Dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames_list = list(fieldnames)
    file_exists = path.exists() and path.stat().st_size > 0

    with path.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames_list)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
        fp.flush()
        os.fsync(fp.fileno())


def iter_csv_rows(path: Path) -> Iterator[Dict[str, str]]:
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return iter(())

    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        rows = [dict(row) for row in reader]
    return iter(rows)


def load_existing_ids_from_csv(path: Path, id_field: str) -> set[str]:
    ids: set[str] = set()
    for row in iter_csv_rows(path):
        value = (row.get(id_field) or "").strip()
        if value:
            ids.add(value)
    return ids


def append_jsonl_record(path: Path, record: Dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(line + "\n")
        fp.flush()
        os.fsync(fp.fileno())


def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return iter(())

    with path.open("r", encoding="utf-8") as fp:
        rows = []
        for line_no, line in enumerate(fp, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                LOGGER.warning("Skipping malformed JSONL line %s in %s", line_no, path.as_posix())
    return iter(rows)


def load_existing_ids_from_jsonl(path: Path, id_field: str) -> set[str]:
    ids: set[str] = set()
    for record in iter_jsonl(path):
        value = str(record.get(id_field, "")).strip()
        if value:
            ids.add(value)
    return ids


def read_json_file(path: Path, default: Dict[str, Any] | None = None) -> Dict[str, Any]:
    path = Path(path)
    if not path.exists():
        return default.copy() if default else {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default.copy() if default else {}


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=path.stem, suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
