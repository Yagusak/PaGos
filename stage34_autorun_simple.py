from __future__ import annotations

import csv
import json
import logging
import msvcrt
import os
import subprocess
import sys
import time
from pathlib import Path


ARTIFACTS_DIR = Path("artifacts")
DOCS_CSV = ARTIFACTS_DIR / "documents.csv"
TEXTS_JSONL = ARTIFACTS_DIR / "texts.jsonl"
INSTANCE_LOCK_FILE = ARTIFACTS_DIR / "state" / "stage34_autorun_simple.lock"
LOG_PATH = ARTIFACTS_DIR / "logs" / "stage34_autorun_simple.log"
POLL_SECONDS = 120


def setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def acquire_single_instance_lock() -> object | None:
    INSTANCE_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    fp = INSTANCE_LOCK_FILE.open("a+", encoding="utf-8")
    try:
        msvcrt.locking(fp.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        fp.close()
        return None

    fp.seek(0)
    fp.truncate()
    fp.write(str(os.getpid()))
    fp.flush()
    os.fsync(fp.fileno())
    return fp


def release_single_instance_lock(fp: object | None) -> None:
    if fp is None:
        return
    try:
        fp_obj = fp
        fp_obj.seek(0)
        msvcrt.locking(fp_obj.fileno(), msvcrt.LK_UNLCK, 1)
        fp_obj.close()
    except Exception:
        logging.debug("Failed to release stage34_autorun_simple lock", exc_info=True)


def count_progress() -> tuple[int, int, int]:
    doc_ids: set[str] = set()
    text_ids: set[str] = set()

    if DOCS_CSV.exists() and DOCS_CSV.stat().st_size > 0:
        with DOCS_CSV.open("r", encoding="utf-8", errors="ignore", newline="") as fp:
            for row in csv.DictReader(fp):
                bill_id = (row.get("bill_id") or "").strip()
                if bill_id:
                    doc_ids.add(bill_id)

    if TEXTS_JSONL.exists() and TEXTS_JSONL.stat().st_size > 0:
        with TEXTS_JSONL.open("r", encoding="utf-8", errors="ignore") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                bill_id = str(record.get("bill_id") or "").strip()
                if bill_id:
                    text_ids.add(bill_id)

    total = len(doc_ids)
    done = len(doc_ids & text_ids)
    pending = max(0, total - done)
    return total, done, pending


def stage3_pids() -> list[int]:
    ps_script = (
        "$p = Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -match '^python(\\.exe)?$' -and $_.CommandLine -match 'gd_pipeline\\.cli\\s+stage3' } | "
        "Select-Object -ExpandProperty ProcessId; "
        "if ($null -eq $p) { '' } else { $p | ConvertTo-Json -Compress }"
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps_script],
        check=False,
        capture_output=True,
        text=True,
    )
    payload = (result.stdout or "").strip()
    if not payload:
        return []
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, int):
        return [parsed]
    if isinstance(parsed, list):
        pids: list[int] = []
        for item in parsed:
            try:
                pids.append(int(item))
            except Exception:
                continue
        return pids
    return []


def run_cmd(cmd: list[str], log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as out:
        out.write(f"\n=== RUN: {' '.join(cmd)} ===\n")
        out.flush()
        rc = subprocess.run(cmd, stdout=out, stderr=out, text=True, check=False).returncode
        out.write(f"=== EXIT CODE: {rc} ===\n")
        out.flush()
    return rc


def main() -> int:
    setup_logging()
    lock_fp = acquire_single_instance_lock()
    if lock_fp is None:
        logging.error("Another stage34_autorun_simple instance is already running. Exiting.")
        return 10

    logging.info("stage34_autorun_simple started")

    stage3_cmd = [sys.executable, "-m", "gd_pipeline.cli", "stage3", "--output-dir", ARTIFACTS_DIR.as_posix()]
    stage4_cmd = [sys.executable, "-m", "gd_pipeline.cli", "stage4", "--output-dir", ARTIFACTS_DIR.as_posix()]

    try:
        while True:
            total, done, pending = count_progress()
            pids = stage3_pids()
            logging.info("Progress: done=%s / total=%s, pending=%s, stage3_pids=%s", done, total, pending, pids)

            if total == 0:
                logging.error("documents.csv is missing or empty")
                return 2

            if pending == 0:
                logging.info("Stage 3 data complete. Running Stage 4...")
                rc4 = run_cmd(stage4_cmd, ARTIFACTS_DIR / "logs" / "stage4_autorun.log")
                if rc4 != 0:
                    logging.error("Stage 4 failed with code=%s", rc4)
                    return rc4
                final_xlsx = ARTIFACTS_DIR / "final_result.xlsx"
                if final_xlsx.exists() and final_xlsx.stat().st_size > 0:
                    logging.info("Done. Result file: %s", final_xlsx.as_posix())
                    return 0
                logging.error("Stage 4 finished but final_result.xlsx missing/empty")
                return 3

            if not pids:
                logging.warning("Stage 3 is not running, starting it")
                rc3 = run_cmd(stage3_cmd, ARTIFACTS_DIR / "logs" / "stage3_autorun.log")
                logging.info("Stage 3 exited with code=%s", rc3)

            time.sleep(POLL_SECONDS)
    finally:
        release_single_instance_lock(lock_fp)


if __name__ == "__main__":
    raise SystemExit(main())
