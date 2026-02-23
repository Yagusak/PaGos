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
LOCK_FILE = ARTIFACTS_DIR / "state" / "stage3.lock"
SUPERVISOR_LOCK_FILE = ARTIFACTS_DIR / "state" / "stage34_supervisor.lock"
LOG_DIR = ARTIFACTS_DIR / "logs"
SUPERVISOR_LOG = LOG_DIR / "stage34_supervisor.log"
STAGE3_STD_LOG = LOG_DIR / "stage3_supervised_stdout.log"
STAGE4_STD_LOG = LOG_DIR / "stage4_supervised_stdout.log"
POLL_SECONDS = 60
STAGE3_STALE_SECONDS = 30 * 60
STAGE3_ACTIVITY_LOGS = (
    STAGE3_STD_LOG,
    LOG_DIR / "stage3_autorun.log",
    LOG_DIR / "pipeline.log",
)


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(SUPERVISOR_LOG, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def acquire_single_instance_lock() -> object | None:
    SUPERVISOR_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    fp = SUPERVISOR_LOCK_FILE.open("a+", encoding="utf-8")
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
        logging.debug("Failed to release supervisor lock", exc_info=True)


def count_pending() -> tuple[int, int, int]:
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
    done = len(text_ids & doc_ids)
    pending = max(0, total - done)
    return total, done, pending


def is_stage3_running() -> bool:
    pids = list_stage3_pids()
    if pids:
        return True

    if not LOCK_FILE.exists():
        return False

    fp = LOCK_FILE.open("a+")
    try:
        msvcrt.locking(fp.fileno(), msvcrt.LK_NBLCK, 1)
        msvcrt.locking(fp.fileno(), msvcrt.LK_UNLCK, 1)
        return False
    except OSError:
        return True
    finally:
        fp.close()


def list_stage3_pids() -> list[int]:
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


def kill_stage3_processes() -> None:
    pids = list_stage3_pids()
    if not pids:
        return
    for pid in pids:
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/F", "/T"],
            check=False,
            capture_output=True,
            text=True,
        )
    logging.warning("Killed stalled Stage 3 processes: %s", ",".join(str(x) for x in pids))


def run_stage(command: list[str], stdout_log: Path) -> int:
    stdout_log.parent.mkdir(parents=True, exist_ok=True)
    with stdout_log.open("a", encoding="utf-8") as out:
        out.write(f"\n=== RUN: {' '.join(command)} ===\n")
        out.flush()
        result = subprocess.run(
            command,
            stdout=out,
            stderr=out,
            text=True,
            check=False,
        )
        out.write(f"=== EXIT CODE: {result.returncode} ===\n")
        out.flush()
    return result.returncode


def run_stage3_supervised(command: list[str], stdout_log: Path) -> int:
    stdout_log.parent.mkdir(parents=True, exist_ok=True)
    with stdout_log.open("a", encoding="utf-8") as out:
        out.write(f"\n=== RUN: {' '.join(command)} ===\n")
        out.flush()
        process = subprocess.Popen(
            command,
            stdout=out,
            stderr=out,
            text=True,
        )

        last_done = -1
        last_progress_ts = time.time()
        last_log_mtime = stdout_log.stat().st_mtime if stdout_log.exists() else 0.0
        while True:
            total, done, pending = count_pending()
            now = time.time()
            logging.info("Progress: done=%s / total=%s, pending=%s", done, total, pending)
            if done > last_done:
                last_done = done
                last_progress_ts = now

            current_log_mtime = stdout_log.stat().st_mtime if stdout_log.exists() else 0.0
            if current_log_mtime > last_log_mtime:
                last_log_mtime = current_log_mtime
                last_progress_ts = now

            if process.poll() is not None:
                rc = process.returncode or 0
                out.write(f"=== EXIT CODE: {rc} ===\n")
                out.flush()
                return rc

            stalled_for = now - last_progress_ts
            if stalled_for >= STAGE3_STALE_SECONDS:
                logging.warning(
                    "No progress for %s sec in supervised Stage 3 run. Killing process tree and retrying.",
                    int(stalled_for),
                )
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/F", "/T"],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                out.write("=== EXIT CODE: killed_due_to_stall ===\n")
                out.flush()
                return 99

            time.sleep(POLL_SECONDS)


def latest_stage3_activity_ts() -> float:
    latest_ts = 0.0
    for path in (LOCK_FILE, *STAGE3_ACTIVITY_LOGS):
        if path.exists():
            try:
                latest_ts = max(latest_ts, path.stat().st_mtime)
            except Exception:
                continue
    return latest_ts


def main() -> int:
    setup_logging()
    lock_fp = acquire_single_instance_lock()
    if lock_fp is None:
        logging.error("Another stage34_supervisor instance is already running. Exiting.")
        return 10

    logging.info("Supervisor started")

    stage3_cmd = [sys.executable, "-m", "gd_pipeline.cli", "stage3", "--output-dir", ARTIFACTS_DIR.as_posix()]
    stage4_cmd = [sys.executable, "-m", "gd_pipeline.cli", "stage4", "--output-dir", ARTIFACTS_DIR.as_posix()]

    try:
        stage3_attempt = 0
        last_done = -1
        last_progress_ts = time.time()
        last_stage3_activity_ts = latest_stage3_activity_ts()
        while True:
            now = time.time()
            total, done, pending = count_pending()
            logging.info("Progress: done=%s / total=%s, pending=%s", done, total, pending)
            if done > last_done:
                last_done = done
                last_progress_ts = now

            current_stage3_activity_ts = latest_stage3_activity_ts()
            if current_stage3_activity_ts > last_stage3_activity_ts:
                last_stage3_activity_ts = current_stage3_activity_ts
                last_progress_ts = now

            if total == 0:
                logging.error("documents.csv is empty or missing. Stop.")
                return 2

            if pending == 0:
                break

            if is_stage3_running():
                stalled_for = now - last_progress_ts
                if stalled_for >= STAGE3_STALE_SECONDS:
                    logging.warning(
                        "No stage3 activity for %s sec while Stage 3 is running. Restarting Stage 3...",
                        int(stalled_for),
                    )
                    kill_stage3_processes()
                    if LOCK_FILE.exists():
                        try:
                            LOCK_FILE.unlink()
                        except Exception:
                            logging.warning("Failed to remove stale lock file: %s", LOCK_FILE.as_posix())
                    time.sleep(5)
                    continue
                logging.info("Stage 3 currently running. Waiting 60 seconds...")
                time.sleep(POLL_SECONDS)
                continue

            stage3_attempt += 1
            logging.info("Stage 3 not running. Starting supervised pass #%s", stage3_attempt)
            rc = run_stage3_supervised(stage3_cmd, STAGE3_STD_LOG)
            logging.info("Stage 3 pass #%s finished with code=%s", stage3_attempt, rc)
            if rc != 0:
                logging.warning("Stage 3 failed. Waiting 30 seconds before next check...")
                time.sleep(30)

        logging.info("All texts collected. Starting Stage 4...")
        rc4 = run_stage(stage4_cmd, STAGE4_STD_LOG)
        if rc4 != 0:
            logging.error("Stage 4 failed with code=%s", rc4)
            return rc4

        final_xlsx = ARTIFACTS_DIR / "final_result.xlsx"
        if final_xlsx.exists() and final_xlsx.stat().st_size > 0:
            logging.info("Stage 4 finished successfully: %s", final_xlsx.as_posix())
            return 0

        logging.error("Stage 4 finished but final_result.xlsx is missing/empty")
        return 3
    finally:
        release_single_instance_lock(lock_fp)


if __name__ == "__main__":
    raise SystemExit(main())
