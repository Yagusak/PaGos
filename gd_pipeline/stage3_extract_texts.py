from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import multiprocessing as mp
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import fitz
from docx import Document
from playwright.async_api import BrowserContext, async_playwright

from .config import PipelineConfig
from .io_utils import (
    append_jsonl_record,
    iter_csv_rows,
)


LOGGER = logging.getLogger(__name__)


def extract_text_worker(task: tuple[str, str, str]) -> tuple[str, str, str | None, str]:
    bill_id, side, file_path_str = task
    path = Path(file_path_str)
    try:
        text = extract_text_from_file(path)
        return bill_id, side, text, ""
    except Exception as exc:  # noqa: BLE001 - isolate file-level failures
        return bill_id, side, None, str(exc)


def extract_text_from_file(path: Path) -> str | None:
    if not path.exists() or path.stat().st_size == 0:
        return None

    try:
        with open(path, "rb") as f:
            header = f.read(8)
    except Exception:
        return None

    header_lower = header.lower()
    real_path = path

    if header.startswith(b"%PDF"):
        if path.suffix.lower() != ".pdf":
            real_path = path.with_suffix(".pdf")
            if not real_path.exists():
                path.rename(real_path)
        return _extract_pdf_text(real_path)

    if header.startswith(b"PK\x03\x04"):
        if path.suffix.lower() != ".docx":
            real_path = path.with_suffix(".docx")
            if not real_path.exists():
                path.rename(real_path)
        return _extract_docx_text(real_path)

    if header.startswith(b"\xd0\xcf\x11\xe0"):
        if path.suffix.lower() != ".doc":
            real_path = path.with_suffix(".doc")
            if not real_path.exists():
                path.rename(real_path)
        return _extract_doc_legacy_text(real_path)

    if header.startswith(b"{\\rtf"):
        if path.suffix.lower() != ".rtf":
            real_path = path.with_suffix(".rtf")
            if not real_path.exists():
                path.rename(real_path)
        return _extract_rtf_text(real_path)

    if header_lower.startswith(b"<?xml") or header_lower.startswith(b"<html") or header_lower.startswith(b"<!doc"):
        if path.suffix.lower() != ".doc":
            real_path = path.with_suffix(".doc")
            if not real_path.exists():
                path.rename(real_path)
        return _extract_doc_legacy_text(real_path)

    return None


def _extract_pdf_text(path: Path) -> str | None:
    chunks: list[str] = []
    with fitz.open(path) as doc:
        if doc.needs_pass:
            return None
        for page in doc:
            text = page.get_text("text")
            if text and text.strip():
                chunks.append(text.strip())
    result = "\n".join(chunks).strip()
    return result or None


def _extract_docx_text(path: Path) -> str | None:
    document = Document(str(path))
    chunks = [paragraph.text.strip() for paragraph in document.paragraphs if paragraph.text.strip()]
    result = "\n".join(chunks).strip()
    return result or None


def _extract_doc_legacy_text(path: Path) -> str | None:
    timeout_seconds = 20
    out_fd, out_name = tempfile.mkstemp(prefix="doc_text_", suffix=".txt")
    err_fd, err_name = tempfile.mkstemp(prefix="doc_err_", suffix=".log")
    os.close(out_fd)
    os.close(err_fd)

    out_path = Path(out_name)
    err_path = Path(err_name)

    helper_script = (
        "import sys, traceback\n"
        "from pathlib import Path\n"
        "doc_path = Path(sys.argv[1])\n"
        "out_path = Path(sys.argv[2])\n"
        "err_path = Path(sys.argv[3])\n"
        "word = None\n"
        "doc = None\n"
        "try:\n"
        "    import pythoncom\n"
        "    import win32com.client\n"
        "    pythoncom.CoInitialize()\n"
        "    word = win32com.client.DispatchEx('Word.Application')\n"
        "    word.Visible = False\n"
        "    word.DisplayAlerts = False\n"
        "    doc = word.Documents.Open(\n"
        "        str(doc_path.resolve()),\n"
        "        ReadOnly=True,\n"
        "        AddToRecentFiles=False,\n"
        "        ConfirmConversions=False,\n"
        "        NoEncodingDialog=True,\n"
        "    )\n"
        "    text = (doc.Content.Text or '').strip()\n"
        "    out_path.write_text(text, encoding='utf-8', errors='ignore')\n"
        "except Exception:\n"
        "    err_path.write_text(traceback.format_exc(), encoding='utf-8', errors='ignore')\n"
        "finally:\n"
        "    try:\n"
        "        if doc is not None:\n"
        "            doc.Close(False)\n"
        "    except Exception:\n"
        "        pass\n"
        "    try:\n"
        "        if word is not None:\n"
        "            word.Quit()\n"
        "    except Exception:\n"
        "        pass\n"
        "    try:\n"
        "        import pythoncom\n"
        "        pythoncom.CoUninitialize()\n"
        "    except Exception:\n"
        "        pass\n"
    )

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                helper_script,
                str(path),
                str(out_path),
                str(err_path),
            ],
            check=False,
            timeout=timeout_seconds,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            LOGGER.warning(
                "Legacy DOC helper exited with code=%s for %s",
                result.returncode,
                path.as_posix(),
            )
            return None

        if err_path.exists() and err_path.stat().st_size > 0:
            LOGGER.warning("Legacy DOC helper error for %s", path.as_posix())
            return None

        if not out_path.exists() or out_path.stat().st_size == 0:
            return None

        text = out_path.read_text(encoding="utf-8", errors="ignore").strip()
        return text or None
    except subprocess.TimeoutExpired:
        LOGGER.warning("Legacy DOC extraction timeout (%ss): %s", timeout_seconds, path.as_posix())
        return None
    except Exception:
        LOGGER.exception("Failed to extract legacy DOC text via helper: %s", path.as_posix())
        return None
    finally:
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            LOGGER.debug("Failed to cleanup legacy DOC out file: %s", out_path.as_posix(), exc_info=True)
        try:
            err_path.unlink(missing_ok=True)
        except Exception:
            LOGGER.debug("Failed to cleanup legacy DOC err file: %s", err_path.as_posix(), exc_info=True)


def _extract_rtf_text(path: Path) -> str | None:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    text = re.sub(r"\\[a-zA-Z]+\d* ?", " ", raw)
    text = re.sub(r"[{}]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


class Stage3TextExtractor:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self._warm_lock = asyncio.Lock()
        self._warmed_bill_urls: set[str] = set()
        self._run_lock_path = self.config.state_dir / "stage3.lock"
        self._run_lock_acquired = False
        self._run_lock_fp = None

    async def run(self) -> None:
        self.config.ensure_directories()
        if not self._acquire_run_lock():
            LOGGER.error(
                "Stage 3 is already running in another process. Lock file: %s",
                self._run_lock_path.as_posix(),
            )
            return

        try:
            self._ensure_texts_jsonl_exists()
            self._normalize_existing_texts_jsonl()

            documents_rows = [self._normalize_documents_row(row) for row in iter_csv_rows(self.config.documents_csv)]
            if not documents_rows:
                LOGGER.warning(
                    "No rows in %s. Run stage2 first.",
                    self.config.documents_csv.as_posix(),
                )
                return

            existing_records = self._load_existing_text_records()
            pending_rows = [
                row
                for row in documents_rows
                if row["bill_id"] and self._needs_processing(row=row, existing_record=existing_records.get(row["bill_id"]))
            ]

            if not pending_rows:
                LOGGER.info("Stage 3 skipped: all rows already present in texts.jsonl")
                return

            LOGGER.info(
                "Stage 3 started. Pending bills=%s, already_done=%s",
                len(pending_rows),
                len(existing_records),
            )
            merged_rows: dict[str, dict[str, str]] = {}
            for row in pending_rows:
                bill_id = row["bill_id"]
                existing = merged_rows.get(bill_id)
                if not existing:
                    merged_rows[bill_id] = row
                    continue
                if not existing["input_doc_url"] and row["input_doc_url"]:
                    existing["input_doc_url"] = row["input_doc_url"]
                if not existing["output_doc_url"] and row["output_doc_url"]:
                    existing["output_doc_url"] = row["output_doc_url"]

            records: dict[str, dict[str, Any]] = {}
            download_jobs: list[tuple[str, str, str, str]] = []

            for row in merged_rows.values():
                bill_id = row["bill_id"]
                existing = existing_records.get(bill_id, {})
                records[bill_id] = {
                    "bill_id": bill_id,
                    "text_a": self._sanitize_text(existing.get("text_a")),
                    "text_b": self._sanitize_text(existing.get("text_b")),
                    "pending_sides": set(),
                    "written": False,
                    "download_errors": [],
                    "extract_errors": [],
                }

                if row["input_doc_url"] and not self._has_text(records[bill_id]["text_a"]):
                    download_jobs.append((bill_id, "a", row["input_doc_url"], row.get("bill_url", "")))
                if row["output_doc_url"] and not self._has_text(records[bill_id]["text_b"]):
                    download_jobs.append((bill_id, "b", row["output_doc_url"], row.get("bill_url", "")))

            download_results = await self._download_documents(download_jobs)
            for bill_id, side, file_path, error_text in download_results:
                record = records[bill_id]
                if file_path:
                    record[f"path_{side}"] = file_path
                    record["pending_sides"].add(side)
                else:
                    record["download_errors"].append(f"{side}: {error_text}")
                    LOGGER.warning("Download failed for bill=%s side=%s: %s", bill_id, side, error_text)

            for record in records.values():
                if not record["pending_sides"] and not record["written"]:
                    self._write_text_record(record)
                    record["written"] = True

            extraction_tasks: list[tuple[str, str, str]] = []
            for bill_id, record in records.items():
                for side in ("a", "b"):
                    file_path = record.get(f"path_{side}")
                    if file_path:
                        extraction_tasks.append((bill_id, side, file_path))

            if extraction_tasks:
                LOGGER.info(
                    "Stage 3 extraction started. Files queued=%s, cpu=%s",
                    len(extraction_tasks),
                    mp.cpu_count(),
                )
                with mp.Pool(processes=mp.cpu_count()) as pool:
                    for bill_id, side, text, error_text in pool.imap_unordered(
                        extract_text_worker,
                        extraction_tasks,
                        chunksize=8,
                    ):
                        record = records[bill_id]
                        record[f"text_{side}"] = text
                        record["pending_sides"].discard(side)

                        if error_text:
                            record["extract_errors"].append(f"{side}: {error_text}")
                            LOGGER.warning(
                                "Extraction failed for bill=%s side=%s: %s",
                                bill_id,
                                side,
                                error_text,
                            )

                        if not record["pending_sides"] and not record["written"]:
                            self._write_text_record(record)
                            record["written"] = True

            for record in records.values():
                if not record["written"]:
                    self._write_text_record(record)
                    record["written"] = True

            self._normalize_existing_texts_jsonl()
            LOGGER.info("Stage 3 finished. Appended/updated records=%s", len(merged_rows))
        finally:
            self._release_run_lock()

    async def _download_documents(
        self,
        jobs: list[tuple[str, str, str, str]],
    ) -> list[tuple[str, str, str | None, str]]:
        if not jobs:
            return []

        queue: asyncio.Queue[tuple[str, str, str, str] | None] = asyncio.Queue()
        for job in jobs:
            queue.put_nowait(job)

        workers_count = max(1, int(self.config.stage3_download_concurrency))
        results: list[tuple[str, str, str | None, str]] = []
        results_lock = asyncio.Lock()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(accept_downloads=False)
            try:
                workers = [
                    asyncio.create_task(
                        self._download_worker(
                            worker_id=i + 1,
                            queue=queue,
                            context=context,
                            results=results,
                            results_lock=results_lock,
                        )
                    )
                    for i in range(workers_count)
                ]

                await queue.join()
                for _ in workers:
                    queue.put_nowait(None)
                await asyncio.gather(*workers)
                return results
            finally:
                await context.close()
                await browser.close()

    async def _download_worker(
        self,
        worker_id: int,
        queue: asyncio.Queue[tuple[str, str, str, str] | None],
        context: BrowserContext,
        results: list[tuple[str, str, str | None, str]],
        results_lock: asyncio.Lock,
    ) -> None:
        processed = 0
        while True:
            job = await queue.get()
            if job is None:
                queue.task_done()
                return

            bill_id, side, url, bill_url = job
            try:
                result = await self._download_with_retry(
                    context=context,
                    bill_id=bill_id,
                    side=side,
                    url=url,
                    bill_url=bill_url,
                )
                async with results_lock:
                    results.append(result)
                processed += 1
                if processed % 100 == 0:
                    LOGGER.info("Stage3 worker %s processed %s files", worker_id, processed)
            except Exception as exc:  # noqa: BLE001 - worker fault isolation
                LOGGER.exception("Stage3 worker %s failed on bill=%s side=%s", worker_id, bill_id, side)
                async with results_lock:
                    results.append((bill_id, side, None, str(exc)))
            finally:
                queue.task_done()

    async def _download_with_retry(
        self,
        context: BrowserContext,
        bill_id: str,
        side: str,
        url: str,
        bill_url: str,
    ) -> tuple[str, str, str | None, str]:
        normalized_url = self._normalize_doc_url(url)
        if not normalized_url:
            return bill_id, side, None, "empty_url"

        destination = self._build_download_path(bill_id=bill_id, side=side, url=normalized_url)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists() and destination.stat().st_size > 0:
            if self._has_known_file_signature(destination):
                return bill_id, side, str(destination), ""
            destination.unlink(missing_ok=True)

        normalized_bill_url = self._normalize_doc_url(bill_url)
        if normalized_bill_url:
            await self._ensure_bill_warmed(context=context, normalized_bill_url=normalized_bill_url)

        attempts = max(1, self.config.stage3_http_retries)
        timeout_ms = max(30_000, int(self.config.stage3_http_timeout_seconds * 1000))

        for attempt in range(1, attempts + 1):
            temp_path = destination.with_suffix(
                f"{destination.suffix}.{os.getpid()}.{attempt}.part"
            )
            try:
                api_response = await context.request.get(
                    normalized_url,
                    headers={"Referer": normalized_bill_url or self.config.base_url},
                    timeout=timeout_ms,
                )
                if not api_response.ok:
                    status = api_response.status
                    await api_response.dispose()
                    raise ValueError(f"HTTP {status} while downloading document")

                body = await api_response.body()
                await api_response.dispose()

                header_bytes = body[:100].lstrip().lower()
                full_head_sample = body[:4096].lower()
                if header_bytes.startswith(b"<html") or header_bytes.startswith(b"<!doc"):
                    if b"schemas-microsoft-com:office" not in full_head_sample and b"xmlns:w=" not in full_head_sample:
                        raise ValueError("Received HTML error page instead of document")

                with temp_path.open("wb") as fp:
                    fp.write(body)

                if not temp_path.exists() or temp_path.stat().st_size == 0:
                    raise ValueError("Downloaded zero-byte file")

                replace_succeeded = False
                for replace_attempt in range(1, 4):
                    try:
                        temp_path.replace(destination)
                        replace_succeeded = True
                        break
                    except PermissionError:
                        # Another process may have already produced the same destination file.
                        if destination.exists() and self._has_known_file_signature(destination):
                            self._safe_unlink(temp_path)
                            return bill_id, side, str(destination), ""
                        if replace_attempt >= 3:
                            raise
                        await asyncio.sleep(0.25 * replace_attempt)

                if not replace_succeeded:
                    raise ValueError("Failed to finalize downloaded file")

                if not self._has_known_file_signature(destination):
                    self._safe_unlink(destination)
                    raise ValueError("Downloaded file has unknown signature")

                return bill_id, side, str(destination), ""
            except Exception as exc:  # noqa: BLE001 - isolate per-file download errors
                self._safe_unlink(temp_path)
                if attempt >= attempts:
                    return bill_id, side, None, str(exc)
                await asyncio.sleep(min(10, 2**attempt))

        return bill_id, side, None, "unknown_download_error"

    async def _ensure_bill_warmed(self, context: BrowserContext, normalized_bill_url: str) -> None:
        if normalized_bill_url in self._warmed_bill_urls:
            return

        async with self._warm_lock:
            if normalized_bill_url in self._warmed_bill_urls:
                return

            page = await context.new_page()
            try:
                await page.goto(
                    normalized_bill_url,
                    wait_until="domcontentloaded",
                    timeout=60_000,
                )
                self._warmed_bill_urls.add(normalized_bill_url)
            except Exception:
                LOGGER.debug("Failed to warm bill URL: %s", normalized_bill_url, exc_info=True)
            finally:
                await page.close()

    def _normalize_documents_row(self, row: dict[str, str]) -> dict[str, str]:
        bill_id = (row.get("bill_id") or "").strip()
        bill_url = (row.get("bill_url") or row.get("url") or "").strip()
        input_doc_url = (row.get("input_doc_url") or row.get("initial_doc_url") or "").strip()
        output_doc_url = (row.get("output_doc_url") or row.get("final_doc_url") or "").strip()
        return {
            "bill_id": bill_id,
            "bill_url": bill_url,
            "input_doc_url": "" if input_doc_url.upper().startswith("ERROR") else input_doc_url,
            "output_doc_url": "" if output_doc_url.upper().startswith("ERROR") else output_doc_url,
        }

    def _normalize_doc_url(self, raw_url: str) -> str:
        url = (raw_url or "").strip()
        if not url:
            return ""
        if url.upper().startswith("ERROR"):
            return ""
        return urljoin(self.config.base_url, url)

    def _build_download_path(self, bill_id: str, side: str, url: str) -> Path:
        extension = self._infer_extension_from_url(url)
        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
        safe_bill = re.sub(r"[^0-9A-Za-z_-]", "_", bill_id)
        filename = f"{safe_bill}_{side}_{digest}{extension}"
        return self.config.temp_download_dir / filename

    @staticmethod
    def _has_known_file_signature(path: Path) -> bool:
        try:
            with path.open("rb") as fp:
                header = fp.read(8)
        except Exception:
            return False

        header_lower = header.lower()
        return (
            header.startswith(b"%PDF")
            or header.startswith(b"PK\x03\x04")
            or header.startswith(b"\xd0\xcf\x11\xe0")
            or header.startswith(b"{\\rtf")
            or header_lower.startswith(b"<?xml")
            or header_lower.startswith(b"<html")
            or header_lower.startswith(b"<!doc")
        )

    @staticmethod
    def _infer_extension_from_url(url: str) -> str:
        path = urlsplit(url).path.lower()
        for ext in (".pdf", ".docx", ".doc", ".rtf", ".txt"):
            if path.endswith(ext):
                return ext
        return ".bin"

    def _write_text_record(self, record: dict[str, Any]) -> None:
        append_jsonl_record(
            self.config.texts_jsonl,
            {
                "bill_id": record["bill_id"],
                "text_a": self._sanitize_text(record.get("text_a")),
                "text_b": self._sanitize_text(record.get("text_b")),
            },
        )

    def _ensure_texts_jsonl_exists(self) -> None:
        if self.config.texts_jsonl.exists():
            return
        self.config.texts_jsonl.parent.mkdir(parents=True, exist_ok=True)
        self.config.texts_jsonl.touch()
        LOGGER.info("Created placeholder JSONL file: %s", self.config.texts_jsonl.as_posix())

    def _normalize_existing_texts_jsonl(self) -> None:
        path = self.config.texts_jsonl
        if not path.exists() or path.stat().st_size == 0:
            return

        cleaned_by_bill: dict[str, dict[str, Any]] = {}
        malformed = 0
        valid_rows = 0
        normalized_values = 0

        with path.open("r", encoding="utf-8", errors="ignore") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
                    continue

                bill_id = str(record.get("bill_id") or "").strip()
                if not bill_id:
                    continue
                valid_rows += 1
                text_a = self._sanitize_text(record.get("text_a"))
                text_b = self._sanitize_text(record.get("text_b"))
                if text_a != record.get("text_a") or text_b != record.get("text_b"):
                    normalized_values += 1

                cleaned_by_bill[bill_id] = {
                    "bill_id": bill_id,
                    "text_a": text_a,
                    "text_b": text_b,
                }

        duplicates = max(0, valid_rows - len(cleaned_by_bill))
        if malformed == 0 and duplicates == 0 and normalized_values == 0:
            return

        fd, tmp_name = tempfile.mkstemp(
            prefix="texts_clean_",
            suffix=".jsonl.tmp",
            dir=str(path.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as tmp_fp:
                for record in cleaned_by_bill.values():
                    tmp_fp.write(json.dumps(record, ensure_ascii=False) + "\n")
                tmp_fp.flush()
                os.fsync(tmp_fp.fileno())
            os.replace(tmp_name, path)
        finally:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)

        LOGGER.warning(
            "Normalized %s: removed malformed=%s, duplicates=%s, normalized_values=%s, remaining=%s",
            path.as_posix(),
            malformed,
            duplicates,
            normalized_values,
            len(cleaned_by_bill),
        )

    def _acquire_run_lock(self) -> bool:
        self._run_lock_path.parent.mkdir(parents=True, exist_ok=True)
        fp = self._run_lock_path.open("a+", encoding="utf-8")
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fp.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            fp.seek(0)
            fp.truncate()
            fp.write(str(os.getpid()))
            fp.flush()
            os.fsync(fp.fileno())

            self._run_lock_fp = fp
            self._run_lock_acquired = True
            return True
        except Exception:
            fp.close()
            return False

    def _release_run_lock(self) -> None:
        if not self._run_lock_acquired:
            return
        try:
            if self._run_lock_fp is not None:
                if os.name == "nt":
                    import msvcrt

                    self._run_lock_fp.seek(0)
                    msvcrt.locking(self._run_lock_fp.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self._run_lock_fp.fileno(), fcntl.LOCK_UN)
                self._run_lock_fp.close()
        except Exception:
            LOGGER.debug("Failed to release stage3 lock cleanly", exc_info=True)
        self._run_lock_acquired = False
        self._run_lock_fp = None

    @staticmethod
    def _safe_unlink(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except PermissionError:
            LOGGER.debug("safe_unlink permission denied for %s", path.as_posix(), exc_info=True)
        except Exception:
            LOGGER.debug("safe_unlink failed for %s", path.as_posix(), exc_info=True)

    @staticmethod
    def _sanitize_text(value: Any) -> str:
        if isinstance(value, str):
            return value
        return ""

    @staticmethod
    def _has_text(value: Any) -> bool:
        return isinstance(value, str) and bool(value.strip())

    def _load_existing_text_records(self) -> dict[str, dict[str, Any]]:
        path = self.config.texts_jsonl
        if not path.exists() or path.stat().st_size == 0:
            return {}

        records: dict[str, dict[str, Any]] = {}
        with path.open("r", encoding="utf-8", errors="ignore") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                bill_id = str(record.get("bill_id") or "").strip()
                if not bill_id:
                    continue
                records[bill_id] = {
                    "bill_id": bill_id,
                    "text_a": self._sanitize_text(record.get("text_a")),
                    "text_b": self._sanitize_text(record.get("text_b")),
                }
        return records

    def _needs_processing(self, row: dict[str, str], existing_record: dict[str, Any] | None) -> bool:
        if existing_record is None:
            return True

        has_text_a = self._has_text(existing_record.get("text_a"))
        has_text_b = self._has_text(existing_record.get("text_b"))
        has_input_url = bool(row.get("input_doc_url"))
        has_output_url = bool(row.get("output_doc_url"))

        need_a = has_input_url and not has_text_a
        need_b = has_output_url and not has_text_b
        return need_a or need_b
