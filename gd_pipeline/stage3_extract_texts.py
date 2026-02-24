from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import multiprocessing as mp
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import fitz
import pytesseract
from docx import Document
from pdf2image import convert_from_path
from PIL import Image, UnidentifiedImageError
from playwright.async_api import BrowserContext, async_playwright
from striprtf.striprtf import rtf_to_text

from .config import PipelineConfig
from .io_utils import (
    append_jsonl_record,
    iter_csv_rows,
    jsonl_file_lock,
)
from .text_quality import garbage_reason_summary, is_probably_garbage_text


LOGGER = logging.getLogger(__name__)
ARCHIVE_OR_ERROR_MARKER = "[\u0410\u0420\u0425\u0418\u0412_\u0418\u041b\u0418_\u041e\u0428\u0418\u0411\u041a\u0410]"
TIMEOUT_WORD_MARKER = "[TIMEOUT_WORD]"
EMPTY_TEXT_LAYER_MARKER = "[EMPTY_TEXT_LAYER]"
POPPLER_MISSING_MARKER = "[POPPLER_MISSING]"
OCR_FAILED_MARKER = "[OCR_FAILED]"
OCR_MIN_TEXT_CHARS = 100
LAST_RESORT_TEXT_MIN_CHARS = 20
RAW_EXTRACT_MAX_CHARS = 120_000
DEFAULT_TESSERACT_PATH = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
DEFAULT_POPPLER_BIN = Path(r"C:\poppler\Library\bin")
DEFAULT_POPPLER_PROGRAM_FILES_BIN = Path(r"C:\Program Files\poppler\bin")
OCR_LANGUAGE = os.getenv("TESSERACT_LANG", "rus+eng")
OCR_TESSDATA_DIR = Path(os.getenv("TESSDATA_DIR", str(Path.home() / "tessdata")))
WINGET_PACKAGES_DIR = Path.home() / "AppData" / "Local" / "Microsoft" / "WinGet" / "Packages"
_TESSERACT_CONFIGURED = False
_TESSERACT_UNAVAILABLE_LOGGED = False
_POPPLER_DISCOVERED = False
_POPPLER_BIN: Path | None = None
_POPPLER_UNAVAILABLE_LOGGED = False
_OCR_FALLBACK_LANG_LOGGED = False


class ExtractionFailure(RuntimeError):
    def __init__(self, marker: str, detail: str = "") -> None:
        super().__init__(detail or marker)
        self.marker = marker
        self.detail = detail


def extract_text_worker(task: tuple[str, str, str]) -> tuple[str, str, str | None, str, str]:
    bill_id, side, file_path_str = task
    path = Path(file_path_str)
    try:
        text = extract_text_from_file(path, bill_id=bill_id)
        return bill_id, side, text, "", ""
    except ExtractionFailure as exc:
        last_resort = extract_text_last_resort(path)
        if last_resort:
            return bill_id, side, last_resort, f"{exc.detail};fallback=last_resort", ""
        return bill_id, side, None, exc.detail, exc.marker
    except Exception as exc:  # noqa: BLE001 - isolate file-level failures
        last_resort = extract_text_last_resort(path)
        if last_resort:
            return bill_id, side, last_resort, f"{exc};fallback=last_resort", ""
        return bill_id, side, None, str(exc), OCR_FAILED_MARKER


def extract_text_from_file(path: Path, bill_id: str | None = None) -> str:
    if not path.exists() or path.stat().st_size == 0:
        raise ExtractionFailure(EMPTY_TEXT_LAYER_MARKER, "file_missing_or_empty")

    try:
        with open(path, "rb") as f:
            header = f.read(8)
    except Exception:
        raise ExtractionFailure(OCR_FAILED_MARKER, "file_header_read_failed")

    header_lower = header.lower()
    real_path = path

    if header.startswith(b"%PDF"):
        if path.suffix.lower() != ".pdf":
            real_path = path.with_suffix(".pdf")
            if not real_path.exists():
                path.rename(real_path)
        parsed_text = _extract_pdf_text(real_path)
        if _is_text_meaningful(parsed_text):
            if is_probably_garbage_text(parsed_text):
                LOGGER.warning(
                    "Garbage-like PDF text detected for bill_id=%s path=%s; forcing OCR fallback (%s)",
                    bill_id or "unknown",
                    real_path.as_posix(),
                    garbage_reason_summary(parsed_text),
                )
            else:
                return parsed_text
        ocr_text, fail_marker = _run_ocr_for_pdf(real_path, bill_id=bill_id)
        if ocr_text:
            if is_probably_garbage_text(ocr_text):
                LOGGER.warning(
                    "Garbage-like OCR text detected for bill_id=%s path=%s; rejecting OCR result (%s)",
                    bill_id or "unknown",
                    real_path.as_posix(),
                    garbage_reason_summary(ocr_text),
                )
            else:
                return ocr_text
        raise ExtractionFailure(fail_marker or EMPTY_TEXT_LAYER_MARKER, "pdf_no_text_after_ocr")

    if header.startswith(b"PK\x03\x04"):
        if not _is_docx_package(path):
            raise ExtractionFailure(ARCHIVE_OR_ERROR_MARKER, "zip_payload_not_docx")
        if path.suffix.lower() != ".docx":
            real_path = path.with_suffix(".docx")
            if not real_path.exists():
                path.rename(real_path)
        parsed_text = _extract_docx_text(real_path)
        if _is_text_meaningful(parsed_text):
            if is_probably_garbage_text(parsed_text):
                LOGGER.warning(
                    "Garbage-like DOCX text detected for bill_id=%s path=%s; forcing image OCR fallback (%s)",
                    bill_id or "unknown",
                    real_path.as_posix(),
                    garbage_reason_summary(parsed_text),
                )
            else:
                return parsed_text
        ocr_text, fail_marker = _run_ocr_for_docx_images(real_path, bill_id=bill_id)
        if ocr_text:
            if is_probably_garbage_text(ocr_text):
                LOGGER.warning(
                    "Garbage-like DOCX image OCR text detected for bill_id=%s path=%s; rejecting OCR result (%s)",
                    bill_id or "unknown",
                    real_path.as_posix(),
                    garbage_reason_summary(ocr_text),
                )
            else:
                return ocr_text
        raise ExtractionFailure(fail_marker or EMPTY_TEXT_LAYER_MARKER, "docx_no_text_after_ocr")

    if header.startswith(b"\xd0\xcf\x11\xe0"):
        if path.suffix.lower() != ".doc":
            real_path = path.with_suffix(".doc")
            if not real_path.exists():
                path.rename(real_path)
        doc_text, fail_marker = _extract_doc_legacy_text(real_path, bill_id=bill_id)
        if doc_text:
            return doc_text
        raise ExtractionFailure(fail_marker or EMPTY_TEXT_LAYER_MARKER, "legacy_doc_no_text")

    if header.startswith(b"{\\rtf"):
        if path.suffix.lower() != ".rtf":
            real_path = path.with_suffix(".rtf")
            if not real_path.exists():
                path.rename(real_path)
        raw_rtf = ""
        try:
            raw_rtf = real_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            raw_rtf = ""
        clean_text = _try_striprtf_cleanup(
            text=raw_rtf,
            bill_id=bill_id,
            path=real_path,
            source="rtf_branch",
            min_chars=LAST_RESORT_TEXT_MIN_CHARS,
        )
        if clean_text:
            return clean_text
        raw_text = extract_text_last_resort(real_path)
        if raw_text:
            return raw_text
        raise ExtractionFailure(EMPTY_TEXT_LAYER_MARKER, "rtf_extract_failed")

    if header_lower.startswith(b"<?xml") or header_lower.startswith(b"<html") or header_lower.startswith(b"<!doc"):
        if path.suffix.lower() != ".doc":
            real_path = path.with_suffix(".doc")
            if not real_path.exists():
                path.rename(real_path)
        doc_text, fail_marker = _extract_doc_legacy_text(real_path, bill_id=bill_id)
        if doc_text:
            return doc_text
        raise ExtractionFailure(fail_marker or EMPTY_TEXT_LAYER_MARKER, "xml_html_as_legacy_doc_no_text")

    if _is_image_signature(header):
        ocr_text, fail_marker = _run_ocr_for_image_file(real_path, bill_id=bill_id)
        if ocr_text:
            return ocr_text
        raise ExtractionFailure(fail_marker or OCR_FAILED_MARKER, "image_no_text_after_ocr")

    raise ExtractionFailure(EMPTY_TEXT_LAYER_MARKER, "unknown_signature_or_empty_payload")


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


def _is_docx_package(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path) as zf:
            names = set(zf.namelist())
            has_content_types = "[Content_Types].xml" in names
            has_word_payload = any(name.startswith("word/") for name in names)
            return has_content_types and has_word_payload
    except Exception:
        return False


def _is_text_meaningful(text: str | None, min_chars: int = OCR_MIN_TEXT_CHARS) -> bool:
    if not text:
        return False
    normalized = " ".join(text.split())
    return len(normalized) >= min_chars


def _try_striprtf_cleanup(
    text: str,
    bill_id: str | None,
    path: Path,
    source: str,
    min_chars: int = 80,
) -> str | None:
    try:
        cleaned = rtf_to_text(text)
    except Exception as exc:
        LOGGER.warning(
            "striprtf failed for bill_id=%s source=%s path=%s: %s",
            bill_id or "unknown",
            source,
            path.as_posix(),
            exc,
        )
        return None

    cleaned = (cleaned or "").strip()
    normalized = " ".join(cleaned.split())
    if len(normalized) < min_chars:
        return None
    if is_probably_garbage_text(cleaned):
        LOGGER.warning(
            "striprtf output still garbage for bill_id=%s source=%s path=%s (%s)",
            bill_id or "unknown",
            source,
            path.as_posix(),
            garbage_reason_summary(cleaned),
        )
        return None
    LOGGER.info(
        "striprtf recovered clean text for bill_id=%s source=%s path=%s",
        bill_id or "unknown",
        source,
        path.as_posix(),
    )
    return cleaned


def extract_text_last_resort(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        data = path.read_bytes()
    except Exception:
        return ""
    if not data:
        return ""

    decoded_candidates: list[str] = []
    for enc in ("utf-8", "cp1251", "latin-1"):
        try:
            decoded = data.decode(enc, errors="ignore")
        except Exception:
            continue
        if decoded:
            decoded_candidates.append(decoded)

    best = ""
    best_score = -1
    text_re = re.compile(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё0-9\s,.;:()\"'«»№%/\-]{2,}")
    for decoded in decoded_candidates:
        normalized = decoded.replace("\x00", " ")
        parts = [re.sub(r"\s+", " ", part).strip() for part in text_re.findall(normalized)]
        parts = [part for part in parts if len(part) >= 3 and any(ch.isalpha() for ch in part)]
        candidate = "\n".join(parts)
        score = len(candidate)
        if score > best_score:
            best = candidate
            best_score = score

    if not best and decoded_candidates:
        fallback = decoded_candidates[0].replace("\x00", " ")
        fallback = re.sub(r"\s+", " ", fallback).strip()
        best = fallback

    best = best[:RAW_EXTRACT_MAX_CHARS].strip()
    if len(best) < LAST_RESORT_TEXT_MIN_CHARS:
        return ""
    return best


def _is_image_signature(header: bytes) -> bool:
    return (
        header.startswith(b"\x89PNG\r\n\x1a\n")
        or header.startswith(b"\xff\xd8\xff")
        or header.startswith((b"II*\x00", b"MM\x00*"))
        or header.startswith(b"BM")
        or header.startswith((b"GIF87a", b"GIF89a"))
    )


def _ensure_tesseract_configured() -> bool:
    global _TESSERACT_CONFIGURED, _TESSERACT_UNAVAILABLE_LOGGED

    if _TESSERACT_CONFIGURED:
        return True

    env_cmd = (os.getenv("TESSERACT_CMD") or "").strip()
    candidates: list[Path] = []
    if env_cmd:
        candidates.append(Path(env_cmd))
    candidates.append(DEFAULT_TESSERACT_PATH)

    for candidate in candidates:
        if candidate.exists():
            pytesseract.pytesseract.tesseract_cmd = str(candidate)
            _TESSERACT_CONFIGURED = True
            LOGGER.info("Configured tesseract_cmd=%s", candidate.as_posix())
            return True

    try:
        subprocess.run(
            ["tesseract", "--version"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        pytesseract.pytesseract.tesseract_cmd = "tesseract"
        _TESSERACT_CONFIGURED = True
        LOGGER.info("Configured tesseract_cmd from PATH")
        return True
    except Exception:
        if not _TESSERACT_UNAVAILABLE_LOGGED:
            LOGGER.warning(
                "Tesseract executable not found. Set TESSERACT_CMD or install to %s",
                DEFAULT_TESSERACT_PATH.as_posix(),
            )
            _TESSERACT_UNAVAILABLE_LOGGED = True
        return False


def _run_ocr_for_pdf(path: Path, bill_id: str | None = None) -> tuple[str | None, str | None]:
    if not _ensure_tesseract_configured():
        return None, OCR_FAILED_MARKER

    bill_tag = bill_id or "unknown"
    LOGGER.info("Running OCR for bill_id=%s source=pdf path=%s", bill_tag, path.as_posix())

    images: list[Image.Image] = []
    poppler_bin = _resolve_poppler_bin()
    poppler_missing = poppler_bin is None
    if poppler_bin is not None:
        try:
            kwargs: dict[str, Any] = {"dpi": 300, "poppler_path": str(poppler_bin)}
            images = convert_from_path(str(path), **kwargs)
        except Exception as exc:
            LOGGER.warning(
                "convert_from_path failed for bill_id=%s path=%s: %s; fallback=fitz",
                bill_tag,
                path.as_posix(),
                exc,
            )
    else:
        LOGGER.warning(
            "Poppler missing for bill_id=%s path=%s. OCR fallback uses fitz renderer.",
            bill_tag,
            path.as_posix(),
        )

    if not images:
        try:
            images = _render_pdf_pages_with_fitz(path)
        except Exception:
            LOGGER.warning("fitz rendering failed for bill_id=%s path=%s", bill_tag, path.as_posix(), exc_info=True)
            return None, POPPLER_MISSING_MARKER if poppler_missing else OCR_FAILED_MARKER

    texts: list[str] = []
    had_ocr_errors = False
    for img in images:
        text, had_error = _ocr_image(img, bill_id=bill_id)
        had_ocr_errors = had_ocr_errors or had_error
        if text:
            texts.append(text)
        try:
            img.close()
        except Exception:
            pass

    result = "\n".join(texts).strip()
    if result:
        return result, None
    if had_ocr_errors:
        return None, OCR_FAILED_MARKER
    if poppler_missing:
        return None, POPPLER_MISSING_MARKER
    return None, EMPTY_TEXT_LAYER_MARKER


def _render_pdf_pages_with_fitz(path: Path) -> list[Image.Image]:
    result: list[Image.Image] = []
    with fitz.open(path) as doc:
        if doc.needs_pass:
            return result
        for page in doc:
            pix = page.get_pixmap(matrix=fitz.Matrix(2.0, 2.0), alpha=False)
            image = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            result.append(image)
    return result


def _resolve_poppler_bin() -> Path | None:
    global _POPPLER_DISCOVERED, _POPPLER_BIN, _POPPLER_UNAVAILABLE_LOGGED

    if _POPPLER_DISCOVERED:
        return _POPPLER_BIN

    _POPPLER_DISCOVERED = True
    candidates: list[Path] = []

    env_poppler = (os.getenv("POPPLER_BIN") or "").strip()
    if env_poppler:
        candidates.append(Path(env_poppler))

    candidates.append(DEFAULT_POPPLER_BIN)
    candidates.append(DEFAULT_POPPLER_PROGRAM_FILES_BIN)

    if WINGET_PACKAGES_DIR.exists():
        for base in sorted(WINGET_PACKAGES_DIR.glob("oschwartz10612.Poppler_*"), reverse=True):
            candidates.extend(sorted(base.glob("poppler-*/Library/bin"), reverse=True))

    pdftoppm_from_path = shutil.which("pdftoppm")
    if pdftoppm_from_path:
        candidates.append(Path(pdftoppm_from_path).parent)

    for candidate in candidates:
        bin_dir = _normalize_poppler_candidate(candidate)
        if bin_dir is None:
            continue
        _POPPLER_BIN = bin_dir
        LOGGER.info("Configured poppler_bin=%s", bin_dir.as_posix())
        return _POPPLER_BIN

    if not _POPPLER_UNAVAILABLE_LOGGED:
        LOGGER.warning(
            "Poppler bin directory not found. Checked %s and %s. Set POPPLER_BIN or install Poppler.",
            DEFAULT_POPPLER_BIN.as_posix(),
            DEFAULT_POPPLER_PROGRAM_FILES_BIN.as_posix(),
        )
        _POPPLER_UNAVAILABLE_LOGGED = True
    return None


def _normalize_poppler_candidate(candidate: Path) -> Path | None:
    path = candidate
    if path.is_file():
        path = path.parent
    if not path.exists() or not path.is_dir():
        return None
    required = path / "pdftoppm.exe"
    if required.exists():
        return path
    return None


def _run_ocr_for_docx_images(path: Path, bill_id: str | None = None) -> tuple[str | None, str | None]:
    if not _ensure_tesseract_configured():
        return None, OCR_FAILED_MARKER

    bill_tag = bill_id or "unknown"
    LOGGER.info("Running OCR for bill_id=%s source=docx_images path=%s", bill_tag, path.as_posix())
    texts: list[str] = []
    had_ocr_errors = False
    images_seen = 0
    try:
        with zipfile.ZipFile(path) as zf:
            image_names = [name for name in zf.namelist() if name.startswith("word/media/")]
            for name in image_names:
                images_seen += 1
                try:
                    data = zf.read(name)
                    with Image.open(io.BytesIO(data)) as img:
                        text, had_error = _ocr_image(img, bill_id=bill_id)
                        had_ocr_errors = had_ocr_errors or had_error
                        if text:
                            texts.append(text)
                except UnidentifiedImageError:
                    continue
                except Exception:
                    had_ocr_errors = True
                    LOGGER.debug(
                        "Failed OCR for embedded image bill_id=%s path=%s image=%s",
                        bill_tag,
                        path.as_posix(),
                        name,
                        exc_info=True,
                    )
    except Exception:
        LOGGER.debug("Failed to scan docx images for OCR: %s", path.as_posix(), exc_info=True)
        return None, OCR_FAILED_MARKER

    result = "\n".join(texts).strip()
    if result:
        return result, None
    if had_ocr_errors:
        return None, OCR_FAILED_MARKER
    if images_seen == 0:
        return None, EMPTY_TEXT_LAYER_MARKER
    return None, OCR_FAILED_MARKER


def _run_ocr_for_image_file(path: Path, bill_id: str | None = None) -> tuple[str | None, str | None]:
    if not _ensure_tesseract_configured():
        return None, OCR_FAILED_MARKER
    bill_tag = bill_id or "unknown"
    LOGGER.info("Running OCR for bill_id=%s source=image path=%s", bill_tag, path.as_posix())
    try:
        with Image.open(path) as img:
            text, had_error = _ocr_image(img, bill_id=bill_id)
            if text:
                return text, None
            if had_error:
                return None, OCR_FAILED_MARKER
            return None, EMPTY_TEXT_LAYER_MARKER
    except UnidentifiedImageError:
        return None, OCR_FAILED_MARKER
    except Exception:
        LOGGER.debug("Failed OCR for image path=%s", path.as_posix(), exc_info=True)
        return None, OCR_FAILED_MARKER


def _ocr_image(image: Image.Image, bill_id: str | None = None) -> tuple[str, bool]:
    global _OCR_FALLBACK_LANG_LOGGED
    bill_tag = bill_id or "unknown"
    try:
        prepared = image
        if prepared.mode not in {"RGB", "L"}:
            prepared = prepared.convert("RGB")
        tessdata_config = ""
        if OCR_TESSDATA_DIR.exists():
            tessdata_config = f"--tessdata-dir {OCR_TESSDATA_DIR.resolve().as_posix()}"
        text = pytesseract.image_to_string(prepared, lang=OCR_LANGUAGE, config=tessdata_config)
        return text.strip(), False
    except Exception as exc:
        if OCR_LANGUAGE != "eng":
            try:
                if not _OCR_FALLBACK_LANG_LOGGED:
                    LOGGER.warning(
                        "OCR language '%s' unavailable for bill_id=%s, falling back to eng",
                        OCR_LANGUAGE,
                        bill_tag,
                    )
                    _OCR_FALLBACK_LANG_LOGGED = True
                tessdata_config = ""
                if OCR_TESSDATA_DIR.exists():
                    tessdata_config = f"--tessdata-dir {OCR_TESSDATA_DIR.resolve().as_posix()}"
                text = pytesseract.image_to_string(prepared, lang="eng", config=tessdata_config)
                return text.strip(), False
            except Exception:
                pass
        LOGGER.warning("OCR failed for bill_id=%s: %s", bill_tag, exc)
        return "", True


def _extract_doc_legacy_text(path: Path, bill_id: str | None = None) -> tuple[str | None, str | None]:
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

    process = None
    try:
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                helper_script,
                str(path),
                str(out_path),
                str(err_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        _, _ = process.communicate(timeout=timeout_seconds)
        if process.returncode != 0:
            LOGGER.warning(
                "Legacy DOC helper exited with code=%s for %s",
                process.returncode,
                path.as_posix(),
            )
            fallback_text, fallback_marker = _fallback_doc_to_pdf_ocr(path, bill_id=bill_id, trigger="extract_failed")
            if fallback_text:
                return fallback_text, None
            raw_text = extract_text_last_resort(path)
            if raw_text:
                return raw_text, None
            return None, fallback_marker or OCR_FAILED_MARKER

        if err_path.exists() and err_path.stat().st_size > 0:
            LOGGER.warning("Legacy DOC helper error for %s", path.as_posix())
            fallback_text, fallback_marker = _fallback_doc_to_pdf_ocr(path, bill_id=bill_id, trigger="extract_error")
            if fallback_text:
                return fallback_text, None
            raw_text = extract_text_last_resort(path)
            if raw_text:
                return raw_text, None
            return None, fallback_marker or OCR_FAILED_MARKER

        if not out_path.exists() or out_path.stat().st_size == 0:
            fallback_text, fallback_marker = _fallback_doc_to_pdf_ocr(path, bill_id=bill_id, trigger="empty_text")
            if fallback_text:
                return fallback_text, None
            raw_text = extract_text_last_resort(path)
            if raw_text:
                return raw_text, None
            return None, fallback_marker or EMPTY_TEXT_LAYER_MARKER

        text = out_path.read_text(encoding="utf-8", errors="ignore").strip()
        if text:
            if is_probably_garbage_text(text):
                LOGGER.warning(
                    "Garbage-like legacy DOC text detected for bill_id=%s path=%s; forcing DOC->PDF->OCR fallback (%s)",
                    bill_id or "unknown",
                    path.as_posix(),
                    garbage_reason_summary(text),
                )
                clean_text = _try_striprtf_cleanup(
                    text=text,
                    bill_id=bill_id,
                    path=path,
                    source="legacy_doc",
                )
                if clean_text:
                    return clean_text, None
            else:
                return text, None

        fallback_text, fallback_marker = _fallback_doc_to_pdf_ocr(path, bill_id=bill_id, trigger="empty_or_garbage_text")
        if fallback_text:
            return fallback_text, None
        raw_text = extract_text_last_resort(path)
        if raw_text:
            return raw_text, None
        return None, fallback_marker or EMPTY_TEXT_LAYER_MARKER
    except subprocess.TimeoutExpired:
        LOGGER.warning("Legacy DOC extraction timeout (%ss): %s", timeout_seconds, path.as_posix())
        # Try DOC->PDF conversion + OCR before killing timed-out helper.
        fallback_text, _ = _fallback_doc_to_pdf_ocr(path, bill_id=bill_id, trigger="timeout")
        _kill_process_tree(process)
        if fallback_text:
            return fallback_text, None
        raw_text = extract_text_last_resort(path)
        if raw_text:
            return raw_text, None
        return None, TIMEOUT_WORD_MARKER
    except Exception:
        LOGGER.exception("Failed to extract legacy DOC text via helper: %s", path.as_posix())
        fallback_text, fallback_marker = _fallback_doc_to_pdf_ocr(path, bill_id=bill_id, trigger="exception")
        if fallback_text:
            return fallback_text, None
        raw_text = extract_text_last_resort(path)
        if raw_text:
            return raw_text, None
        return None, fallback_marker or OCR_FAILED_MARKER
    finally:
        _kill_process_tree(process)
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            LOGGER.debug("Failed to cleanup legacy DOC out file: %s", out_path.as_posix(), exc_info=True)
        try:
            err_path.unlink(missing_ok=True)
        except Exception:
            LOGGER.debug("Failed to cleanup legacy DOC err file: %s", err_path.as_posix(), exc_info=True)


def _fallback_doc_to_pdf_ocr(
    path: Path,
    bill_id: str | None = None,
    trigger: str = "",
) -> tuple[str | None, str | None]:
    pdf_fd, pdf_name = tempfile.mkstemp(prefix="legacy_doc_", suffix=".pdf")
    os.close(pdf_fd)
    pdf_path = Path(pdf_name)
    try:
        converted, timed_out, error_text = _convert_doc_to_pdf_via_word(path, pdf_path)
        if not converted:
            LOGGER.warning(
                "Legacy DOC conversion fallback failed trigger=%s path=%s timed_out=%s error=%s",
                trigger,
                path.as_posix(),
                timed_out,
                error_text or "none",
            )
            if timed_out:
                raw_text = extract_text_last_resort(path)
                if raw_text:
                    return raw_text, None
                return None, TIMEOUT_WORD_MARKER
            raw_text = extract_text_last_resort(path)
            if raw_text:
                return raw_text, None
            return None, OCR_FAILED_MARKER

        ocr_text, fail_marker = _run_ocr_for_pdf(pdf_path, bill_id=bill_id)
        if ocr_text:
            return ocr_text, None
        raw_text = extract_text_last_resort(path)
        if raw_text:
            return raw_text, None
        return None, fail_marker or OCR_FAILED_MARKER
    finally:
        try:
            pdf_path.unlink(missing_ok=True)
        except Exception:
            LOGGER.debug("Failed to cleanup temporary PDF %s", pdf_path.as_posix(), exc_info=True)


def _convert_doc_to_pdf_via_word(path: Path, pdf_path: Path) -> tuple[bool, bool, str]:
    timeout_seconds = 15
    err_fd, err_name = tempfile.mkstemp(prefix="doc_pdf_err_", suffix=".log")
    os.close(err_fd)
    err_path = Path(err_name)

    helper_script = (
        "import sys, traceback\n"
        "from pathlib import Path\n"
        "doc_path = Path(sys.argv[1])\n"
        "pdf_path = Path(sys.argv[2])\n"
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
        "    pdf_path.parent.mkdir(parents=True, exist_ok=True)\n"
        "    doc.SaveAs(str(pdf_path.resolve()), FileFormat=17)\n"
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
                str(pdf_path),
                str(err_path),
            ],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        if result.returncode != 0:
            return False, False, f"pdf_helper_exit_code={result.returncode}"
        if err_path.exists() and err_path.stat().st_size > 0:
            return False, False, "pdf_helper_error_log_present"
        if not pdf_path.exists() or pdf_path.stat().st_size == 0:
            return False, False, "pdf_not_generated"
        return True, False, ""
    except subprocess.TimeoutExpired:
        LOGGER.warning("Legacy DOC->PDF conversion timeout (%ss): %s", timeout_seconds, path.as_posix())
        _kill_automation_winword_processes()
        return False, True, "timeout"
    except Exception as exc:
        LOGGER.warning("Legacy DOC->PDF conversion exception for %s: %s", path.as_posix(), exc)
        return False, False, str(exc)
    finally:
        try:
            err_path.unlink(missing_ok=True)
        except Exception:
            LOGGER.debug("Failed to cleanup DOC->PDF error log: %s", err_path.as_posix(), exc_info=True)


def _kill_process_tree(process: subprocess.Popen[str] | None) -> None:
    if process is None:
        return
    try:
        if process.poll() is None:
            if os.name == "nt":
                # /T kills child processes; critical to avoid orphan WINWORD.EXE.
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=15,
                )
            else:
                process.kill()
        try:
            process.wait(timeout=5)
        except Exception:
            pass
    except Exception:
        LOGGER.debug("Failed to terminate helper process tree pid=%s", process.pid, exc_info=True)
    finally:
        try:
            process.communicate(timeout=1)
        except Exception:
            pass


def _kill_automation_winword_processes() -> None:
    if os.name != "nt":
        return
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq WINWORD.EXE", "/FO", "CSV", "/NH"],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line or "No tasks are running" in line:
                continue
            if "/Automation -Embedding" not in line:
                continue
            parts = [part.strip().strip('"') for part in line.split(",")]
            if len(parts) < 2:
                continue
            pid = parts[1]
            if pid.isdigit():
                subprocess.run(
                    ["taskkill", "/PID", pid, "/F"],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                )
    except Exception:
        LOGGER.debug("Failed to cleanup automation WINWORD processes", exc_info=True)


def _extract_rtf_text(path: Path) -> str | None:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    text = re.sub(r"\\[a-zA-Z]+\d* ?", " ", raw)
    text = re.sub(r"[{}]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _count_winword_processes() -> int:
    if os.name != "nt":
        return 0
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq WINWORD.EXE", "/FO", "CSV", "/NH"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip() and "No tasks are running" not in line]
        return len(lines)
    except Exception:
        return 0


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
        winword_count = _count_winword_processes()
        if winword_count >= 20:
            LOGGER.warning(
                "Detected high WINWORD.EXE count=%s before stage3 start. This can cause timeouts and system instability.",
                winword_count,
            )
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
                done_sides: set[str] = set()
                if bool(existing.get("text_a_present")):
                    done_sides.add("a")
                if bool(existing.get("text_b_present")):
                    done_sides.add("b")
                records[bill_id] = {
                    "bill_id": bill_id,
                    "text_a": self._sanitize_text(existing.get("text_a")),
                    "text_b": self._sanitize_text(existing.get("text_b")),
                    "pending_sides": set(),
                    "done_sides": done_sides,
                    "download_errors": [],
                    "extract_errors": [],
                }

                if row["input_doc_url"] and "a" not in done_sides:
                    download_jobs.append((bill_id, "a", row["input_doc_url"], row.get("bill_url", "")))
                if row["output_doc_url"] and "b" not in done_sides:
                    download_jobs.append((bill_id, "b", row["output_doc_url"], row.get("bill_url", "")))

            download_results = await self._download_documents(download_jobs)
            for bill_id, side, file_path, error_text in download_results:
                record = records[bill_id]
                if file_path:
                    record[f"path_{side}"] = file_path
                    record["pending_sides"].add(side)
                else:
                    record[f"text_{side}"] = self._marker_for_download_error(error_text)
                    record["done_sides"].add(side)
                    record["download_errors"].append(f"{side}: {error_text}")
                    LOGGER.warning("Download failed for bill=%s side=%s: %s", bill_id, side, error_text)
                    # Persist immediately: crash-safe checkpoint after per-side completion.
                    self._write_text_record(record)

            for record in records.values():
                if not record["pending_sides"]:
                    self._write_text_record(record)

            extraction_tasks: list[tuple[str, str, str]] = []
            for bill_id, record in records.items():
                for side in ("a", "b"):
                    file_path = record.get(f"path_{side}")
                    if file_path:
                        extraction_tasks.append((bill_id, side, file_path))

            if extraction_tasks:
                extract_processes = max(1, int(self.config.stage3_extract_processes))
                LOGGER.info(
                    "Stage 3 extraction started. Files queued=%s, extract_processes=%s, cpu=%s",
                    len(extraction_tasks),
                    extract_processes,
                    mp.cpu_count(),
                )
                with mp.Pool(processes=extract_processes) as pool:
                    for bill_id, side, text, error_text, worker_marker in pool.imap_unordered(
                        extract_text_worker,
                        extraction_tasks,
                        chunksize=8,
                    ):
                        record = records[bill_id]
                        if text is None:
                            marker = self._marker_for_extraction_failure(
                                file_path=record.get(f"path_{side}"),
                                error_text=error_text,
                                worker_marker=worker_marker,
                            )
                            record[f"text_{side}"] = marker
                            LOGGER.warning(
                                "Text not extracted for bill=%s side=%s; marker=%s; error=%s",
                                bill_id,
                                side,
                                marker,
                                error_text or "none",
                            )
                        else:
                            record[f"text_{side}"] = text
                        record["pending_sides"].discard(side)
                        record["done_sides"].add(side)

                        if error_text:
                            record["extract_errors"].append(f"{side}: {error_text}")
                            LOGGER.warning(
                                "Extraction failed for bill=%s side=%s: %s",
                                bill_id,
                                side,
                                error_text,
                            )

                        # Persist every completed side to prevent data loss on crashes.
                        self._write_text_record(record)

            for record in records.values():
                if not record["pending_sides"]:
                    self._write_text_record(record)

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
        if normalized_bill_url and self.config.stage3_warm_bill_pages:
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
                    head_hex = self._read_file_head_hex(destination, bytes_count=20)
                    self._safe_unlink(destination)
                    raise ValueError(f"Downloaded file has unknown signature (head20_hex={head_hex})")

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
            or _is_image_signature(header)
        )

    def _is_archive_payload(self, file_path: Path | str | None) -> bool:
        if not file_path:
            return False
        path = Path(file_path)
        if not path.exists():
            return False
        try:
            with path.open("rb") as fp:
                head = fp.read(8)
        except Exception:
            return False

        if head.startswith((b"Rar!\x1a\x07\x00", b"Rar!\x1a\x07\x01\x00", b"7z\xbc\xaf\x27\x1c")):
            return True

        if head.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
            return not _is_docx_package(path)
        return False

    @staticmethod
    def _read_file_head_hex(path: Path, bytes_count: int = 20) -> str:
        try:
            with path.open("rb") as fp:
                data = fp.read(bytes_count)
        except Exception:
            return "unavailable"
        if not data:
            return "empty"
        return " ".join(f"{b:02X}" for b in data)

    @staticmethod
    def _infer_extension_from_url(url: str) -> str:
        path = urlsplit(url).path.lower()
        for ext in (".pdf", ".docx", ".doc", ".rtf", ".txt", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".gif"):
            if path.endswith(ext):
                return ext
        return ".bin"

    def _write_text_record(self, record: dict[str, Any]) -> None:
        payload: dict[str, Any] = {"bill_id": record["bill_id"]}
        done_sides = record.get("done_sides", set())
        if "a" in done_sides:
            payload["text_a"] = self._sanitize_text(record.get("text_a"))
        if "b" in done_sides:
            payload["text_b"] = self._sanitize_text(record.get("text_b"))
        append_jsonl_record(self.config.texts_jsonl, payload)

    def _ensure_texts_jsonl_exists(self) -> None:
        if self.config.texts_jsonl.exists():
            return
        self.config.texts_jsonl.parent.mkdir(parents=True, exist_ok=True)
        self.config.texts_jsonl.touch()
        LOGGER.info("Created placeholder JSONL file: %s", self.config.texts_jsonl.as_posix())

    def _normalize_existing_texts_jsonl(self) -> None:
        path = self.config.texts_jsonl
        with jsonl_file_lock(path):
            if not path.exists() or path.stat().st_size == 0:
                return

            cleaned_by_bill: dict[str, dict[str, Any]] = {}
            malformed = 0
            valid_rows = 0
            normalized_values = 0
            poisoned_sides_dropped = 0

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
                    text_a_present = "text_a" in record
                    text_b_present = "text_b" in record
                    if text_a_present and is_probably_garbage_text(text_a):
                        text_a_present = False
                        text_a = ""
                        poisoned_sides_dropped += 1
                    if text_b_present and is_probably_garbage_text(text_b):
                        text_b_present = False
                        text_b = ""
                        poisoned_sides_dropped += 1

                    if (
                        text_a != record.get("text_a")
                        or text_b != record.get("text_b")
                        or text_a_present != ("text_a" in record)
                        or text_b_present != ("text_b" in record)
                    ):
                        normalized_values += 1

                    normalized_record: dict[str, Any] = {"bill_id": bill_id}
                    if text_a_present:
                        normalized_record["text_a"] = text_a
                    if text_b_present:
                        normalized_record["text_b"] = text_b
                    cleaned_by_bill[bill_id] = normalized_record

            duplicates = max(0, valid_rows - len(cleaned_by_bill))
            if malformed == 0 and duplicates == 0 and normalized_values == 0 and poisoned_sides_dropped == 0:
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
                "Normalized %s: removed malformed=%s, duplicates=%s, normalized_values=%s, poisoned_sides_dropped=%s, remaining=%s",
                path.as_posix(),
                malformed,
                duplicates,
                normalized_values,
                poisoned_sides_dropped,
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

    def _marker_for_download_error(self, error_text: str) -> str:
        _ = error_text
        return ARCHIVE_OR_ERROR_MARKER

    def _marker_for_extraction_failure(
        self,
        file_path: str | Path | None,
        error_text: str,
        worker_marker: str = "",
    ) -> str:
        if worker_marker:
            return worker_marker
        if self._is_archive_payload(file_path):
            return ARCHIVE_OR_ERROR_MARKER
        if error_text:
            return OCR_FAILED_MARKER
        return EMPTY_TEXT_LAYER_MARKER

    def _load_existing_text_records(self) -> dict[str, dict[str, Any]]:
        path = self.config.texts_jsonl
        if not path.exists() or path.stat().st_size == 0:
            return {}

        records: dict[str, dict[str, Any]] = {}
        with jsonl_file_lock(path):
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
                    text_a = self._sanitize_text(record.get("text_a"))
                    text_b = self._sanitize_text(record.get("text_b"))
                    text_a_present = ("text_a" in record) and not is_probably_garbage_text(text_a)
                    text_b_present = ("text_b" in record) and not is_probably_garbage_text(text_b)
                    records[bill_id] = {
                        "bill_id": bill_id,
                        "text_a": text_a if text_a_present else "",
                        "text_b": text_b if text_b_present else "",
                        "text_a_present": text_a_present,
                        "text_b_present": text_b_present,
                    }
        return records

    def _needs_processing(self, row: dict[str, str], existing_record: dict[str, Any] | None) -> bool:
        if existing_record is None:
            return True

        has_input_url = bool(row.get("input_doc_url"))
        has_output_url = bool(row.get("output_doc_url"))
        attempted_a = bool(existing_record.get("text_a_present", False))
        attempted_b = bool(existing_record.get("text_b_present", False))

        # One attempt per side: marker/empty/non-empty are all terminal states.
        need_a = has_input_url and not attempted_a
        need_b = has_output_url and not attempted_b
        return need_a or need_b

