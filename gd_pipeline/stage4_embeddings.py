from __future__ import annotations

import logging
import re
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import numpy as np
import pandas as pd
import requests
from scipy.spatial.distance import cosine
from sentence_transformers import SentenceTransformer

from .config import PipelineConfig
from .io_utils import iter_csv_rows, iter_jsonl, upsert_jsonl_records
from .text_quality import is_probably_garbage_text


LOGGER = logging.getLogger(__name__)
MAX_EMBED_CHARS = 12_000
MAX_EXCEL_CHARS = 32_000
MIN_MEANINGFUL_TEXT_CHARS = 50
RECOVERY_HTTP_RETRIES = 2
RECOVERY_TIMEOUT_SECONDS = 60
ILLEGAL_EXCEL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
ERROR_MARKERS = {
    "[НЕ_РАСПОЗНАНО_СКАНИРОВАНИЕ]",
    "[АРХИВ_ИЛИ_ОШИБКА]",
    "[TIMEOUT_WORD]",
    "[EMPTY_TEXT_LAYER]",
    "[POPPLER_MISSING]",
    "[OCR_FAILED]",
    # Legacy mojibake markers from old runs.
    "[РќР•_Р РђРЎРџРћР—РќРђРќРћ_РЎРљРђРќРР РћР’РђРќРР•]",
    "[РђР РҐРР’_РР›Р_РћРЁРР‘РљРђ]",
}


class Stage4EmbeddingScorer:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def run(self) -> None:
        self.config.ensure_directories()
        documents_rows = [self._normalize_documents_row(row) for row in iter_csv_rows(self.config.documents_csv)]
        if not documents_rows:
            LOGGER.warning("No rows in %s. Run stage2 first.", self.config.documents_csv.as_posix())
            return

        texts_by_bill = self._load_texts_by_bill()
        merged_rows: list[dict[str, Any]] = []
        recovered_updates: dict[str, dict[str, Any]] = {}
        recovered_sides = 0

        for row in documents_rows:
            bill_id = row["bill_id"]
            existing = texts_by_bill.get(bill_id, {"bill_id": bill_id, "text_a": "", "text_b": ""})
            text_a = self._sanitize_text(existing.get("text_a"))
            text_b = self._sanitize_text(existing.get("text_b"))

            if self._needs_recovery(text_a) and row["input_doc_url"]:
                recovered_text = self._recover_missing_text(
                    bill_id=bill_id,
                    side="a",
                    doc_url=row["input_doc_url"],
                    bill_url=row["bill_url"],
                )
                if recovered_text:
                    text_a = recovered_text
                    recovered_sides += 1

            if self._needs_recovery(text_b) and row["output_doc_url"]:
                recovered_text = self._recover_missing_text(
                    bill_id=bill_id,
                    side="b",
                    doc_url=row["output_doc_url"],
                    bill_url=row["bill_url"],
                )
                if recovered_text:
                    text_b = recovered_text
                    recovered_sides += 1

            updated_record = {
                "bill_id": bill_id,
                "text_a": text_a,
                "text_b": text_b,
            }
            texts_by_bill[bill_id] = updated_record
            recovered_updates[bill_id] = updated_record
            merged_rows.append(
                {
                    "bill_id": bill_id,
                    "bill_url": row["bill_url"],
                    "input_doc_url": row["input_doc_url"],
                    "output_doc_url": row["output_doc_url"],
                    "text_a": text_a,
                    "text_b": text_b,
                    "score": float("nan"),
                }
            )

        if recovered_updates:
            upserted = upsert_jsonl_records(
                path=self.config.texts_jsonl,
                key_field="bill_id",
                records=list(recovered_updates.values()),
            )
            LOGGER.info("Stage 4 reconciliation upserted records=%s, recovered_sides=%s", upserted, recovered_sides)

        score_indices: list[int] = []
        score_records: list[dict[str, Any]] = []
        for idx, row in enumerate(merged_rows):
            if self._is_meaningful_text(row["text_a"]) and self._is_meaningful_text(row["text_b"]):
                score_indices.append(idx)
                score_records.append(row)

        if score_records:
            try:
                model = SentenceTransformer(self.config.embedding_model_name)
                vectors_a = self._encode_side(model, score_records, "text_a")
                vectors_b = self._encode_side(model, score_records, "text_b")
                for local_idx, global_idx in enumerate(score_indices):
                    merged_rows[global_idx]["score"] = self._cosine_distance(vectors_a[local_idx], vectors_b[local_idx])
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Embedding scoring failed, preserving rows with NaN score: %s", exc)

        dropped_rows = len(merged_rows) - len(score_records)
        LOGGER.info("Дропнуто пустых строк: %s, оставлено для скоринга: %s", dropped_rows, len(score_records))

        dataframe = pd.DataFrame(
            [
                {
                    "bill_url": row["bill_url"],
                    "bill_id": row["bill_id"],
                    "input_doc_url": row["input_doc_url"],
                    "output_doc_url": row["output_doc_url"],
                    "text_a": self._trim_for_excel(row["text_a"]),
                    "text_b": self._trim_for_excel(row["text_b"]),
                    "score": row["score"],
                }
                for row in merged_rows
            ]
        )
        self._save_formatted_excel(dataframe)
        LOGGER.info(
            "Stage 4 finished. Final rows=%s (documents.csv rows), score-ready rows=%s, file=%s",
            len(dataframe),
            len(score_records),
            self.config.final_result_xlsx.as_posix(),
        )

    def _recover_missing_text(self, bill_id: str, side: str, doc_url: str, bill_url: str) -> str:
        from .stage3_extract_texts import ExtractionFailure, extract_text_from_file, extract_text_last_resort

        normalized_url = self._normalize_doc_url(doc_url)
        if not normalized_url:
            return ""
        referer = self._normalize_doc_url(bill_url) or self.config.base_url

        for attempt in range(1, RECOVERY_HTTP_RETRIES + 1):
            temp_path: Path | None = None
            try:
                response = requests.get(
                    normalized_url,
                    headers={"Referer": referer},
                    timeout=RECOVERY_TIMEOUT_SECONDS,
                )
                if response.status_code >= 400:
                    continue
                body = response.content or b""
                if not body:
                    continue

                suffix = self._infer_extension_from_url(normalized_url)
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_fp:
                    tmp_fp.write(body)
                    temp_path = Path(tmp_fp.name)

                try:
                    text = extract_text_from_file(temp_path, bill_id=bill_id)
                except ExtractionFailure:
                    text = extract_text_last_resort(temp_path)
                except Exception:
                    text = extract_text_last_resort(temp_path)

                text = self._sanitize_text(text)
                if self._has_any_text(text):
                    LOGGER.info(
                        "Stage4 recovered text for bill=%s side=%s attempt=%s",
                        bill_id,
                        side,
                        attempt,
                    )
                    return text
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(
                    "Stage4 recovery failed for bill=%s side=%s attempt=%s: %s",
                    bill_id,
                    side,
                    attempt,
                    exc,
                )
            finally:
                if temp_path is not None:
                    try:
                        temp_path.unlink(missing_ok=True)
                    except Exception:
                        LOGGER.debug("Failed to cleanup stage4 temp file %s", temp_path.as_posix(), exc_info=True)
        return ""

    def _load_texts_by_bill(self) -> dict[str, dict[str, Any]]:
        records: dict[str, dict[str, Any]] = {}
        for record in iter_jsonl(self.config.texts_jsonl):
            bill_id = str(record.get("bill_id") or "").strip()
            if not bill_id:
                continue
            records[bill_id] = {
                "bill_id": bill_id,
                "text_a": self._sanitize_text(record.get("text_a")),
                "text_b": self._sanitize_text(record.get("text_b")),
            }
        return records

    @staticmethod
    def _normalize_documents_row(row: dict[str, str]) -> dict[str, str]:
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
        if not url or url.upper().startswith("ERROR"):
            return ""
        return urljoin(self.config.base_url, url)

    @staticmethod
    def _infer_extension_from_url(url: str) -> str:
        path = urlsplit(url).path.lower()
        for ext in (".pdf", ".docx", ".doc", ".rtf", ".txt", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".gif"):
            if path.endswith(ext):
                return ext
        return ".bin"

    def _save_formatted_excel(self, dataframe: pd.DataFrame) -> None:
        with pd.ExcelWriter(self.config.final_result_xlsx, engine="xlsxwriter") as writer:
            sheet_name = "Sheet1"
            dataframe.to_excel(
                writer,
                sheet_name=sheet_name,
                index=False,
                startrow=1,
                header=False,
            )

            workbook = writer.book
            worksheet = writer.sheets[sheet_name]

            header_format = workbook.add_format(
                {
                    "bold": True,
                    "align": "center",
                    "valign": "vcenter",
                    "bg_color": "#D3D3D3",
                    "border": 1,
                }
            )
            short_center_format = workbook.add_format({"align": "center", "valign": "vcenter"})
            url_format = workbook.add_format({"valign": "top"})
            text_wrap_format = workbook.add_format({"text_wrap": True, "valign": "top"})

            for col_idx, column_name in enumerate(dataframe.columns):
                worksheet.write(0, col_idx, column_name, header_format)
                if column_name in {"text_a", "text_b"}:
                    worksheet.set_column(col_idx, col_idx, 90, text_wrap_format)
                elif column_name in {"bill_url", "input_doc_url", "output_doc_url"}:
                    worksheet.set_column(col_idx, col_idx, 45, url_format)
                else:
                    worksheet.set_column(col_idx, col_idx, 15, short_center_format)

            worksheet.freeze_panes(1, 0)
            last_row = len(dataframe)
            last_col = max(0, len(dataframe.columns) - 1)
            worksheet.autofilter(0, 0, last_row, last_col)

    def _encode_side(
        self,
        model: SentenceTransformer,
        records: list[dict[str, Any]],
        field_name: str,
    ) -> list[np.ndarray | None]:
        vectors: list[np.ndarray | None] = [None] * len(records)
        valid_indices: list[int] = []
        texts: list[str] = []

        for idx, record in enumerate(records):
            text = record.get(field_name)
            if isinstance(text, str) and text.strip():
                valid_indices.append(idx)
                texts.append(self._prepare_for_embedding(text))

        if not texts:
            return vectors

        batch_size = int(self.config.embedding_batch_size)
        while True:
            try:
                embeddings = model.encode(
                    texts,
                    batch_size=batch_size,
                    show_progress_bar=True,
                    convert_to_numpy=True,
                )
                break
            except Exception:  # noqa: BLE001
                if batch_size <= 8:
                    raise
                batch_size = max(8, batch_size // 2)
                LOGGER.warning("Stage 4 encode retry with reduced batch_size=%s", batch_size)

        for idx, emb in zip(valid_indices, embeddings):
            vectors[idx] = emb
        return vectors

    @staticmethod
    def _cosine_distance(vec_a: np.ndarray | None, vec_b: np.ndarray | None) -> float:
        if vec_a is None or vec_b is None:
            return float("nan")
        if np.linalg.norm(vec_a) == 0.0 or np.linalg.norm(vec_b) == 0.0:
            return float("nan")
        return float(cosine(vec_a, vec_b))

    @staticmethod
    def _prepare_for_embedding(text: str) -> str:
        normalized = " ".join(text.split())
        return normalized[:MAX_EMBED_CHARS]

    @staticmethod
    def _trim_for_excel(text: Any) -> Any:
        if not isinstance(text, str):
            return text
        cleaned = ILLEGAL_EXCEL_CHARS_RE.sub("", text)
        return cleaned[:MAX_EXCEL_CHARS]

    @staticmethod
    def _sanitize_text(value: Any) -> str:
        if isinstance(value, str):
            return value
        return ""

    @staticmethod
    def _has_any_text(value: str) -> bool:
        return bool(" ".join(value.split()).strip())

    @staticmethod
    def _is_meaningful_text(value: str) -> bool:
        normalized = " ".join(value.split()).strip()
        if not normalized:
            return False
        if normalized in ERROR_MARKERS:
            return False
        if is_probably_garbage_text(normalized):
            return False
        return len(normalized) > MIN_MEANINGFUL_TEXT_CHARS

    @classmethod
    def _needs_recovery(cls, value: str) -> bool:
        normalized = " ".join(value.split()).strip()
        if not normalized:
            return True
        if normalized in ERROR_MARKERS:
            return True
        if is_probably_garbage_text(normalized):
            return True
        return False
