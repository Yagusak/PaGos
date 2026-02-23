from __future__ import annotations

import logging
import re
from typing import Any

import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine
from sentence_transformers import SentenceTransformer

from .config import PipelineConfig
from .io_utils import iter_csv_rows, iter_jsonl


LOGGER = logging.getLogger(__name__)
MAX_EMBED_CHARS = 12_000
MAX_EXCEL_CHARS = 32_000
ILLEGAL_EXCEL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")


class Stage4EmbeddingScorer:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def run(self) -> None:
        self.config.ensure_directories()
        records_by_bill: dict[str, dict[str, Any]] = {}
        raw_rows = 0
        for record in iter_jsonl(self.config.texts_jsonl):
            raw_rows += 1
            bill_id = str(record.get("bill_id") or "").strip()
            if not bill_id:
                continue
            records_by_bill[bill_id] = {
                "bill_id": bill_id,
                "text_a": self._sanitize_text(record.get("text_a")),
                "text_b": self._sanitize_text(record.get("text_b")),
            }

        records = list(records_by_bill.values())
        if not records:
            LOGGER.warning(
                "No rows in %s. Run stage3 first.",
                self.config.texts_jsonl.as_posix(),
            )
            return

        LOGGER.info("Stage 4 started. Raw rows=%s, unique bills for embedding=%s", raw_rows, len(records))
        model = SentenceTransformer(self.config.embedding_model_name)

        vectors_a = self._encode_side(model, records, "text_a")
        vectors_b = self._encode_side(model, records, "text_b")
        passport_map = self._load_passport_map()

        rows: list[dict[str, Any]] = []
        for idx, record in enumerate(records):
            bill_id = str(record.get("bill_id", "")).strip()
            vec_a = vectors_a[idx]
            vec_b = vectors_b[idx]
            distance = self._cosine_distance(vec_a, vec_b)

            rows.append(
                {
                    "??????? ??????": passport_map.get(bill_id, bill_id),
                    "bill_id": bill_id,
                    "????? ????": self._trim_for_excel(record.get("text_a")),
                    "????? ?????": self._trim_for_excel(record.get("text_b")),
                    "?????????? ??????????": distance,
                }
            )

        dataframe = pd.DataFrame(rows)
        dataframe.to_excel(self.config.final_result_xlsx, index=False)
        LOGGER.info(
            "Stage 4 finished. Result file saved: %s",
            self.config.final_result_xlsx.as_posix(),
        )

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
            except Exception:  # noqa: BLE001 - dynamic fallback for memory spikes
                if batch_size <= 8:
                    raise
                batch_size = max(8, batch_size // 2)
                LOGGER.warning("Stage 4 encode retry with reduced batch_size=%s", batch_size)

        for idx, emb in zip(valid_indices, embeddings):
            vectors[idx] = emb
        return vectors

    def _load_passport_map(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for row in iter_csv_rows(self.config.documents_csv):
            bill_id = (row.get("bill_id") or "").strip()
            bill_url = (row.get("bill_url") or row.get("url") or "").strip()
            if bill_id and bill_url:
                mapping[bill_id] = bill_url
        return mapping

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
