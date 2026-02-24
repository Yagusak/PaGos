from __future__ import annotations

import argparse
import concurrent.futures as futures
import logging
import random
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from tqdm.auto import tqdm
except Exception as exc:  # pragma: no cover - dependency guard
    raise SystemExit("Не найден tqdm. Установите: pip install tqdm") from exc


LOGGER = logging.getLogger("stage5_metadata")
MISSING = "[НЕ_НАЙДЕНО]"
ILLEGAL_EXCEL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
DATE_RE = re.compile(r"\b(\d{2}\.\d{2}\.\d{4})\b")

METADATA_COLUMNS = [
    "Инициатор",
    "Профильный комитет",
    "Дата внесения в ГД",
    "Тематический блок / Отрасль законодательства",
    "Статус",
]


@dataclass
class Stage5Config:
    input_xlsx: Path = Path("artifacts/final_result.xlsx")
    output_xlsx: Path = Path("artifacts/final_result_enriched.xlsx")
    min_delay_seconds: float = 0.5
    max_delay_seconds: float = 1.0
    timeout_seconds: int = 30
    retries: int = 3
    max_workers: int = 5


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _clean_for_excel(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return ILLEGAL_EXCEL_CHARS_RE.sub("", value)


def _missing_metadata() -> dict[str, str]:
    return {col: MISSING for col in METADATA_COLUMNS}


def _normalize_bill_id(value: Any) -> str:
    bill_id = _clean_text(value)
    if not bill_id:
        return ""
    # Защита от случаев, когда Excel превращает идентификатор в число с ".0".
    if bill_id.endswith(".0") and bill_id[:-2].isdigit():
        return bill_id[:-2]
    return bill_id


def _extract_passport_pairs(soup: BeautifulSoup) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        key = _clean_text(cells[0].get_text(" ", strip=True))
        value = _clean_text(cells[1].get_text(" ", strip=True))
        if not key:
            continue
        if key not in pairs and value:
            pairs[key] = value
    return pairs


def _find_by_labels(pairs: dict[str, str], labels: list[str]) -> str:
    for label in labels:
        label_low = label.lower()
        for key, value in pairs.items():
            if label_low in key.lower():
                cleaned = _clean_text(value)
                if cleaned:
                    return cleaned
    return MISSING


def _extract_date_submitted(soup: BeautifulSoup) -> str:
    for stage in soup.select(".root-stage.bh_item.bhi1"):
        stage_text = _clean_text(stage.get_text(" ", strip=True))
        if "внесение законопроекта" not in stage_text.lower():
            continue
        date_match = DATE_RE.search(stage_text)
        if date_match:
            return date_match.group(1)

    page_text = _clean_text(soup.get_text(" ", strip=True))
    match = re.search(
        r"Внесение законопроекта в Государственную Думу.{0,250}?(\d{2}\.\d{2}\.\d{4})",
        page_text,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1)

    return MISSING


def _extract_status(soup: BeautifulSoup) -> str:
    text = _clean_text(soup.get_text(" ", strip=True)).lower()

    reject_keywords = (
        "законопроект отклонен",
        "отклонить законопроект",
        "отклонен государственной думой",
        "снят с рассмотрения",
        "возвращен субъекту права законодательной инициативы",
    )
    accept_keywords = (
        "закон опубликован",
        "подписать федеральный закон",
        "подписан президентом российской федерации",
    )

    if any(keyword in text for keyword in reject_keywords):
        return "отклонен"

    if soup.select_one(".root-stage.bh_item.bhi11.green"):
        return "принят"

    if soup.select_one(".root-stage.bh_item.bhi8.green") and any(keyword in text for keyword in accept_keywords):
        return "принят"

    if any(keyword in text for keyword in accept_keywords):
        return "принят"

    if "внесение законопроекта" in text:
        return "на рассмотрении"

    return MISSING


def _extract_metadata_from_html(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    passport_pairs = _extract_passport_pairs(soup)

    initiator = _find_by_labels(
        passport_pairs,
        ["Субъект права законодательной инициативы", "Инициатор"],
    )
    profile_committee = _find_by_labels(
        passport_pairs,
        ["Профильный комитет", "Ответственный комитет"],
    )
    thematic_block = _find_by_labels(
        passport_pairs,
        ["Тематический блок законопроектов"],
    )
    legislation_sector = _find_by_labels(
        passport_pairs,
        ["Отрасль законодательства"],
    )
    date_submitted = _extract_date_submitted(soup)
    status = _extract_status(soup)

    if thematic_block != MISSING and legislation_sector != MISSING:
        thematic_or_sector = f"{thematic_block} | {legislation_sector}"
    elif thematic_block != MISSING:
        thematic_or_sector = thematic_block
    elif legislation_sector != MISSING:
        thematic_or_sector = legislation_sector
    else:
        thematic_or_sector = MISSING

    return {
        "Инициатор": initiator,
        "Профильный комитет": profile_committee,
        "Дата внесения в ГД": date_submitted,
        "Тематический блок / Отрасль законодательства": thematic_or_sector,
        "Статус": status,
    }


def _build_bill_url(bill_id: str) -> str:
    return f"https://sozd.duma.gov.ru/bill/{bill_id}"


def _request_bill_html(
    session: requests.Session,
    bill_id: str,
    config: Stage5Config,
) -> str | None:
    if not bill_id:
        return None

    url = _build_bill_url(bill_id)
    for attempt in range(1, config.retries + 1):
        try:
            response = session.get(url, timeout=config.timeout_seconds)
            if response.status_code >= 400:
                raise requests.HTTPError(f"HTTP {response.status_code}")
            return response.text
        except Exception as exc:  # noqa: BLE001 - per-bill fault isolation
            if attempt >= config.retries:
                LOGGER.warning("bill_id=%s не получен: %s", bill_id, exc)
                return None
            time.sleep(min(2.0, 0.4 * attempt))
    return None


_THREAD_LOCAL = threading.local()


def _get_thread_session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is not None:
        return session

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    _THREAD_LOCAL.session = session
    return session


def _fetch_metadata_for_bill(bill_id: str, config: Stage5Config) -> tuple[str, dict[str, str]]:
    metadata = _missing_metadata()
    if not bill_id:
        return bill_id, metadata

    time.sleep(random.uniform(config.min_delay_seconds, config.max_delay_seconds))

    try:
        html = _request_bill_html(
            session=_get_thread_session(),
            bill_id=bill_id,
            config=config,
        )
        if html:
            metadata = _extract_metadata_from_html(html)
    except Exception as exc:  # noqa: BLE001 - per-bill fault isolation
        LOGGER.warning("Ошибка парсинга bill_id=%s: %s", bill_id, exc)
    return bill_id, metadata


def _enrich_dataframe(df: pd.DataFrame, config: Stage5Config) -> pd.DataFrame:
    if "bill_id" not in df.columns:
        raise ValueError("Во входном Excel отсутствует колонка bill_id")

    bill_ids_raw = df["bill_id"].tolist()
    bill_ids = [_normalize_bill_id(value) for value in bill_ids_raw]

    unique_bill_ids: list[str] = []
    seen: set[str] = set()
    for bill_id in bill_ids:
        if not bill_id or bill_id in seen:
            continue
        seen.add(bill_id)
        unique_bill_ids.append(bill_id)

    metadata_by_bill: dict[str, dict[str, str]] = {}
    workers = max(1, min(5, int(config.max_workers)))
    with futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_bill = {
            executor.submit(_fetch_metadata_for_bill, bill_id, config): bill_id
            for bill_id in unique_bill_ids
        }
        with tqdm(total=len(unique_bill_ids), desc="Сбор метаданных", unit="bill") as pbar:
            for future in futures.as_completed(future_to_bill):
                fallback_bill_id = future_to_bill[future]
                try:
                    bill_id, metadata = future.result()
                except Exception as exc:  # noqa: BLE001 - per-future fault isolation
                    LOGGER.warning("Ошибка future bill_id=%s: %s", fallback_bill_id, exc)
                    bill_id, metadata = fallback_bill_id, _missing_metadata()
                metadata_by_bill[bill_id] = metadata
                pbar.update(1)

    enriched = df.copy()
    metadata_columns_data: dict[str, list[str]] = {col: [] for col in METADATA_COLUMNS}

    for bill_id in bill_ids:
        row_meta = metadata_by_bill.get(bill_id, _missing_metadata())
        for col in METADATA_COLUMNS:
            metadata_columns_data[col].append(row_meta.get(col, MISSING))

    for col in METADATA_COLUMNS:
        if col in enriched.columns:
            enriched.drop(columns=[col], inplace=True)

    insertion_base = "bill_url" if "bill_url" in enriched.columns else enriched.columns[0]
    insert_at = enriched.columns.get_loc(insertion_base) + 1
    for offset, col in enumerate(METADATA_COLUMNS):
        enriched.insert(insert_at + offset, col, metadata_columns_data[col])

    for col in enriched.columns:
        enriched[col] = enriched[col].map(_clean_for_excel)

    return enriched


def _save_formatted_excel(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        sheet_name = "Sheet1"
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, header=False)

        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        header_format = workbook.add_format(
            {
                "bold": True,
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#D3D3D3",
                "border": 1,
                "text_wrap": True,
            }
        )
        center_format = workbook.add_format({"align": "center", "valign": "vcenter"})
        url_format = workbook.add_format({"valign": "top"})
        text_wrap_format = workbook.add_format({"text_wrap": True, "valign": "top"})
        metadata_format = workbook.add_format({"text_wrap": True, "valign": "top"})

        for col_idx, column_name in enumerate(df.columns):
            worksheet.write(0, col_idx, column_name, header_format)

            if column_name in {"text_a", "text_b"}:
                worksheet.set_column(col_idx, col_idx, 90, text_wrap_format)
            elif column_name in {"bill_url", "input_doc_url", "output_doc_url"}:
                worksheet.set_column(col_idx, col_idx, 45, url_format)
            elif column_name in METADATA_COLUMNS:
                if column_name in {"Дата внесения в ГД", "Статус"}:
                    worksheet.set_column(col_idx, col_idx, 20, center_format)
                else:
                    worksheet.set_column(col_idx, col_idx, 45, metadata_format)
            elif column_name in {"bill_id", "score"}:
                worksheet.set_column(col_idx, col_idx, 15, center_format)
            else:
                worksheet.set_column(col_idx, col_idx, 18, metadata_format)

        worksheet.freeze_panes(1, 0)
        last_row = len(df)
        last_col = max(0, len(df.columns) - 1)
        worksheet.autofilter(0, 0, last_row, last_col)


def run_stage5(config: Stage5Config) -> None:
    LOGGER.info("Чтение входного файла: %s", config.input_xlsx.as_posix())
    df = pd.read_excel(config.input_xlsx)
    rows_before = len(df)
    LOGGER.info("Входных строк: %s", rows_before)

    enriched_df = _enrich_dataframe(df=df, config=config)
    rows_after = len(enriched_df)
    if rows_after != rows_before:
        raise RuntimeError(f"Потеря строк: было {rows_before}, стало {rows_after}")

    LOGGER.info("Сохранение результата: %s", config.output_xlsx.as_posix())
    _save_formatted_excel(enriched_df, config.output_xlsx)
    LOGGER.info("Готово. Строк сохранено: %s", rows_after)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Обогащение final_result.xlsx метаданными карточек законопроектов Госдумы."
    )
    parser.add_argument("--input", type=Path, default=Path("artifacts/final_result.xlsx"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/final_result_enriched.xlsx"))
    parser.add_argument("--min-delay", type=float, default=0.5)
    parser.add_argument("--max-delay", type=float, default=1.0)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--max-workers", type=int, default=5)
    parser.add_argument("--log-level", type=str, default="INFO")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    config = Stage5Config(
        input_xlsx=args.input,
        output_xlsx=args.output,
        min_delay_seconds=max(0.0, float(args.min_delay)),
        max_delay_seconds=max(float(args.min_delay), float(args.max_delay)),
        timeout_seconds=max(5, int(args.timeout)),
        retries=max(1, int(args.retries)),
        max_workers=max(1, min(5, int(args.max_workers))),
    )
    run_stage5(config)


if __name__ == "__main__":
    main()
