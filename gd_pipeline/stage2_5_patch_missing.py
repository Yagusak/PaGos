from __future__ import annotations

import asyncio
import csv
import logging
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright


LOGGER = logging.getLogger(__name__)

DOCUMENTS_PATH = Path("artifacts/documents.csv")
CONCURRENCY = 10

OUTPUT_MARKERS = (
    "принятого закона",
    "третьему чтению",
    "направляемого в совет федерации",
    "текст закона",
    "опубликован",
    "постановление совета федерации",
)

EXCLUDE_MARKERS = (
    "пояснительн",
    "заключени",
    "решени",
    "письмо",
    "протокол",
)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def read_documents_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        rows = [dict(row) for row in reader]
        fieldnames = list(reader.fieldnames or [])
    return rows, fieldnames


def write_documents_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def find_output_url(items: list[dict[str, Any]]) -> str:
    # ??????? ???? ??????? ??????? ??????. ???? ????? - ????? ?????.
    for item in reversed(items):
        href = str(item.get("href") or "").strip()
        text = str(item.get("text") or "").strip().lower()
        if any(m in text for m in ["принятого закона", "текст закона", "опубликован", "постановление совета федерации"]):
            return href

    # ???? ??????? ???, ???? ????? ??????, ?? ????????? ??????????
    for item in reversed(items):
        href = str(item.get("href") or "").strip()
        text = str(item.get("text") or "").strip().lower()

        if not href or not text:
            continue

        if any(m in text for m in OUTPUT_MARKERS):
            if any(exc in text for exc in EXCLUDE_MARKERS):
                continue
            return href
    return ""


async def patch_row(
    row: dict[str, str],
    context,
    sem: asyncio.Semaphore,
) -> str:
    bill_id = (row.get("bill_id") or "").strip()
    bill_url = (row.get("bill_url") or row.get("url") or "").strip()
    if not bill_url:
        return ""

    async with sem:
        page = await context.new_page()
        try:
            await page.goto(bill_url, wait_until="domcontentloaded", timeout=60_000)
            items = await page.evaluate(
                """
                () => {
                    return Array.from(document.querySelectorAll("a[href*='/download/']")).map(a => {
                        // ????? ????? ????? ?????? ? ????? ????????????????? ???????? (?????? ??? <td> ??? <div> ??????)
                        let ownText = (a.innerText || "").trim();
                        let parentText = a.parentElement ? (a.parentElement.innerText || "").trim() : "";

                        // ??????????, ??????????? ?????, ????? ?? ??????? ???????? ?????? ???????
                        let combinedText = (ownText + " " + parentText).substring(0, 150).toLowerCase().replace(/\\s+/g, ' ');
                        return { href: a.href, text: combinedText };
                    });
                }
                """
            )
            output_url = find_output_url(items)
            if output_url:
                LOGGER.info("Patched bill_id: %s found output_url: %s", bill_id, output_url)
            return output_url
        except Exception:
            LOGGER.exception("Failed to patch bill_id=%s, bill_url=%s", bill_id, bill_url)
            return ""
        finally:
            await page.close()


async def patch_missing_outputs(rows: list[dict[str, str]]) -> int:
    candidate_indices: list[int] = []
    for idx, row in enumerate(rows):
        input_doc_url = (row.get("input_doc_url") or "").strip()
        output_doc_url = (row.get("output_doc_url") or "").strip()
        if input_doc_url and not output_doc_url:
            candidate_indices.append(idx)

    if not candidate_indices:
        LOGGER.info("No rows to patch: all output_doc_url already filled or input_doc_url is empty.")
        return 0

    LOGGER.info("Rows to patch: %s", len(candidate_indices))
    sem = asyncio.Semaphore(CONCURRENCY)
    patched_count = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        try:
            tasks = [asyncio.create_task(patch_row(rows[idx], context, sem)) for idx in candidate_indices]
            results = await asyncio.gather(*tasks)
        finally:
            await context.close()
            await browser.close()

    for idx, output_url in zip(candidate_indices, results):
        if not output_url:
            continue
        rows[idx]["output_doc_url"] = output_url
        if "status" in rows[idx]:
            rows[idx]["status"] = "OK"
        if "error" in rows[idx]:
            rows[idx]["error"] = ""
        patched_count += 1

    return patched_count


async def main() -> None:
    setup_logging()
    if not DOCUMENTS_PATH.exists():
        LOGGER.error("Missing file: %s", DOCUMENTS_PATH.as_posix())
        return

    rows, fieldnames = read_documents_csv(DOCUMENTS_PATH)
    if not rows:
        LOGGER.warning("documents.csv is empty: %s", DOCUMENTS_PATH.as_posix())
        return
    if not fieldnames:
        LOGGER.error("Could not read CSV headers from: %s", DOCUMENTS_PATH.as_posix())
        return

    patched_count = await patch_missing_outputs(rows)
    write_documents_csv(DOCUMENTS_PATH, rows, fieldnames)
    LOGGER.info("Patch completed. Updated rows: %s", patched_count)


if __name__ == "__main__":
    asyncio.run(main())
