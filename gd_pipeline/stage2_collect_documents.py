from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from .config import PipelineConfig
from .io_utils import append_csv_row, iter_csv_rows, load_existing_ids_from_csv


LOGGER = logging.getLogger(__name__)

DOCUMENTS_FIELDNAMES = (
    "bill_id",
    "bill_url",
    "input_doc_url",
    "output_doc_url",
    "status",
    "error",
)

INPUT_MARKERS = ("при внесении", "внесенного", "внесённого")
OUTPUT_MARKERS = ("закон", "опубликован", "третьему")


@dataclass(slots=True)
class BillTask:
    bill_id: str
    bill_url: str
    retries_done: int = 0


class Stage2DocumentCollector:
    def __init__(self, config: PipelineConfig, workers: int | None = None) -> None:
        self.config = config
        self.workers = workers or config.stage2_workers
        self._write_lock = asyncio.Lock()

    async def run(self, headless: bool = True) -> None:
        self.config.ensure_directories()

        urls_rows = [row for row in iter_csv_rows(self.config.urls_csv)]
        if not urls_rows:
            LOGGER.warning(
                "No source rows in %s. Run stage1 first.",
                self.config.urls_csv.as_posix(),
            )
            return

        processed_ids = load_existing_ids_from_csv(self.config.documents_csv, "bill_id")
        queue: asyncio.Queue[BillTask] = asyncio.Queue()

        for row in urls_rows:
            bill_id = (row.get("bill_id") or "").strip()
            bill_url = (row.get("url") or "").strip()
            if not bill_id or not bill_url:
                continue
            if bill_id in processed_ids:
                continue
            queue.put_nowait(BillTask(bill_id=bill_id, bill_url=bill_url))

        if queue.empty():
            LOGGER.info("Stage 2 skipped: all bills already processed in documents.csv")
            return

        LOGGER.info(
            "Stage 2 started. Queue=%s, workers=%s, already_done=%s",
            queue.qsize(),
            self.workers,
            len(processed_ids),
        )

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context()
            workers = [
                asyncio.create_task(self._worker(worker_id=i + 1, queue=queue, context=context))
                for i in range(self.workers)
            ]

            await queue.join()
            for _ in workers:
                await queue.put(None)  # type: ignore[arg-type]
            await asyncio.gather(*workers)

            await context.close()
            await browser.close()

        LOGGER.info("Stage 2 finished.")

    async def _worker(self, worker_id: int, queue: asyncio.Queue[BillTask], context) -> None:
        page = await context.new_page()
        try:
            while True:
                task = await queue.get()
                if task is None:
                    queue.task_done()
                    return

                try:
                    await page.goto(
                        task.bill_url,
                        wait_until="domcontentloaded",
                        timeout=self.config.stage2_navigation_timeout_ms,
                    )
                    input_doc_url, output_doc_url = await self._extract_links(page)

                    status = "OK" if (input_doc_url or output_doc_url) else "NOT_FOUND"
                    await self._write_row(
                        bill_id=task.bill_id,
                        bill_url=task.bill_url,
                        input_doc_url=input_doc_url,
                        output_doc_url=output_doc_url,
                        status=status,
                        error="",
                    )
                    LOGGER.info(
                        "Worker %s processed %s (%s)",
                        worker_id,
                        task.bill_id,
                        status,
                    )
                except PlaywrightTimeoutError as exc:
                    if task.retries_done < self.config.stage2_max_retries:
                        task.retries_done += 1
                        await queue.put(task)
                        LOGGER.warning(
                            "Worker %s timeout on %s. Retry %s/%s",
                            worker_id,
                            task.bill_id,
                            task.retries_done,
                            self.config.stage2_max_retries,
                        )
                    else:
                        await self._write_row(
                            bill_id=task.bill_id,
                            bill_url=task.bill_url,
                            input_doc_url="",
                            output_doc_url="",
                            status="ERROR_TIMEOUT",
                            error=str(exc),
                        )
                        LOGGER.error(
                            "Worker %s timeout limit reached for %s",
                            worker_id,
                            task.bill_id,
                        )
                except Exception as exc:  # noqa: BLE001 - stage-level fault isolation
                    await self._write_row(
                        bill_id=task.bill_id,
                        bill_url=task.bill_url,
                        input_doc_url="",
                        output_doc_url="",
                        status="ERROR",
                        error=str(exc),
                    )
                    LOGGER.exception("Worker %s failed on bill %s", worker_id, task.bill_id)
                finally:
                    queue.task_done()
        finally:
            await page.close()

    async def _extract_links(self, page) -> tuple[str, str]:
        input_doc_url = ""
        output_doc_url = ""

        # Приоритетно берем "файловые" ссылки из событий; fallback — любые /download/
        elements = page.locator("a.a_event_files[href*='/download/']")
        count = await elements.count()
        if count == 0:
            elements = page.locator("a[href*='/download/']")
            count = await elements.count()
        files: list[tuple[str, str]] = []

        for idx in range(count):
            element = elements.nth(idx)
            # Забираем текст всей строки таблицы (или родительского блока), где лежит ссылка
            raw_text = await element.evaluate(
                "el => { const row = el.closest('tr'); return row ? row.innerText : el.parentElement.innerText; }"
            )
            text = (raw_text or "").strip().lower()
            href = await element.get_attribute("href")
            if not href:
                continue
            full_url = urljoin(self.config.base_url, href)
            files.append((text, full_url))

        # Убираем дубли URL, сохраняя порядок появления
        deduped_files: list[tuple[str, str]] = []
        seen_urls: set[str] = set()
        for text, url in files:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            deduped_files.append((text, url))
        files = deduped_files

        def is_auxiliary(text: str) -> bool:
            return any(
                marker in text
                for marker in (
                    "пояснительн",
                    "заключени",
                    "сопровод",
                    "финансово-эконом",
                    "перечень",
                    "письмо",
                    "решени",
                    "приложени",
                    "протокол",
                    "таблиц",
                    "поправ",
                )
            )

        # Точка А (ВХОД): сверху вниз, приоритет строкам с "текст"
        for text, url in files:
            has_input_marker = any(m in text for m in ("внесен", "внесён", "при внесении", "законопроект", "проект"))
            has_text_marker = "текст" in text or "пакет документов при внесении" in text
            if has_input_marker and has_text_marker and not is_auxiliary(text):
                input_doc_url = url
                break
        if not input_doc_url:
            for text, url in files:
                if any(m in text for m in ("внесен", "внесён", "при внесении", "законопроект", "проект")) and not is_auxiliary(text):
                    input_doc_url = url
                    break

        # Точка Б (ВЫХОД): снизу вверх, приоритет строкам с "текст"
        for text, url in reversed(files):
            has_output_marker = any(m in text for m in ("опубликован", "третьему", "принят", "одобрен", "редакции"))
            has_law_without_project = ("закон" in text and "проект" not in text)
            has_text_marker = "текст" in text or "федерального закона" in text
            if (has_output_marker or has_law_without_project) and has_text_marker and not is_auxiliary(text):
                output_doc_url = url
                break
        if not output_doc_url:
            for text, url in reversed(files):
                if is_auxiliary(text):
                    continue
                if any(m in text for m in ("опубликован", "третьему", "принят", "одобрен", "редакции")) or (
                    "закон" in text and "проект" not in text
                ):
                    # Исключаем ложное "принятие законопроекта"
                    if "законопроект" in text and "закон" not in text.replace("законопроект", ""):
                        continue
                    output_doc_url = url
                    break

        # Если вход и выход совпали, пытаемся выбрать другой кандидат на выход
        if input_doc_url and output_doc_url and input_doc_url == output_doc_url:
            for text, url in reversed(files):
                if url == input_doc_url:
                    continue
                if is_auxiliary(text):
                    continue
                if ("закон" in text and "проект" not in text) or "опубликован" in text:
                    output_doc_url = url
                    break

        return input_doc_url, output_doc_url

    async def _write_row(
        self,
        bill_id: str,
        bill_url: str,
        input_doc_url: str,
        output_doc_url: str,
        status: str,
        error: str,
    ) -> None:
        row = {
            "bill_id": bill_id,
            "bill_url": bill_url,
            "input_doc_url": input_doc_url,
            "output_doc_url": output_doc_url,
            "status": status,
            "error": error,
        }
        async with self._write_lock:
            try:
                append_csv_row(self.config.documents_csv, DOCUMENTS_FIELDNAMES, row)
            except Exception:  # noqa: BLE001 - write failure should not stop workers
                LOGGER.exception(
                    "Failed to append documents row for bill_id=%s into %s",
                    bill_id,
                    self.config.documents_csv.as_posix(),
                )
