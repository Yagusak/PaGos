from __future__ import annotations

import logging
import re
from typing import List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .config import PipelineConfig
from .io_utils import (
    append_csv_row,
    atomic_write_json,
    load_existing_ids_from_csv,
    read_json_file,
)


LOGGER = logging.getLogger(__name__)

URLS_FIELDNAMES = ("bill_id", "url")
BILL_RE = re.compile(r"/bill/(\d+-\d+)")


class Stage1URLCollector:
    def __init__(self, config: PipelineConfig, session: requests.Session | None = None) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
        )

    def run(self) -> None:
        self.config.ensure_directories()

        state = read_json_file(
            self.config.stage1_state_json,
            default={"last_completed_page": self.config.start_page - 1, "finished": False},
        )
        if state.get("finished"):
            LOGGER.info("Stage 1 already marked as finished. Nothing to do.")
            return

        seen_ids = load_existing_ids_from_csv(self.config.urls_csv, "bill_id")
        start_page = max(self.config.start_page, int(state.get("last_completed_page", 0)) + 1)
        LOGGER.info(
            "Stage 1 started. Resuming from page %s. Existing bills in urls.csv: %s",
            start_page,
            len(seen_ids),
        )

        for page_num in range(start_page, self.config.end_page + 1):
            page_url = self.config.search_url_template.format(page_num=page_num)
            LOGGER.info("Processing page %s: %s", page_num, page_url)

            try:
                html = self._fetch_page_html(page_url)
                links = self._extract_bill_links(html)
            except Exception:
                LOGGER.exception("Could not fetch page %s via requests. Trying Playwright fallback.", page_num)
                links = self._extract_bill_links_with_playwright(page_url)
                if not links:
                    LOGGER.error("Playwright fallback also failed for page %s. Stage 1 paused.", page_num)
                    return

            if not links:
                LOGGER.warning("No links from requests on page %s. Trying Playwright fallback.", page_num)
                links = self._extract_bill_links_with_playwright(page_url)
            if not links:
                LOGGER.info(
                    "Page %s has no bill links. Stopping pagination and marking stage complete.",
                    page_num,
                )
                state["last_completed_page"] = page_num
                state["finished"] = True
                atomic_write_json(self.config.stage1_state_json, state)
                return

            new_rows = 0
            for bill_id, bill_url in links:
                if bill_id in seen_ids:
                    continue
                append_csv_row(
                    self.config.urls_csv,
                    URLS_FIELDNAMES,
                    {"bill_id": bill_id, "url": bill_url},
                )
                seen_ids.add(bill_id)
                new_rows += 1

            LOGGER.info(
                "Page %s parsed: total links=%s, new=%s, cumulative=%s",
                page_num,
                len(links),
                new_rows,
                len(seen_ids),
            )
            state["last_completed_page"] = page_num
            state["finished"] = False
            atomic_write_json(self.config.stage1_state_json, state)

        state["last_completed_page"] = self.config.end_page
        state["finished"] = True
        atomic_write_json(self.config.stage1_state_json, state)
        LOGGER.info("Stage 1 finished. Total unique bill URLs: %s", len(seen_ids))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.RequestException, ValueError)),
        reraise=True,
    )
    def _fetch_page_html(self, page_url: str) -> str:
        response = self.session.get(page_url, timeout=60)
        response.raise_for_status()
        html = response.text.strip()
        if not html:
            raise ValueError(f"Empty response from {page_url}")
        return html

    def _extract_bill_links(self, html: str) -> List[Tuple[str, str]]:
        soup = BeautifulSoup(html, "html.parser")
        items: list[Tuple[str, str]] = []
        seen_ids: set[str] = set()

        for anchor in soup.find_all("a", href=True):
            href = str(anchor["href"]).strip()
            if "/bill/" not in href:
                continue
            match = BILL_RE.search(href)
            if not match:
                continue
            bill_id = match.group(1)
            if bill_id in seen_ids:
                continue
            seen_ids.add(bill_id)
            full_url = urljoin(self.config.base_url, href)
            items.append((bill_id, full_url))

        return items

    def _extract_bill_links_with_playwright(self, page_url: str) -> List[Tuple[str, str]]:
        try:
            from playwright.sync_api import sync_playwright
        except ModuleNotFoundError:
            LOGGER.error("Playwright is not installed, fallback unavailable.")
            return []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(page_url, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(2_000)
                html = page.content()
                browser.close()
            return self._extract_bill_links(html)
        except Exception:
            LOGGER.exception("Playwright fallback failed on URL: %s", page_url)
            return []
