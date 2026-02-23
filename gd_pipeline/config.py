from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class PipelineConfig:
    base_url: str = "https://sozd.duma.gov.ru"
    search_url_template: str = (
        "https://sozd.duma.gov.ru/oz/b?"
        "class=b&b[Convocation][]=7|6&b[LastDecisions][]=8.1.1|8.2.1&"
        "b[ClassOfTheObjectLawmakingId]=1&count_items=250&page={page_num}"
    )

    start_page: int = 1
    end_page: int = 24

    stage2_workers: int = 18
    stage2_navigation_timeout_ms: int = 45_000
    stage2_max_retries: int = 2

    stage3_download_concurrency: int = 10
    stage3_http_timeout_seconds: int = 90
    stage3_http_retries: int = 3

    embedding_model_name: str = "cointegrated/rubert-tiny2"
    embedding_batch_size: int = 64

    output_dir: Path = Path("artifacts")

    def __post_init__(self) -> None:
        self.output_dir = Path(self.output_dir)

    @property
    def urls_csv(self) -> Path:
        return self.output_dir / "urls.csv"

    @property
    def documents_csv(self) -> Path:
        return self.output_dir / "documents.csv"

    @property
    def texts_jsonl(self) -> Path:
        return self.output_dir / "texts.jsonl"

    @property
    def final_result_xlsx(self) -> Path:
        return self.output_dir / "final_result.xlsx"

    @property
    def state_dir(self) -> Path:
        return self.output_dir / "state"

    @property
    def stage1_state_json(self) -> Path:
        return self.state_dir / "stage1_state.json"

    @property
    def temp_download_dir(self) -> Path:
        return self.output_dir / "tmp_downloads"

    @property
    def logs_dir(self) -> Path:
        return self.output_dir / "logs"

    @property
    def pipeline_log_file(self) -> Path:
        return self.logs_dir / "pipeline.log"

    def ensure_directories(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.temp_download_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
