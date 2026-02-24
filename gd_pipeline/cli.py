from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
from pathlib import Path

from .config import PipelineConfig
from .logging_utils import setup_logging


LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Production-ready pipeline for parsing and NLP analysis of State Duma bills."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_args(target_parser: argparse.ArgumentParser) -> None:
        target_parser.add_argument("--output-dir", type=Path, default=Path("artifacts"))
        target_parser.add_argument("--log-level", type=str, default="INFO")

    parser_stage1 = subparsers.add_parser("stage1", help="Collect bill URLs into urls.csv")
    add_common_args(parser_stage1)
    parser_stage1.add_argument("--start-page", type=int, default=1)
    parser_stage1.add_argument("--end-page", type=int, default=24)

    parser_stage2 = subparsers.add_parser("stage2", help="Collect input/output document URLs")
    add_common_args(parser_stage2)
    parser_stage2.add_argument("--workers", type=int, default=18)
    parser_stage2.add_argument("--headful", action="store_true", help="Run browser with UI")

    parser_stage3 = subparsers.add_parser("stage3", help="Download documents and extract texts to texts.jsonl")
    add_common_args(parser_stage3)
    parser_stage3.add_argument("--download-concurrency", type=int, default=6)
    parser_stage3.add_argument("--extract-processes", type=int, default=4)
    parser_stage3.add_argument("--http-timeout-seconds", type=int, default=90)
    parser_stage3.add_argument("--http-retries", type=int, default=3)
    parser_stage3.add_argument("--warm-bill-pages", action="store_true")

    parser_stage4 = subparsers.add_parser("stage4", help="Compute embeddings and cosine distance into Excel")
    add_common_args(parser_stage4)

    parser_stage5 = subparsers.add_parser("stage5", help="Enrich final_result.xlsx with bill card metadata")
    add_common_args(parser_stage5)
    parser_stage5.add_argument("--input", type=Path, default=None, help="Input XLSX (default: <output-dir>/final_result.xlsx)")
    parser_stage5.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output XLSX (default: <output-dir>/final_result_enriched.xlsx)",
    )
    parser_stage5.add_argument("--min-delay", type=float, default=0.5)
    parser_stage5.add_argument("--max-delay", type=float, default=1.0)
    parser_stage5.add_argument("--timeout", type=int, default=30)
    parser_stage5.add_argument("--retries", type=int, default=3)
    parser_stage5.add_argument("--max-workers", type=int, default=5)

    parser_clean_texts = subparsers.add_parser(
        "clean-texts",
        help="Remove poisoned text payloads from texts.jsonl so stage3 can rebuild them",
    )
    add_common_args(parser_clean_texts)
    parser_clean_texts.add_argument("--no-backup", action="store_true")

    parser_all = subparsers.add_parser("all", help="Run all stages sequentially")
    add_common_args(parser_all)
    parser_all.add_argument("--start-page", type=int, default=1)
    parser_all.add_argument("--end-page", type=int, default=24)
    parser_all.add_argument("--workers", type=int, default=18)
    parser_all.add_argument("--headful", action="store_true", help="Run browser with UI")
    parser_all.add_argument("--download-concurrency", type=int, default=6)
    parser_all.add_argument("--extract-processes", type=int, default=4)
    parser_all.add_argument("--http-timeout-seconds", type=int, default=90)
    parser_all.add_argument("--http-retries", type=int, default=3)
    parser_all.add_argument("--warm-bill-pages", action="store_true")
    parser_all.add_argument("--stage5-min-delay", type=float, default=0.5)
    parser_all.add_argument("--stage5-max-delay", type=float, default=1.0)
    parser_all.add_argument("--stage5-timeout", type=int, default=30)
    parser_all.add_argument("--stage5-retries", type=int, default=3)
    parser_all.add_argument("--stage5-max-workers", type=int, default=5)

    return parser


def _safe_import(module_name: str, class_name: str):
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except ModuleNotFoundError as exc:
        LOGGER.error(
            "Missing dependency: %s. Install requirements first: pip install -r requirements.txt",
            exc.name,
        )
        raise SystemExit(1) from exc


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = PipelineConfig(output_dir=args.output_dir)
    config.ensure_directories()
    setup_logging(config.pipeline_log_file, level=args.log_level)

    if args.command in {"stage1", "all"}:
        config.start_page = int(args.start_page)
        config.end_page = int(args.end_page)

    if args.command in {"stage3", "all"}:
        config.stage3_download_concurrency = max(1, int(args.download_concurrency))
        config.stage3_extract_processes = max(1, int(args.extract_processes))
        config.stage3_http_timeout_seconds = max(30, int(args.http_timeout_seconds))
        config.stage3_http_retries = max(1, int(args.http_retries))
        config.stage3_warm_bill_pages = bool(args.warm_bill_pages)

    if args.command == "stage1":
        Stage1URLCollector = _safe_import("gd_pipeline.stage1_collect_urls", "Stage1URLCollector")

        Stage1URLCollector(config).run()
        return

    if args.command == "stage2":
        Stage2DocumentCollector = _safe_import(
            "gd_pipeline.stage2_collect_documents",
            "Stage2DocumentCollector",
        )

        collector = Stage2DocumentCollector(config, workers=int(args.workers))
        asyncio.run(collector.run(headless=not args.headful))
        return

    if args.command == "stage3":
        Stage3TextExtractor = _safe_import("gd_pipeline.stage3_extract_texts", "Stage3TextExtractor")

        extractor = Stage3TextExtractor(config)
        asyncio.run(extractor.run())
        return

    if args.command == "stage4":
        Stage4EmbeddingScorer = _safe_import("gd_pipeline.stage4_embeddings", "Stage4EmbeddingScorer")

        Stage4EmbeddingScorer(config).run()
        return

    if args.command == "stage5":
        Stage5Config = _safe_import("stage5_metadata", "Stage5Config")
        run_stage5 = _safe_import("stage5_metadata", "run_stage5")

        input_xlsx = args.input or config.final_result_xlsx
        output_xlsx = args.output or (config.output_dir / "final_result_enriched.xlsx")
        stage5_cfg = Stage5Config(
            input_xlsx=input_xlsx,
            output_xlsx=output_xlsx,
            min_delay_seconds=float(args.min_delay),
            max_delay_seconds=float(args.max_delay),
            timeout_seconds=max(5, int(args.timeout)),
            retries=max(1, int(args.retries)),
            max_workers=max(1, int(args.max_workers)),
        )
        run_stage5(config=stage5_cfg)
        return

    if args.command == "clean-texts":
        clean_texts_jsonl_file = _safe_import("gd_pipeline.clean_texts_jsonl", "clean_texts_jsonl_file")
        stats = clean_texts_jsonl_file(path=config.texts_jsonl, make_backup=not bool(args.no_backup))
        LOGGER.info(
            "clean-texts completed: rows_read=%s unique_bills=%s written_records=%s dropped_side_a=%s dropped_side_b=%s dropped_records=%s malformed=%s",
            stats["rows_read"],
            stats["unique_bills"],
            stats["written_records"],
            stats["dropped_side_a"],
            stats["dropped_side_b"],
            stats["dropped_records"],
            stats["malformed"],
        )
        return

    if args.command == "all":
        Stage1URLCollector = _safe_import("gd_pipeline.stage1_collect_urls", "Stage1URLCollector")
        Stage2DocumentCollector = _safe_import(
            "gd_pipeline.stage2_collect_documents",
            "Stage2DocumentCollector",
        )
        Stage3TextExtractor = _safe_import("gd_pipeline.stage3_extract_texts", "Stage3TextExtractor")
        Stage4EmbeddingScorer = _safe_import("gd_pipeline.stage4_embeddings", "Stage4EmbeddingScorer")
        Stage5Config = _safe_import("stage5_metadata", "Stage5Config")
        run_stage5 = _safe_import("stage5_metadata", "run_stage5")

        LOGGER.info("Running all stages...")
        Stage1URLCollector(config).run()
        collector = Stage2DocumentCollector(config, workers=int(args.workers))
        asyncio.run(collector.run(headless=not args.headful))
        extractor = Stage3TextExtractor(config)
        asyncio.run(extractor.run())
        Stage4EmbeddingScorer(config).run()
        stage5_cfg = Stage5Config(
            input_xlsx=config.final_result_xlsx,
            output_xlsx=config.output_dir / "final_result_enriched.xlsx",
            min_delay_seconds=float(args.stage5_min_delay),
            max_delay_seconds=float(args.stage5_max_delay),
            timeout_seconds=max(5, int(args.stage5_timeout)),
            retries=max(1, int(args.stage5_retries)),
            max_workers=max(1, int(args.stage5_max_workers)),
        )
        run_stage5(config=stage5_cfg)
        LOGGER.info("All stages completed.")
        return

    parser.error("Unknown command")


if __name__ == "__main__":
    main()
