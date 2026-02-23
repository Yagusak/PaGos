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

    parser_stage4 = subparsers.add_parser("stage4", help="Compute embeddings and cosine distance into Excel")
    add_common_args(parser_stage4)

    parser_all = subparsers.add_parser("all", help="Run all stages sequentially")
    add_common_args(parser_all)
    parser_all.add_argument("--start-page", type=int, default=1)
    parser_all.add_argument("--end-page", type=int, default=24)
    parser_all.add_argument("--workers", type=int, default=18)
    parser_all.add_argument("--headful", action="store_true", help="Run browser with UI")

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

    if args.command == "all":
        Stage1URLCollector = _safe_import("gd_pipeline.stage1_collect_urls", "Stage1URLCollector")
        Stage2DocumentCollector = _safe_import(
            "gd_pipeline.stage2_collect_documents",
            "Stage2DocumentCollector",
        )
        Stage3TextExtractor = _safe_import("gd_pipeline.stage3_extract_texts", "Stage3TextExtractor")
        Stage4EmbeddingScorer = _safe_import("gd_pipeline.stage4_embeddings", "Stage4EmbeddingScorer")

        LOGGER.info("Running all stages...")
        Stage1URLCollector(config).run()
        collector = Stage2DocumentCollector(config, workers=int(args.workers))
        asyncio.run(collector.run(headless=not args.headful))
        extractor = Stage3TextExtractor(config)
        asyncio.run(extractor.run())
        Stage4EmbeddingScorer(config).run()
        LOGGER.info("All stages completed.")
        return

    parser.error("Unknown command")


if __name__ == "__main__":
    main()
