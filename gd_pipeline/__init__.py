"""Production pipeline for parsing and NLP analysis of State Duma bills."""

__all__ = [
    "PipelineConfig",
    "Stage1URLCollector",
    "Stage2DocumentCollector",
    "Stage3TextExtractor",
    "Stage4EmbeddingScorer",
]
