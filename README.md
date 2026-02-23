# PaGos Parser

Production parser pipeline for State Duma bills:
- `stage1`: collect bill URLs
- `stage2`: collect input/output document links
- `stage3`: download documents and extract texts to JSONL
- `stage4`: compute embeddings and write `final_result.xlsx`

This repository is clean:
- no downloaded documents
- no generated artifacts
- no Telegram script/token

## Requirements

- Python 3.11+ (tested on Windows)
- Microsoft Word installed (for legacy `.doc` extraction in stage3)

Install dependencies:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

## Quick Start

Run the full pipeline (recommended):

```bash
python run_pipeline.py --mode full
```

Resume only stage3+stage4 with supervisor:

```bash
python run_pipeline.py --mode stage34
```

Run stage3+stage4 directly (without supervisor):

```bash
python run_pipeline.py --mode stage34 --direct-stage34
```

## CLI Commands

You can run stages directly:

```bash
python -m gd_pipeline.cli stage1 --output-dir artifacts --start-page 1 --end-page 24
python -m gd_pipeline.cli stage2 --output-dir artifacts --workers 18
python -m gd_pipeline.cli stage3 --output-dir artifacts
python -m gd_pipeline.cli stage4 --output-dir artifacts
```

Or all at once:

```bash
python -m gd_pipeline.cli all --output-dir artifacts --workers 18
```

## Artifacts

Generated files:
- `artifacts/urls.csv`
- `artifacts/documents.csv`
- `artifacts/texts.jsonl`
- `artifacts/final_result.xlsx`
- `artifacts/state/stage1_state.json`
- `artifacts/logs/*.log`

## Reliability Notes

- `stage3` is resumable and uses file/process locking.
- `stage34_supervisor.py` prevents duplicate supervisor instances and restarts stale stage3 runs.
- `stage4` sanitizes illegal Excel control characters before export.

## Security

- This repo intentionally excludes Telegram automation and credentials.
- `.gitignore` excludes runtime artifacts, caches, and downloaded files.
