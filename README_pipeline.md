# Госдума Bill NLP Pipeline

## Установка
```bash
pip install -r requirements.txt
python -m playwright install chromium
```

## Запуск всех этапов
```bash
python -m gd_pipeline.cli all --output-dir artifacts --workers 18
```

## Запуск по этапам
```bash
python -m gd_pipeline.cli stage1 --output-dir artifacts --start-page 1 --end-page 24
python -m gd_pipeline.cli stage2 --output-dir artifacts --workers 18
python -m gd_pipeline.cli stage3 --output-dir artifacts
python -m gd_pipeline.cli stage4 --output-dir artifacts
```

## Артефакты
- `artifacts/urls.csv`
- `artifacts/documents.csv`
- `artifacts/texts.jsonl`
- `artifacts/final_result.xlsx`
- `artifacts/state/stage1_state.json`
- `artifacts/logs/pipeline.log`

## Идемпотентность
- `stage1`: резюмирует с последней страницы из `stage1_state.json`, не дублирует `bill_id` в `urls.csv`.
- `stage2`: пропускает `bill_id`, уже присутствующие в `documents.csv`.
- `stage3`: пропускает `bill_id`, уже присутствующие в `texts.jsonl`, использует кэш скачанных файлов.
- `stage4`: детерминированно пересобирает `final_result.xlsx` из `texts.jsonl`.
