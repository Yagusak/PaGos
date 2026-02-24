# PaGos Parser: Полный Пайплайн
Проект собирает документы по законопроектам Госдумы, извлекает тексты входной и итоговой редакций, считает семантическую дистанцию между версиями и обогащает результат метаданными карточки.
## Архитектура по стадиям
## Что делает pipeline

1. `stage1` — собирает ссылки на карточки законопроектов (`bill_id`, `bill_url`) в `artifacts/urls.csv`.
2. `stage2` — находит ссылки на документы входной и итоговой версий (`input_doc_url`, `output_doc_url`) и пишет `artifacts/documents.csv`.
3. `stage3` — скачивает документы и извлекает текст (`text_a`, `text_b`) в `artifacts/texts.jsonl`.
4. `stage4` — выполняет reconciliation, доизвлечение пропусков, считает `score` и формирует `artifacts/final_result.xlsx`.
5. `stage5` — обогащает итог метаданными карточки и сохраняет `artifacts/final_result_enriched.xlsx`.

## Артефакты
- `artifacts/urls.csv`
- `artifacts/documents.csv`
- `artifacts/texts.jsonl`
- `artifacts/final_result.xlsx`
- `artifacts/final_result_enriched.xlsx`
- `artifacts/state/stage1_state.json`
- `artifacts/logs/pipeline.log`

## Расчет score

`score` считается в `stage4` через эмбеддинги модели `cointegrated/rubert-tiny2`:

- `emb_a = model.encode(text_a)`
- `emb_b = model.encode(text_b)`
- `score = cosine_distance(emb_a, emb_b)`

Важно:

- Это расстояние, а не сходство.
- Чем меньше `score`, тем тексты ближе.
- Чем больше `score`, тем редакции дальше (или есть риск, что выбрана не та версия/плохое извлечение).

Практическая интерпретация (рабочая эвристика):

- `0.00–0.05`: очень близкие редакции.
- `0.05–0.15`: умеренные изменения.
- `>0.15`: заметные различия, полезна ручная проверка.
- `>0.30`: часто аномальные случаи (битый текст, не финальный документ, неверная ссылка).

`score = NaN` выставляется, если хотя бы одна сторона не прошла проверку качества (пусто, маркеры ошибок, мусорный текст и т.п.).

## Библиотеки и зачем они нужны

- `requests` — HTTP-запросы к карточкам и файлам.
- `playwright` — устойчивый рендер и загрузка там, где обычный HTTP нестабилен.
- `beautifulsoup4` — парсинг HTML в `stage1` и `stage5`.
- `tenacity` — retry/backoff в сетевых местах `stage1`.
- `pandas` + `xlsxwriter`/`openpyxl` — сборка и форматирование Excel-результатов.
- `sentence-transformers` + `scipy` + `numpy` — эмбеддинги и cosine distance.
- `PyMuPDF (fitz)` — извлечение текста из PDF и рендер PDF-страниц.
- `pdf2image` + `pytesseract` + `Pillow` — OCR fallback для сканов и изображений.
- `python-docx` — чтение DOCX.
- `pywin32` — извлечение/конверсия legacy DOC через Word COM (Windows).
- `striprtf` — декодирование RTF.
- `filelock` — блокировки JSONL для безопасной многопроцессной записи.
- `tqdm` — прогресс-бар в `stage5`.

## Отказоустойчивость

### Общие принципы

- Идемпотентность: повторный запуск продолжает работу, а не ломает уже готовые данные.
- Изоляция ошибок по строкам: сбой одного `bill_id` не валит весь прогон.
- Атомарная запись файлов и блокировки при конкурентной работе.

### По стадиям

- `stage1`:
  - checkpoint в `stage1_state.json` (resume по страницам);
  - дедуп по `bill_id`;
  - retry и fallback на Playwright.

- `stage2`:
  - конкурентные воркеры;
  - retry при timeout;
  - статусы `OK/NOT_FOUND/ERROR_TIMEOUT/ERROR`;
  - фильтрация «не тех» выходных документов (отзывы, приложения, служебные материалы).

- `stage3`:
  - lock на один активный процесс (`stage3.lock`);
  - per-side checkpoint записи в `texts.jsonl` после завершения каждой стороны;
  - retry скачивания, проверка сигнатур файлов, обработка HTML-ошибок вместо документа;
  - многоступенчатые fallback-ветки извлечения (PDF/DOCX/DOC/RTF/изображения/OCR/last-resort);
  - нормализация `texts.jsonl` (удаление дублей, битых строк, poisoned payload).

- `stage4`:
  - reconciliation от `documents.csv` (строки не теряются);
  - до-восстановление пропусков текста по ссылкам документа;
  - защита от невалидных текстов и нефинальных выходных документов;
  - graceful degradation: при сбое эмбеддингов строки сохраняются с `NaN`, а файл строится.

- `stage5`:
  - повторные попытки по карточке (`retries`), таймауты;
  - изоляция ошибок по каждому `bill_id`;
  - если метаданные не получены, заполняется маркер `[НЕ_НАЙДЕНО]`, строки не теряются.

## Установка

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

Для OCR на Windows:

- Tesseract OCR (желательно языки `rus` и `eng`)
- Poppler (опционально, есть fallback через Fitz)

## Запуск

Полный пайплайн (все стадии 1→5):

```bash
python -m gd_pipeline.cli all --output-dir artifacts --workers 18
```

По стадиям:

```bash
python -m gd_pipeline.cli stage1 --output-dir artifacts --start-page 1 --end-page 24
python -m gd_pipeline.cli stage2 --output-dir artifacts --workers 18
python -m gd_pipeline.cli stage3 --output-dir artifacts
python -m gd_pipeline.cli stage4 --output-dir artifacts
python -m gd_pipeline.cli stage5 --output-dir artifacts
```

Через обертку:

```bash
python run_pipeline.py --mode full
python run_pipeline.py --mode stage34
python run_pipeline.py --mode stage3
python run_pipeline.py --mode stage4
python run_pipeline.py --mode stage5
```

## Ключевые файлы

- `gd_pipeline/cli.py` — единая CLI-точка входа.
- `gd_pipeline/stage1_collect_urls.py` — сбор карточек.
- `gd_pipeline/stage2_collect_documents.py` — сбор ссылок на документы.
- `gd_pipeline/stage3_extract_texts.py` — скачивание, извлечение, OCR, fallback.
- `gd_pipeline/stage4_embeddings.py` — reconciliation, scoring, финальный Excel.
- `stage5_metadata.py` — enrichment метаданными карточек.
- `gd_pipeline/io_utils.py` — атомарный I/O и блокировки.
- `stage34_supervisor.py` — мониторинг и автоперезапуск stage3/stage4.

## Что коммитится

- Исходный код pipeline.
- Итоговый пример результата: `deliverables/final_result.xlsx`.

Runtime-артефакты из `artifacts/` в git обычно не добавляются.
