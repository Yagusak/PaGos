# PaGos Parser: Полный Пайплайн

Проект собирает документы по законопроектам Госдумы, извлекает тексты входной и итоговой редакции, считает semantic score и формирует итоговый Excel.

## Архитектура по стадиям

1. `stage1`:
   - Сбор ссылок на карточки законопроектов.
   - Результат: `artifacts/urls.csv`.

2. `stage2`:
   - Обход карточек и сбор ссылок на входной/выходной документы.
   - Результат: `artifacts/documents.csv`.

3. `stage3`:
   - Скачивание файлов и извлечение текста.
   - Поддержка: `pdf/docx/doc/rtf/изображения`.
   - OCR для сложных случаев (Tesseract + Poppler/Fitz fallback).
   - Last-resort извлечение из бинарника, чтобы не терять текст полностью.
   - Результат: `artifacts/texts.jsonl`.

4. `stage4`:
   - Reconciliation по `documents.csv` (left join по всем `bill_id`).
   - Дозагрузка/довосстановление пропусков текста на лету.
   - Расчет эмбеддингов и `score`.
   - Форматированный Excel (freeze panes, header style, widths, autofilter).
   - Результат: `artifacts/final_result.xlsx`.

## Почему это работает надежно

- Потокобезопасная запись `texts.jsonl` через `filelock`.
- Инкрементное сохранение прогресса в `stage3` по каждой стороне (`a`/`b`) для защиты от потерь при краше.
- Нормализация JSONL и защита от битых строк.
- Жесткие маркеры причин ошибок вместо `null`.
- Reconciliation в `stage4`: итоговая таблица строится от `documents.csv`, поэтому строки не пропадают.

## Технические зависимости

Установить Python-зависимости:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

Для OCR на Windows:

- Tesseract OCR (желательно с `rus` и `eng` языками).
- Poppler (для `pdf2image`, если доступен; есть fallback через Fitz).

## Запуск

Полный запуск:

```bash
python -m gd_pipeline.cli all --output-dir artifacts --workers 18
```

По стадиям:

```bash
python -m gd_pipeline.cli stage1 --output-dir artifacts --start-page 1 --end-page 24
python -m gd_pipeline.cli stage2 --output-dir artifacts --workers 18
python -m gd_pipeline.cli stage3 --output-dir artifacts
python -m gd_pipeline.cli stage4 --output-dir artifacts
```

Через обертку:

```bash
python run_pipeline.py --mode full
python run_pipeline.py --mode stage34
python run_pipeline.py --mode stage34 --direct-stage34
```

## Что лежит в репозитории

- Исходный код пайплайна.
- Конечный Excel-результат этого прогона:
  - `deliverables/final_result.xlsx`

Рантайм-артефакты и скачанные массивы по умолчанию не коммитятся (`.gitignore`).

## Ключевые файлы

- `gd_pipeline/cli.py`: единая CLI-точка входа.
- `gd_pipeline/stage3_extract_texts.py`: скачивание, парсинг, OCR, fallback.
- `gd_pipeline/stage4_embeddings.py`: reconciliation, scoring, Excel.
- `gd_pipeline/io_utils.py`: атомарные I/O операции и блокировки.
- `stage34_supervisor.py`: контроль и перезапуск stage3/stage4.

