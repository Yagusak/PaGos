# PaGos Parser

Парсерный пайплайн для законопроектов Государственной Думы:
- `stage1`: сбор ссылок на карточки законопроектов
- `stage2`: сбор ссылок на входные/выходные документы
- `stage3`: скачивание документов и извлечение текстов в `JSONL`
- `stage4`: расчёт эмбеддингов и формирование `final_result.xlsx`

Репозиторий чистый:
- без скачанных документов
- без сгенерированных артефактов
- без Telegram-скрипта и токенов

## Требования

- Python 3.11+ (проверено на Windows)
- Установленный Microsoft Word (для извлечения текста из legacy `.doc` в `stage3`)

Установка зависимостей:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

## Быстрый старт

Полный запуск (рекомендуется):

```bash
python run_pipeline.py --mode full
```

Возобновление только `stage3 + stage4` через супервизор:

```bash
python run_pipeline.py --mode stage34
```

Прямой запуск `stage3 + stage4` (без супервизора):

```bash
python run_pipeline.py --mode stage34 --direct-stage34
```

## Запуск по стадиям (CLI)

```bash
python -m gd_pipeline.cli stage1 --output-dir artifacts --start-page 1 --end-page 24
python -m gd_pipeline.cli stage2 --output-dir artifacts --workers 18
python -m gd_pipeline.cli stage3 --output-dir artifacts
python -m gd_pipeline.cli stage4 --output-dir artifacts
```

Запуск всего пайплайна одной командой:

```bash
python -m gd_pipeline.cli all --output-dir artifacts --workers 18
```

## Артефакты

Пайплайн создаёт:
- `artifacts/urls.csv`
- `artifacts/documents.csv`
- `artifacts/texts.jsonl`
- `artifacts/final_result.xlsx`
- `artifacts/state/stage1_state.json`
- `artifacts/logs/*.log`

## Надёжность

- `stage3` поддерживает возобновление и использует блокировки файлов/процессов.
- `stage34_supervisor.py` предотвращает параллельный запуск нескольких супервизоров и перезапускает зависший `stage3`.
- `stage4` очищает недопустимые для Excel управляющие символы перед экспортом.

## Безопасность

- В репозитории намеренно отсутствуют Telegram-автоматизация и учётные данные.
- `.gitignore` исключает runtime-артефакты, кэши и скачанные файлы.
