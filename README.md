# Multi-Marketplace Stock Sync v2.0 — Wildberries API v3 + Ozon Seller API

Асинхронный отказоустойчивый сервис на **Python 3.11+** для автоматической
синхронизации складских остатков одновременно на **Wildberries** (Marketplace API v3)
и **Ozon** (Seller API) из одного CSV-файла учётной системы.

> Версия 2.0 — полная архитектурная переработка синхронного скрипта `wb_v3_sync.py`:
> `requests` → `aiohttp`, `time.sleep` → `await asyncio.sleep`,
> `BlockingScheduler` → `AsyncIOScheduler`, `os.getenv` → `pydantic-settings`.

---

## Ключевые возможности

| Возможность | Описание |
|---|---|
| **Полная асинхронность** | Один event loop, `aiohttp`, никаких блокирующих вызовов. |
| **Параллельная отправка** | WB и Ozon обрабатываются одновременно через `asyncio.gather()`. |
| **Изолированный Rate Limit Shield** | У каждого маркетплейса собственное состояние backoff: 429 на WB не тормозит Ozon. |
| **Exponential Backoff + jitter** | Задержка удваивается (`base * 2^attempt`), ограничена потолком, учитывает `Retry-After`. |
| **Батчинг по 100** | Нарезка payload на пакеты по 100 позиций для обоих API. |
| **Кросс-маркетплейсный маппинг** | `mapping.json` связывает внутренний SKU ↔ WB barcode ↔ Ozon offer_id. |
| **Валидация конфигурации** | `pydantic-settings`: сервис не стартует с неполными или битыми секретами. |
| **Неблокирующий планировщик** | `AsyncIOScheduler`, запуск каждые 15 минут, `max_instances=1`, `coalesce=True`. |
| **Коммерческая гибкость** | Флаги `ENABLE_WB` / `ENABLE_OZON` — «эконом-тариф» на одну площадку одним переключателем. |
| **Ротация логов** | `RotatingFileHandler`, 5 МБ × 5 файлов. |

---

## Архитектура

```text
            stocks.csv (Pandas: dropna → strip → drop_duplicates)
                               |
                               v
                     ProductMapper (mapping.json)
                               |
            +------------------+------------------+
            |                                     |
            v                                     v
    WBStockItem[]  (sku = barcode)        OzonStockItem[] (offer_id)
            |                                     |
            v                                     v
   WildberriesClient                        OzonClient
   - свой aiohttp session                   - свой aiohttp session
   - свой cooldown / backoff                - свой cooldown / backoff
   - PUT /api/v3/stocks/{wh}                - POST /v1/product/import/stocks
            |                                     |
            +----------- asyncio.gather() --------+
                               |
                               v
                   SyncScheduler (AsyncIOScheduler, 15 мин)
```

### Почему backoff изолирован

Состояние `_cooldown_until` и `_state_lock` живут **внутри экземпляра клиента**.
Когда Wildberries отвечает `429`, клиент WB взводит собственное окно ожидания и
засыпает через `await asyncio.sleep(...)` — event loop в это время продолжает
обслуживать корутины клиента Ozon. В синхронной версии `time.sleep(60)`
останавливал **весь процесс целиком**.

---

## Структура проекта

```text
.
├── .env                 # секреты (в .gitignore)
├── .env.example         # шаблон переменных окружения
├── .gitignore
├── config.py            # валидатор настроек на pydantic-settings
├── mapping.json         # sku_internal ↔ wb_barcode ↔ ozon_offer_id
├── mapper.py            # ProductMapper + модели payload
├── wb_client.py         # асинхронный клиент Wildberries API v3
├── ozon_client.py       # асинхронный клиент Ozon Seller API
├── scheduler.py         # AsyncIOScheduler-обёртка
├── main.py              # точка входа, Pandas-пайплайн, asyncio.gather
├── requirements.txt
└── README.md
```

---

## Установка

```bash
git clone https://github.com/Bless221/wb-api-stock-sync.git
cd wb-api-stock-sync

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements.txt
cp .env.example .env             # Windows: copy .env.example .env
```

Заполните `.env` своими ключами и укажите реальные `WB_WAREHOUSE_ID` /
`OZON_WAREHOUSE_ID`.

---

## Формат данных

### `stocks.csv`

```csv
item_sku,quantity
SKU-1001,12
SKU-1002,0
SKU-1003,45
```

* `item_sku` — внутренний SKU склада;
* `quantity` — целое неотрицательное количество;
* дубликаты схлопываются через `df.drop_duplicates(subset=["item_sku"], keep="last")`.

### `mapping.json`

```json
{
  "version": "2.0",
  "items": [
    {
      "sku_internal": "SKU-1001",
      "title": "Футболка оверсайз, белая, M",
      "wb_barcode": "2000000010014",
      "ozon_offer_id": "TS-OVER-WHT-M",
      "active": true
    }
  ]
}
```

* `wb_barcode: null` — товар не выгружается на WB;
* `ozon_offer_id: null` — товар не выгружается на Ozon;
* `active: false` — позиция полностью исключается из синхронизации.

---

## Запуск

```bash
python main.py
```

Сервис выполнит прогон сразу при старте (`RUN_ON_STARTUP=true`), затем будет
повторять цикл каждые `SYNC_INTERVAL_MINUTES` минут.

### systemd (production)

```ini
[Unit]
Description=Multi-Marketplace Stock Sync v2.0
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/wb-api-stock-sync
ExecStart=/opt/wb-api-stock-sync/.venv/bin/python main.py
Restart=always
RestartSec=10
User=sync

[Install]
WantedBy=multi-user.target
```

---

## Тарифные сборки

**Вариант 1 — через `.env` (без правки кода):**

```dotenv
ENABLE_WB=true
ENABLE_OZON=false
```

**Вариант 2 — через код**: закомментировать соответствующий блок в
`run_sync_cycle()` внутри `main.py`. Клиенты не имеют общего состояния,
поэтому отключение одного не влияет на второй.

---

## Параметры `.env`

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `WB_API_TOKEN` | — | JWT-токен Wildberries (обязателен) |
| `WB_WAREHOUSE_ID` | — | ID склада продавца WB |
| `OZON_CLIENT_ID` | — | Заголовок `Client-Id` |
| `OZON_API_KEY` | — | Заголовок `Api-Key` |
| `OZON_WAREHOUSE_ID` | пусто | ID склада Ozon (опционально) |
| `ENABLE_WB` / `ENABLE_OZON` | `true` | Включение площадок |
| `BATCH_SIZE` | `100` | Размер пакета |
| `WB_REQUEST_DELAY` / `OZON_REQUEST_DELAY` | `1.0` / `0.8` | Пауза между батчами, сек |
| `WB_BACKOFF_BASE` / `OZON_BACKOFF_BASE` | `2.0` | База экспоненты |
| `WB_BACKOFF_MAX` / `OZON_BACKOFF_MAX` | `120.0` | Потолок задержки |
| `WB_MAX_RETRIES` / `OZON_MAX_RETRIES` | `5` | Число повторов |
| `REQUEST_TIMEOUT` | `30` | Таймаут HTTP, сек |
| `SYNC_INTERVAL_MINUTES` | `15` | Интервал планировщика |
| `RUN_ON_STARTUP` | `true` | Прогон при старте |
| `LOG_LEVEL` / `LOG_FILE` | `INFO` / `sync.log` | Логирование |

---

## Технический стек

Python 3.11+ · asyncio · aiohttp · Pandas · Pydantic v2 / pydantic-settings ·
APScheduler (AsyncIOScheduler) · logging

---

## Безопасность

* Секреты хранятся только в `.env`, который включён в `.gitignore`.
* Токены обёрнуты в `SecretStr` — не попадают в логи и трейсбеки.
* Тела ответов обрезаются до 500 символов перед записью в лог.

---

*Разработано в рамках комплексных решений автоматизации для e-commerce.
По вопросам кастомизации под индивидуальные учётные системы (1С, МойСклад,
Битрикс) — в личные сообщения.*
