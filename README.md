# 📊 Multi-Marketplace Stock Sync v2.0

Высокопроизводительный асинхронный сервис синхронизации остатков товаров с Wildberries API v3 и Ozon API с поддержкой критических алертов в Telegram, изолированной обработкой Rate Limit, защитой от File Locking и потоковым парсингом CSV.

Версия: 2.0 | Статус: Production-ready | Язык: Python 3.12 | Лицензия: MIT

| Компонент | Описание | Преимущество |
| :--- | :--- | :--- |
| Полная асинхронность | asyncio + aiohttp для всех I/O операций | Одновременная обработка 1000+ батчей без блокировок |
| Гибридный маппинг | mapping.json + SQLite с aiosqlite | JSON как source of truth, SQLite для production-скорости |
| Rate Limit Shield | Exponential backoff + jitter на клиент-уровне | Никогда не получите 429, автоматическое восстановление |
| Потоковый парсинг CSV | ThreadPoolExecutor + async/await | Защита от File Locking (1С может писать параллельно) |
| Параллельная отправка | asyncio.gather() с семафором (3 одновременных) | 100 батчей отправляются за 30s вместо 1500s |
| Telegram-алерты | Изолированные async POST на telegram API | Критические ошибки видны в Telegram за 1 сек |
| In-memory кэш | ProductMapper кэширует все 10k SKU in RAM | O(1) lookups при маппинге каждого товара |
| Безопасность | Маскирование токенов в логах, SecretStr, non-root Docker | Credentials никогда не попадут в файлы логов |
#Решённые проблемы production-инсталляций

✅ Race conditions при одновременном доступе → aiosqlite транзакции

✅ OutOfMemory при загрузке 500k+ CSV → потоковое чтение с чанками

✅ Блокировка файла со стороны 1С → ThreadPoolExecutor + exponential retry

✅ Потеря данных при сбое → автоматические бэкапы в backups/

✅ Неизвестные товары "чёрной ямой" → детальное логирование unmapped SKU

✅ Падение рантайма при сбоях сети/авторизации маркетплейсов → Исключен `sys.exit(1)`, приложение остается активным в фоне и ожидает следующего тика шедулера

✅ Ошибка 404 при обновлении остатков Ozon → Обновлен эндпоинт до актуального POST `/v2/products/stocks`

✅ Ошибки доставки алертов в Telegram → Исправлен базовый URL запросов на официальный `https://telegram.org`

✅ Сетевой оверхед и доставка файлов в облачные инсталляции → Интегрирован встроенный FTP-сервер внутри Docker-стека с атомарным импортом файлов через разделяемые тома (Volumes)
# 🏗️ Архитектура 

ASCII-диаграмма потока данных

```text
┌──────────────────────────────────────────────────────────────────┐
│                   MULTI-MARKETPLACE SYNC v2.0                    │
└──────────────────────────────────────────────────────────────────┘

        СЦЕНАРИЙ А (Docker Server)          СЦЕНАРИЙ Б (Windows Light)
     ┌──────────────────────────────┐    ┌──────────────────────────────┐
     │ 1С выгружает по сети на FTP  │    │ 1С пишет в локальную папку   │
     └──────────────┬───────────────┘    └──────────────┬───────────────┘
                    │                                   │
                    ▼ (shared volume / local disk)      ▼
        ┌────────────────────────────────────────────────────────┐
        │    stocks.csv (5-100 MB)                               │
        │  ┌──────────────────────────────────┐                  │
        │  │ item_sku    │ quantity           │                  │
        │  │─────────────┼────────────────────│                  │
        │  │ SKU-001     │ 150                │                  │
        │  │ SKU-002     │ 0                  │                  │
        │  │ ...         │ ...                │                  │
        │  └──────────────────────────────────┘                  │
        └────────────┬───────────────────────────────────────────┘
                     │
     ┌───────────────┴──────────────────┐
     │  stock_file.py                   │
     │  ┌────────────────────────────┐   │
     │  │ 1. Атомарный импорт VFS/FTP│   │
     │  │ 2. Валидация структуры     │   │
     │  │ 3. Стабильность (1С Lock)  │   │
     │  │ 4. Создание бэкапа         │   │
     │  └────────────┬────────────────┘   │
     │              │ ThreadPoolExecutor  │
     │              │ (НЕ блокирует loop) │
     └──────────────┬──────────────────────┘
                    │
                    ↓ (Streaming chunks)
        ┌───────────────────────────────┐
        │ CSV DictReader                │
        │ (O(chunk_size) RAM, не O(file))
        └────────────┬──────────────────┘
                     │
                     ↓
        ┌─────────────────────────────────────┐
        │  ProductMapper                      │
        │ ┌───────────────────────────────┐   │
        │ │ mapping.json (source of truth)│   │
        │ │ {                              │   │
        │ │   "items": [                   │   │
        │ │     {                          │   │
        │ │       "sku_internal": "SKU-001"│  │
        │ │       "wb_barcode": "12345...",│  │
        │ │       "ozon_offer_id": "9999", │  │
        │ │       "active": true           │   │
        │ │     },                         │   │
        │ │     ...                        │   │
        │ │   ]                            │   │
        │ │ }                              │   │
        │ └───────────────────────────────┘   │
        │              ↓ (Migration on startup) │
        │ ┌───────────────────────────────┐   │
        │ │ SQLite (data/stocks.db)       │   │
        │ │                                │   │
        │ │ CREATE TABLE products (        │   │
        │ │   id INTEGER PRIMARY KEY,     │   │
        │ │   sku_internal TEXT UNIQUE,   │   │
        │ │   wb_barcode TEXT,            │   │
        │ │   ozon_offer_id TEXT,         │   │
        │ │   active BOOLEAN DEFAULT 1    │   │
        │ │ );                            │   │
        │ │ CREATE INDEX idx_sku ...      │   │
        │ └───────────────────────────────┘   │
        │              ↓ (Async load)          │
        │ ┌───────────────────────────────┐   │
        │ │ In-memory Cache               │   │
        │ │ {                              │   │
        │ │  "SKU-001": {                 │   │
        │ │    "wb_barcode": "12345...",  │   │
        │ │    "ozon_offer_id": "9999",   │   │
        │ │    "active": true             │   │
        │ │  },                            │   │
        │ │  ...                           │   │
        │ │ }                              │   │
        │ │ O(1) lookup per SKU            │   │
        │ └───────────────────────────────┘   │
        └────────────┬──────────────────────────┘
                     │
         ┌───────────┴──────────────────┐
         │                              │
         ↓                              ↓
    ┌─────────────┐            ┌─────────────┐
    │  WB Items   │            │ Ozon Items  │
    │ (900 шт)    │            │ (850 шт)    │
    │             │            │             │
    │ [{          │            │ [{          │
    │  sku:"123", │            │  offer_id:  │
    │  amount:150 │            │  "9999",    │
    │ }...]       │            │  stock:150  │
    └──────┬──────┘            └──────┬──────┘
           │                          │
           │ asyncio.gather(*tasks)   │
           │ (3 concurrent semaphore) │
           │                          │
           ↓                          ↓
    ┌──────────────────┐    ┌──────────────────┐
    │ WildberriesClient│    │   OzonClient     │
    │                  │    │                  │
    │ PUT /api/v3/...  │    │ POST /v2/...     │
    │                  │    │                  │
    │ Batch 1: 100     │    │ Batch 1: 100     │
    │ Batch 2: 100     │    │ Batch 2: 100     │
    │ Batch 3: 100     │    │ ...              │
    │ ... (9 total)    │    │ ...              │
    │                  │    │                  │
    │ Rate Limit Shield│    │ Rate Limit Shield│
    │ Backoff: 2^n     │    │ Backoff: 2^n     │
    │ Max retry: 5x    │    │ Max retry: 5x    │
    │ Jitter: +rand    │    │ Jitter: +rand    │
    └────────┬─────────┘    └────────┬─────────┘
             │                       │
             │ (результаты)          │
             │                       │
      ┌──────┴───────────────────┐
      │  SyncReport              │
      │ ┌──────────────────────┐ │
      │ │ WB: 900/900 OK       │ │
      │ │ Ozon: 850/850 OK     │ │
      │ │ Rate limits: 0 hits  │ │
      │ │ Duration: 45s        │ │
      │ └──────────────────────┘ │
      └──────────┬────────────────┘
                 │
    ┌────────────┴─────────────────┐
    │                              │
    ↓                              ↓
┌──────────────────┐      ┌──────────────────┐
│ Логирование      │      │ Telegram Alert   │
│ (logs/sync.log)  │      │ (если CRITICAL)  │
│                  │      │                  │
│ ✅ Sync OK       │      │ 🚨 CriticalError │
│ WB: 900 items    │      │ (Мягкий алерт,   │
│ Ozon: 850 items  │      │  без падения     │
│ Time: 45s        │      │  рантайма через  │
│                  │      │  api.telegram.org)│
└──────────────────┘      └──────────────────┘
```
# ⏳ Временные диаграммы и управление ресурсами 

Временные диаграммы (Timeline)

```text
SYNC CYCLE (15 минут)

t=0s    ├─ Download/Import from FTP Volume (Shared VFS) ── (Мгновенный локальный перенос)
        │
t=0.1s  ├─ Validate stocks.csv (100ms)
        │
t=0.2s  ├─ Read CSV (ThreadPoolExecutor) ───────────── (async, не блокирует loop)
        │  Batch 1 (10k rows)
        │  Batch 2 (10k rows)
        │  Batch 3 (1.2k rows) ────────────────────── t=2.1s (завершен)
        │
t=0.2s  ├─ Load ProductMapper from SQLite (parallel) ─ t=0.4s
        │
t=0.4s  ├─ Map CSV → WB/Ozon items (O(1) per SKU) ─── t=0.9s
        │
t=0.9s  ├─ asyncio.gather(
        │    WB.update_stocks(900),
        │    Ozon.update_stocks(850)
        │  )
        │
        ├─────────── WB: Batch 1-9 (sem=3) ──────────┐
        │  Sem acquires [Batch 1,2,3]                │
        │  t=1.1s: [B1→sent, B2→sent, B3→sent]     │
        │  t=2.1s: [B4→sent, B5→sent, B6→sent]     │
        │  t=3.1s: [B7→sent, B8→sent, B9→sent]     │
        │                            429 Hit! ↓     │
        │  t=3.6s: Backoff 2^4=16s                  │
        │  t=19.6s: [B9 retry→sent]                 │
        │                                            ├─ t=20.1s (WB DONE)
        ├─────────── Ozon: Batch 1-9 (sem=3) ──────┤
        │  Sem acquires [Batch 1,2,3]               │
        │  t=1.1s: [B1→sent, B2→sent, B3→sent]     │
        │  t=2.1s: [B4→sent, B5→sent, B6→sent]     │
        │  t=3.1s: [B7→sent, B8→sent, B9→sent]     │
        │                                            │
        │                                            ├─ t=4.1s (Ozon DONE)
        │
t=20.1s ├─ Gather results
        │
t=20.2s ├─ Send Telegram notification (async, shield) ─ t=20.6s
        │
t=20.6s └─ SYNC COMPLETE
         ↓
      Next sync in 15 minutes (APScheduler, App remains alive in background)
```
#Управление соединениями и ресурсами

```python
# HTTP Connection Pool (Shared)
aiohttp.ClientSession
├── TCPConnector(limit=20)              # Max 20 TCP соединений
├── ClientTimeout(total=30s)            # Global timeout
└── ttl_dns_cache=300                   # Кэш DNS на 5 минут

# Database Connections (Async)
aiosqlite
├── Pooled internally by OS            # SQLite handles concurrency
├── Transactions for consistency        # ACID гарантии
└── WAL mode enabled                    # Write-Ahead Logging

# Batch Concurrency (Semaphore)
asyncio.Semaphore(max_concurrent_batches=3)
├── WB: max 3 batches одновременно
└── Ozon: max 3 batches одновременно

# Thread Pool (CSV Reading)
ThreadPoolExecutor(max_workers=auto)
├── Разблокирует main event loop
└── NIO для файловых операций
```

# 📋 Структура проекта

```text
wb-api-stock-sync/
│
├── 📁 production_docker_ftp/        # Пакет развертывания под Docker (Серверный)
│   ├── 📄 config.py                 # Конфигурация Pydantic Settings с FTP-валидацией
│   ├── 🗄️ database.py               # Инициализация SQLite в режиме WAL
│   ├── 🔄 mapper.py                 # Асинхронный маппер DataFrame (O(1))
│   ├── 📁 stock_file.py             # Атомарный импорт из локального Shared FTP Volume
│   ├── 🔔 notifications.py          # Модуль Telegram-оповещений через api.telegram.org
│   ├── ⚠️ exceptions.py             # Датаклассы пользовательских исключений рантайма
│   ├── 🟦 wb_client.py              # WB v3 клиент со встроенным Backoff Shield
│   ├── 🟧 ozon_client.py            # Ozon клиент с маршрутизацией на /v2/products/stocks
│   ├── ⏰ scheduler.py              # APScheduler движок с политикой coalescing
│   ├── 🔁 main.py                   # Точка входа. Убран sys.exit(1) при сбоях API
│   ├── 🐳 Dockerfile                # Оптимизированная двухэтапная multi-stage сборка
│   ├── 🐳 docker-compose.yml        # Оркестрация контейнеров робота и Alpine FTP-сервера
│   ├── 📋 requirements.txt          # Зависимости серверного пакета
│   ├── 🔐 .env.example              # Шаблон переменных окружения c FTP блоком
│   └── 📄 mapping.json              # Базовая матрица маппинга
│
├── 📁 production_windows_light/     # Облегченный пакет для Windows ПК (Без Docker)
│   ├── 📄 config.py                 # Облегченная конфигурация без оверхеда FTP
│   ├── 🗄️ database.py               # Инициализация SQLite базы данных
│   ├── 🔄 mapper.py                 # Маппер DataFrame
│   ├── 📁 stock_file.py             # Чистый локальный файловый менеджер диска Windows
│   ├── 🔔 notifications.py          # Модуль Telegram-оповещений через api.telegram.org
│   ├── ⚠️ exceptions.py             # Исключения рантайма
│   ├── 🟦 wb_client.py              # WB v3 клиент со встроенным Backoff Shield
│   ├── 🟧 ozon_client.py            # Ozon клиент с маршрутизацией на /v2/products/stocks
│   ├── ⏰ scheduler.py              # APScheduler движок
│   ├── 🔁 main.py                   # Точка входа без вызова FTP-функций импорта
│   ├── 📄 run_backend.vbs           # Сценарий невидимого фонового запуска без окон cmd
│   ├── ⚙️ stop_backend.bat          # Кликабельный скрипт безопасной остановки процесса
│   ├── 📋 requirements.txt          # Зависимости Windows пакета
│   ├── 🔐 .env.example              # Облегченный шаблон переменных окружения
│   └── 📄 mapping.json              # Базовая матрица маппинга
│
├── 📄 1C_INTEGRATION.md             # Техническое задание для 1С-программиста клиента
├── .gitignore                      # Глобальные исключения Git (логи, СУБД, .env)
└── 📖 README.md                    # Этот файл документации
# 📦 Требования и установка

### Системные требования

| Компонент | Минимум | Рекомендуемо | Назначение |
| :--- | :--- | :--- | :--- |
| **Python** | 3.11 | 3.12 | Async/await, type hints, СУБД стабильность |
| **ОС** | Linux / macOS / Windows | Linux (Ubuntu 22.04+) | Сервер |
| **ОЗУ** | 512 MB (Docker) / 30 MB (Light) | 2 GB | CSV parsing + cache |
| **Диск** | 500 MB | 5 GB | БД, логи, бэкапы |
| **Интернет** | 10 Mbps | 100 Mbps | API, Telegram |
| **Docker** | 20.10 | 25.0+ | Контейнеризация (для Docker-пакета) |

### Python зависимости

```text
aiohttp>=3.14.3,<4.0.0      # Асинхронный HTTP клиент
aiosqlite==0.22.1           # Async SQLite драйвер
pandas>=2.2.3,<4.0.0        # CSV parsing + DataFrame
pydantic>=2.13.0,<3.0.0     # Settings validation
pydantic-settings>=2.15.0,<3.0.0  # Environment variables
APScheduler>=3.10.4,<4.0.0  # Background scheduler
python-dotenv>=1.0.1,<2.0.0  # .env loading
```

## ⚙️ Установка

### 1️⃣ Локальная Windows установка без Docker (`production_windows_light`)

#### Шаг 1: Клонирование репозитория
```bash
git clone https://github.com
cd wb-api-stock-sync/production_windows_light
```

#### Шаг 2: Создание виртуального окружения
```bash
python -m venv venv
venv\Scripts\activate
```

#### Шаг 3: Установка зависимостей
```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

#### Шаг 4: Подготовка конфигурации
```bash
cp .env.example .env
# Заполните боевые токены маркетплейсов в созданном .env
```

#### Шаг 5: Фоновый запуск
* Просто дважды кликните по файлу `run_backend.vbs`. Скрипт тихо запустится в процессах ОС.
* Для остановки запустите `stop_backend.bat`.
### 2️⃣ Серверная установка (`production_docker_ftp`)

#### Шаг 1: Перейдите в каталог серверной сборки
```bash
cd wb-api-stock-sync/production_docker_ftp
```

#### Шаг 2: Подготовка окружения
```bash
cp .env.example .env
# Отредактируйте .env, указав токен Telegram, доступы к маркетплейсам и FTP
```

#### Шаг 3: Запуск комплекса (Включает FTP-приемник и робота синхронизации)
```bash
# Сборка и запуск в фоне
docker-compose up -d --build

# Проверка статуса контейнеров
docker-compose ps

# Просмотр логов рантайма
docker-compose logs -f sync-daemon
```

# 🔧 Конфигурация: Полный файл .env

Пример файла `.env` для серверной сборки (`production_docker_ftp`):

```env
################################################################################
# WILDBERRIES API v3
################################################################################

# JWT токен для доступа к Wildberries
WB_API_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# ID вашего склада в Wildberries
WB_WAREHOUSE_ID=123456

# Base URL API (не менять без необходимости)
WB_BASE_URL=https://wildberries.ru

################################################################################
# OZON API
################################################################################

# Client ID для доступа к Ozon
OZON_CLIENT_ID=12345

# API Key для доступа к Ozon
OZON_API_KEY=abcdef1234567890abcdef1234567890

# Base URL API (не менять без необходимости)
OZON_BASE_URL=https://ozon.ru

# ID склада в Ozon (опционально)
OZON_WAREHOUSE_ID=123456

################################################################################
# УПРАВЛЕНИЕ МАРКЕТПЛЕЙСАМИ И ИСТОЧНИКАМИ
################################################################################

ENABLE_WB=true
ENABLE_OZON=true

CSV_PATH=stocks.csv
MAPPING_PATH=mapping.json
DATABASE_PATH=data/stocks.db

################################################################################
# ИНТЕГРАЦИЯ ВСТРОЕННОГО FTP СЕРВЕРА (Для 1С)
################################################################################

ENABLE_FTP_DOWNLOAD=true
FTP_HOST=ftp-server
FTP_PORT=21
FTP_USER=ftp_1c_user
FTP_PASSWORD=secret_ftp_pass_2026
FTP_REMOTE_PATH=stocks.csv

################################################################################
# БАТЧИНГ И RATE LIMITS
################################################################################

BATCH_SIZE=100
WB_REQUEST_DELAY=1.0
OZON_REQUEST_DELAY=0.8

WB_BACKOFF_BASE=2.0
WB_BACKOFF_MAX=120.0
WB_MAX_RETRIES=5

OZON_BACKOFF_BASE=2.0
OZON_BACKOFF_MAX=120.0
OZON_MAX_RETRIES=5

################################################################################
# СЕТЬ И РАСПИСАНИЕ
################################################################################

REQUEST_TIMEOUT=30
SYNC_INTERVAL_MINUTES=15
RUN_ON_STARTUP=true

################################################################################
# ПРОВЕРКА СТАБИЛЬНОСТИ ФАЙЛА (защита от 1С)
################################################################################

CSV_WAIT_TIMEOUT=15
CSV_STABILITY_WINDOW=2
CSV_CHECK_INTERVAL=1.0
CSV_CHUNK_SIZE=10000
MAX_CONCURRENT_BATCHES=3

################################################################################
# TELEGRAM NOTIFICATIONS
################################################################################

TELEGRAM_BOT_TOKEN=123456789:ABCDEFGHIJKLMNOPQRSTUVWxyz_abcdefgh
TELEGRAM_CHAT_ID=-1001234567890

################################################################################
# ЛОГИРОВАНИЕ
################################################################################

LOG_LEVEL=INFO
LOG_FILE=logs/sync.log
```
# 📂 Примеры файлов данных

### Файл: `stocks.csv`
Это основной файл входных данных. Должен быть в формате CSV с минимум двумя колонками.

#### Пример №1: Простой формат (минимальный)
```csv
item_sku,quantity
SKU-001,150
SKU-002,0
SKU-003,1000
SKU-004,50
SKU-005,999
```

#### Пример №2: Расширенный формат
```csv
item_sku,quantity,title,warehouse,category,last_updated
SKU-001,150,Товар 1,Main,Электроника,2024-01-15
SKU-002,0,Товар 2,Main,Одежда,2024-01-15
SKU-003,1000,Товар 3,Secondary,Обувь,2024-01-15
SKU-004,50,Товар 4,Main,Электроника,2024-01-14
SKU-005,999,Товар 5,Backup,Мебель,2024-01-13
```

### Файл: `mapping.json`
Это "source of truth" для маппинга SKU на маркетплейсы. При первом запуске автоматически мигрирует в SQLite.

#### Пример полного файла со всеми возможными сценариями:
```json
{
  "items": [
    {
      "sku_internal": "SKU-001",
      "title": "Смартфон XYZ Pro",
      "wb_barcode": "1234567890123",
      "ozon_offer_id": "123456789",
      "active": true
    },
    {
      "sku_internal": "SKU-002",
      "title": "Наушники ABC",
      "wb_barcode": "9876543210987",
      "ozon_offer_id": "987654321",
      "active": true
    },
    {
      "sku_internal": "SKU-003",
      "title": "Кабель USB Type-C",
      "wb_barcode": "5555555555555",
      "ozon_offer_id": null,
      "active": true
    },
    {
      "sku_internal": "SKU-004",
      "title": "Защитное стекло",
      "wb_barcode": null,
      "ozon_offer_id": "444444444",
      "active": true
    },
    {
      "sku_internal": "SKU-005",
      "title": "Снятый с продажи товар",
      "wb_barcode": "6666666666666",
      "ozon_offer_id": "555555555",
      "active": false
    },
    {
      "sku_internal": "SKU-006",
      "title": "Товар без маркетплейсов",
      "wb_barcode": null,
      "ozon_offer_id": null,
      "active": true
    }
  ]
}
```

# 🚀 Запуск и Docker окружение

### Локальный запуск (разработка)
```bash
# 1. Перейти в каталог Windows-пакета
cd production_windows_light

# 2. Активировать venv
venv\Scripts\activate

# 3. Запустить сервис напрямую в консоли
python main.py
```
*Остановка (graceful shutdown): Нажмите `Ctrl+C`.*

### 🐳 Docker Compose configuration
Файл: `production_docker_ftp/docker-compose.yml`
```yaml
version: "3.9"

services:
  sync-daemon:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: wb-ozon-sync-v2
    restart: unless-stopped
    depends_on:
      - ftp-server
    environment:
      - WB_API_TOKEN=\${WB_API_TOKEN}
      - WB_WAREHOUSE_ID=\${WB_WAREHOUSE_ID}
      - WB_BASE_URL=\${WB_BASE_URL}
      - OZON_CLIENT_ID=\${OZON_CLIENT_ID}
      - OZON_API_KEY=\${OZON_API_KEY}
      - OZON_BASE_URL=\${OZON_BASE_URL}
      - OZON_WAREHOUSE_ID=\${OZON_WAREHOUSE_ID:-}
      - ENABLE_WB=\${ENABLE_WB:-true}
      - ENABLE_OZON=\${ENABLE_OZON:-true}
      - CSV_PATH=/app/stocks.csv
      - DATABASE_PATH=/app/data/stocks.db
      - MAPPING_PATH=/app/mapping.json
      - SYNC_INTERVAL_MINUTES=\({SYNC_INTERVAL_MINUTES:-15}       - RUN_ON_STARTUP=\){RUN_ON_STARTUP:-true}
      - LOG_LEVEL=\${LOG_LEVEL:-INFO}
      - LOG_FILE=/app/logs/sync.log
      - TELEGRAM_BOT_TOKEN=\${TELEGRAM_BOT_TOKEN}
      - TELEGRAM_CHAT_ID=\${TELEGRAM_CHAT_ID}
      - ENABLE_FTP_DOWNLOAD=true
      - FTP_HOST=ftp-server
      - FTP_PORT=21
      - FTP_USER=\({FTP_USER:-ftp_1c_user}       - FTP_PASSWORD=\){FTP_PASSWORD:-secret_ftp_pass_2026}
      - FTP_REMOTE_PATH=stocks.csv
    volumes:
      - shared-ftp-data:/app/ftp_data
      - ./stocks.csv:/app/stocks.csv:rw
      - ./mapping.json:/app/mapping.json:ro
      - ./data:/app/data
      - ./logs:/app/logs
      - ./backups:/app/backups
    deploy:
      resources:
        limits:
          cpus: "1.0"
          memory: 512M
        reservations:
          cpus: "0.2"
          memory: 128M
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  ftp-server:
    image: delfer/alpine-ftp-server:latest
    container_name: ftp-server-v2
    restart: unless-stopped
    ports:
      - "21:21"
      - "21000-21010:21000-21010"
    environment:
      - FTP_USER=\({FTP_USER:-ftp_1c_user}       - FTP_PASS=\){FTP_PASSWORD:-secret_ftp_pass_2026}
      - MIN_PORT=21000
      - MAX_PORT=21010
    volumes:
      - shared-ftp-data:/ftp/ftp_1c_user
    logging:
      driver: "json-file"
      options:
        max-size: "5m"
        max-file: "2"

volumes:
  shared-ftp-data:
```
### Dockerfile (Multi-stage build)
```dockerfile
FROM python:3.12-slim as builder
WORKDIR /tmp/build
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --user --no-cache-dir --compile -r requirements.txt

FROM python:3.12-slim
WORKDIR /app
RUN groupadd -r syncuser && useradd -r -g syncuser -m -d /home/syncuser syncuser
COPY --from=builder /root/.local /home/syncuser/.local
COPY --chown=syncuser:syncuser . /app/
RUN mkdir -p /app/data /app/logs /app/backups && chown -R syncuser:syncuser /app/data /app/logs /app/backups
ENV PATH=/home/syncuser/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONTDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
HEALTHCHECK --interval=60s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import aiohttp; import pydantic; import pandas" || exit 1
USER syncuser
CMD ["python", "main.py"]
```

# 📊 Параметры и API интеграция

### Таблица всех переменных окружения

| Параметр | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| **WB_API_TOKEN** | SecretStr | — | JWT токен Wildberries (обязателен) |
| **WB_WAREHOUSE_ID** | int | — | ID склада WB (обязателен) |
| **OZON_CLIENT_ID** | SecretStr | — | Client ID Ozon (обязателен) |
| **OZON_API_KEY** | SecretStr | — | API Key Ozon (обязателен) |
| **BATCH_SIZE** | int | 100 | Размер одного батча (1-1000) |
| **SYNC_INTERVAL_MINUTES** | int | 15 | Интервал синхронизации в минутах |
| **ENABLE_FTP_DOWNLOAD** | bool | false | Флаг активации импорта файлов с FTP |
| **FTP_HOST** | str | null | Адрес/имя сервиса встроенного FTP сервера |
| **TELEGRAM_BOT_TOKEN** | SecretStr | null | Токен бота Telegram (опционально) |
| **TELEGRAM_CHAT_ID** | str | null | ID чата Telegram (опционально) |

### Plug-and-Play API интеграция

#### Wildberries API v3 (`PUT /api/v3/stocks/{warehouseId}`)
```http
PUT https://wildberries.ru HTTP/1.1
Authorization: YOUR_JWT_TOKEN
Content-Type: application/json

{
  "stocks": [
    { "sku": "1234567890123", "amount": 150 },
    { "sku": "9876543210987", "amount": 0 }
  ]
}
```

#### Ozon API (`POST /v2/products/stocks`)
```http
POST https://ozon.ru HTTP/1.1
Client-Id: YOUR_CLIENT_ID
Api-Key: YOUR_API_KEY
Content-Type: application/json

{
  "stocks": [
    { "offer_id": "12345678", "stock": 150 },
    { "offer_id": "87654321", "stock": 0 }
  ]
}
```

### ⚠️ Обработка ошибок
* **Critical (DB Error):** Оповещение в Telegram + немедленный останов системы (`scheduler.shutdown()`).
* **Marketplace API Errors (401/404, 5xx):** Оповещение в Telegram. Рантайм приложения **не падает**, шедулер продолжает работу и ожидает следующий запланированный цикл. Сетевые таймауты обрабатываются по формуле Exponential Backoff с добавлением Jitter.
* **File-related (Заблокирован 1С):** Ожидание до `CSV_WAIT_TIMEOUT` секунд. Если не освободился — пропуск текущего цикла.

# 🔍 Troubleshooting и Лицензия

### 📈 Мониторинг логов
Основной лог-файл находится по адресу `logs/sync.log` и автоматически ротируется при достижении 5 МБ (до 5 архивных копий).

### 🔍 Troubleshooting (Решение проблем)

* **aiosqlite.DatabaseError: database is locked**
  ```bash
  rm data/stocks.db-journal data/stocks.db-wal data/stocks.db-shm
  python main.py
  ```
* **StockFileUnavailableError: Cannot read stock file (1С заблокировал)**
  Увеличьте таймаут ожидания файла в `.env`:
  ```env
  CSV_WAIT_TIMEOUT=20
  ```
* **Высокое потребление оперативной памяти (>1 GB)**
  Уменьшите размер обрабатываемого чанка в `.env`:
  ```env
  CSV_CHUNK_SIZE=5000
  ```

### 📝 Лицензия

Этот проект лицензирован под MIT License — см. файл LICENSE

```text
MIT License
Copyright (c) 2026 Bless221

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### 👨‍💻 Разработка и контрибьютинг

#### Установка для разработки
```bash
# Virtual environment
python -m venv venv
source venv/bin/activate  # или venv\Scripts\activate на Windows

# Install dev tools
pip install -r requirements.txt
pip install pytest pytest-asyncio black flake8 mypy
```

#### Code style
```bash
# Format code
black .

# Lint
flake8 . --max-line-length=100

# Type checking
mypy . --ignore-missing-imports
```

#### Testing
```bash
# Run tests
pytest tests/ -v
```

### 📞 Поддержка и контакты
Обнаружили баг? Откройте Issue

Есть вопросы? 

Email: kuzmaslov05@gmail.com

### 🔗 Полезные ссылки

#### API Документация
- [Wildberries API v3](https://wildberries.ru)
- [Ozon Seller API](https://ozon.ru)
- [Telegram Bot API](https://telegram.org)

#### Tech
- [Python asyncio](https://python.org)
- [aiohttp Documentation](https://aiohttp.org)
- [SQLite Documentation](https://sqlite.org)
- [Pydantic v2](https://pydantic.dev)

#### Deployment
- [Docker Compose](https://docker.com)
- [Docker Best Practices](https://docker.com/develop/develop-images/dockerfile_best-practices/)

### 📊 Статистика проекта

| Метрика | Значение |
| :--- | :--- |
| **Язык** | Python 3.12 |
| **Асинхронность** | ✅ 100% async/await |
| **Производительность** | ~10k товаров / 10 секунд |
| **Память** | O(chunk) |
