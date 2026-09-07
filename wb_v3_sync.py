import os
import pandas as pd
import requests
from datetime import datetime
from typing import Optional
from apscheduler.schedulers.blocking import BlockingScheduler
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wb_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CSV_PATH = "sklad_stocks.csv"
WB_API_BASE_URL = "https://suppliers-api.wildberries.ru/api/v3/stocks"
WB_TOKEN = os.getenv("WB_API_TOKEN")
WB_WAREHOUSE_ID = os.getenv("WB_WAREHOUSE_ID", "1")

if not WB_TOKEN:
    raise ValueError("WB_API_TOKEN not found in environment variables")

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


def read_csv_stocks() -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(CSV_PATH, encoding='utf-8')

        if not all(col in df.columns for col in ['chrtId', 'quantity']):
            logger.error("CSV не содержит колонок 'chrtId' и 'quantity'")
            return None

        df = df.dropna(subset=['chrtId', 'quantity'])
        df['chrtId'] = df['chrtId'].astype(int)
        df['quantity'] = df['quantity'].astype(int)

        logger.info(f"✓ Прочитано {len(df)} размеров товаров из CSV")
        return df

    except FileNotFoundError:
        logger.error(f"Файл {CSV_PATH} не найден")
        return None
    except Exception as e:
        logger.error(f"Ошибка чтения CSV: {e}")
        return None


def update_wb_stocks() -> bool:
    df = read_csv_stocks()
    if df is None or len(df) == 0:
        logger.warning("Нет данных для загрузки")
        return False

    headers = {
        "Authorization": WB_TOKEN,
        "Content-Type": "application/json"
    }

    batch_size = 100
    total_success = 0
    total_failed = 0
    batch_num = 0

    for i in range(0, len(df), batch_size):
        batch_num += 1
        batch = df.iloc[i:i + batch_size].copy()

        payload = {
            "skus": batch[['chrtId', 'quantity']].rename(
                columns={'chrtId': 'sku', 'quantity': 'quantity'}
            ).to_dict('records')
        }

        url = f"{WB_API_BASE_URL}/{WB_WAREHOUSE_ID}"

        try:
            response = session.post(url, json=payload, headers=headers, timeout=10)

            if response.status_code == 200:
                logger.info(f"✓ Батч {batch_num}: {len(batch)} размеров отправлено")
                total_success += len(batch)
            elif response.status_code == 401:
                logger.error(f"✗ Ошибка аутентификации (401): проверьте WB_API_TOKEN и категорию доступа")
                total_failed += len(batch)
            elif response.status_code == 429:
                logger.warning(f"✗ Rate limit (429) на батче {batch_num}. Пауза 60 сек")
                time.sleep(60)
                total_failed += len(batch)
            elif response.status_code in [500, 502, 503, 504]:
                logger.warning(f"✗ Server error ({response.status_code}) на батче {batch_num}")
                total_failed += len(batch)
            elif response.status_code == 400:
                logger.error(f"✗ Ошибка 400: {response.text}. Проверьте структуру payload и наличие chrtId")
                total_failed += len(batch)
            elif response.status_code == 404:
                logger.error(f"✗ Ошибка 404: endpoint не найден. Проверьте warehouseId: {WB_WAREHOUSE_ID}")
                total_failed += len(batch)
            else:
                logger.error(f"✗ Неизвестная ошибка {response.status_code}: {response.text}")
                total_failed += len(batch)

        except requests.RequestException as e:
            logger.error(f"✗ Ошибка запроса батча {batch_num}: {e}")
            total_failed += len(batch)

        time.sleep(21)

    logger.info(f"Итого: {total_success} успешно, {total_failed} ошибок")
    return total_failed == 0


def job():
    logger.info("=" * 60)
    logger.info("Начало синхронизации остатков WB")
    update_wb_stocks()
    logger.info("Синхронизация завершена")
    logger.info("=" * 60)


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(job, 'interval', minutes=15, id='wb_sync_job')

    logger.info("WB Stock Sync запущен. Синхронизация каждые 15 минут.")
    logger.info(f"Используется warehouse ID: {WB_WAREHOUSE_ID}")
    logger.info("Для остановки нажмите Ctrl+C")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler остановлен пользователем")