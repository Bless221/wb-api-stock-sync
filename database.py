from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiosqlite

logger = logging.getLogger("database")

class DatabaseError(Exception):
    """Базовое исключение для ошибок базы данных."""
    pass

@dataclass(frozen=True)
class DBProduct:
    sku_internal: str
    wb_barcode: Optional[str]
    ozon_offer_id: Optional[str]
    active: bool

async def init_database(database_path: Path, mapping_path: Path) -> None:
    """Инициализирует схему БД, включаем WAL-режим и запускает миграцию данных."""
    database_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with aiosqlite.connect(database_path, isolation_level=None) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            await db.execute("PRAGMA busy_timeout=5000;")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku_internal TEXT UNIQUE NOT NULL,
                    wb_barcode TEXT,
                    ozon_offer_id TEXT,
                    active BOOLEAN DEFAULT 1
                );
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku_internal);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_products_wb ON products(wb_barcode) WHERE wb_barcode IS NOT NULL;")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_products_ozon ON products(ozon_offer_id) WHERE ozon_offer_id IS NOT NULL;")
        logger.info("Database schema initialized successfully (WAL mode enabled): %s", database_path)
        await _sync_from_json(database_path, mapping_path)
    except Exception as exc:
        logger.error("Failed to initialize database: %s", exc)
        raise DatabaseError(f"Database initialization failed: {exc}") from exc

async def _sync_from_json(database_path: Path, mapping_path: Path) -> None:
    """Синхронизирует данные из mapping.json в SQLite."""
    if not mapping_path.exists():
        logger.warning("Mapping source file not found at %s. Skipping synchronization.", mapping_path)
        return
    try:
        with open(mapping_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("items", [])
        if not items:
            logger.warning("No items found in mapping.json")
            return
        payload = [
            (
                str(item["sku_internal"]).strip(),
                str(item["wb_barcode"]).strip() if item.get("wb_barcode") else None,
                str(item["ozon_offer_id"]).strip() if item.get("ozon_offer_id") else None,
                bool(item.get("active", True))
            )
            for item in items
        ]
        async with aiosqlite.connect(database_path, isolation_level=None) as db:
            async with db.in_transaction():
                await db.execute("DELETE FROM products;")
                await db.executemany("""
                    INSERT INTO products (sku_internal, wb_barcode, ozon_offer_id, active)
                    VALUES (?, ?, ?, ?);
                """, payload)
        logger.info("Successfully synchronized %d products from mapping.json into SQLite repository", len(payload))
    except Exception as exc:
        logger.error("Synchronization with mapping.json failed: %s", exc)
        raise DatabaseError(f"Sync failed: {exc}") from exc

async def get_all_products(database_path: Path) -> list[DBProduct]:
    products = []
    try:
        async with aiosqlite.connect(database_path, isolation_level=None) as db:
            async with db.execute(
                "SELECT sku_internal, wb_barcode, ozon_offer_id, active FROM products WHERE active = 1;"
            ) as cursor:
                async for row in cursor:
                    products.append(DBProduct(
                        sku_internal=str(row[0]),
                        wb_barcode=str(row[1]) if row[1] else None,
                        ozon_offer_id=str(row[2]) if row[2] else None,
                        active=bool(row[3])
                    ))
        return products
    except Exception as exc:
        logger.error("Failed to fetch products grid cache from SQLite: %s", exc)
        raise DatabaseError(f"Failed to fetch products: {exc}") from exc
