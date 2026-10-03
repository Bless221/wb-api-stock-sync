from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any, Optional

import aiosqlite

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    pass


async def init_database(database_path: Path, mapping_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        async with aiosqlite.connect(database_path) as db:
            db.isolation_level = None
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            await db.execute("PRAGMA busy_timeout=5000;")

            await db.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku_internal TEXT NOT NULL UNIQUE,
                    title TEXT DEFAULT '',
                    wb_barcode TEXT,
                    ozon_offer_id TEXT,
                    active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await db.execute("CREATE INDEX IF NOT EXISTS idx_sku_internal ON products (sku_internal)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_wb_barcode ON products (wb_barcode)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_ozon_offer_id ON products (ozon_offer_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_active ON products (active)")

        logger.info("Database schema initialized successfully (WAL mode enabled): %s", database_path)
        await _sync_from_json(database_path, mapping_path)

    except Exception as exc:
        logger.exception("Failed to initialize database")
        raise DatabaseError(f"Database initialization failed: {exc}") from exc


async def _sync_from_json(database_path: Path, mapping_path: Path) -> None:
    """Синхронизирует данные из mapping.json в SQLite с использованием временных стейджинг-таблиц."""
    if not mapping_path.exists():
        logger.debug("mapping.json not found, skipping sync")
        return

    logger.info("Synchronizing SQLite database with mapping.json (Source of Truth)...")

    try:
        mapping_data = json.loads(mapping_path.read_text(encoding="utf-8"))
        items = mapping_data.get("items") if isinstance(mapping_data, dict) else mapping_data

        if not isinstance(items, list) or not items:
            logger.warning("Invalid or empty mapping.json structure, skipping sync")
            return

        active_skus: list[str] = []
        upsert_payload: list[tuple[str, str, Optional[str], Optional[str], bool]] = []

        for index, item in enumerate(items):
            if not isinstance(item, dict):
                continue

            sku_internal = str(item.get("sku_internal", "")).strip()
            if not sku_internal:
                logger.warning("Item #%d has empty sku_internal, skipping", index)
                continue

            active_skus.append(sku_internal)
            title = str(item.get("title", "")).strip()
            
            wb_barcode = item.get("wb_barcode")
            wb_barcode = str(wb_barcode).strip() if wb_barcode is not None else None
            
            ozon_offer_id = item.get("ozon_offer_id")
            ozon_offer_id = str(ozon_offer_id).strip() if ozon_offer_id is not None else None
            
            active = bool(item.get("active", True))

            upsert_payload.append((sku_internal, title, wb_barcode, ozon_offer_id, active))

        async with aiosqlite.connect(database_path) as db:
            async with db.transaction():
                # 1. Массовая вставка/обновление всех позиций (Bulk Upsert)
                await db.executemany(
                    """
                    INSERT OR REPLACE INTO products 
                    (sku_internal, title, wb_barcode, ozon_offer_id, active, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    upsert_payload,
                )

                # 2. Безопасное каскадное отключение удаленных SKU через архитектурный паттерн Staging Table
                if active_skus:
                    await db.execute("CREATE TEMPORARY TABLE IF NOT EXISTS temp_active_skus (sku TEXT);")
                    await db.execute("DELETE FROM temp_active_skus;")
                    await db.executemany("INSERT INTO temp_active_skus (sku) VALUES (?);", [(sku,) for sku in active_skus])
                    await db.execute("""
                        UPDATE products 
                        SET active = 0 
                        WHERE sku_internal NOT IN (SELECT sku FROM temp_active_skus);
                    """)
                    await db.execute("DROP TABLE temp_active_skus;")

        backup_path = mapping_path.with_suffix(".json.bak")
        shutil.copy2(mapping_path, backup_path)
        logger.info("Successfully synced %d items via temporary staging table. Backup created: %s", len(upsert_payload), backup_path)

    except Exception as exc:
        logger.exception("Synchronization with mapping.json failed")
        raise DatabaseError(f"Sync failed: {exc}") from exc


async def get_all_products(database_path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    try:
        async with aiosqlite.connect(database_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM products") as cursor:
                rows = await cursor.fetchall()
                for row in rows:
                    sku_internal = row["sku_internal"]
                    result[sku_internal] = {
                        "id": row["id"],
                        "sku_internal": row["sku_internal"],
                        "title": row["title"],
                        "wb_barcode": row["wb_barcode"],
                        "ozon_offer_id": row["ozon_offer_id"],
                        "active": bool(row["active"]),
                    }
    except Exception as exc:
        logger.exception("Failed to load products from database")
        raise DatabaseError(f"Failed to load products: {exc}") from exc
    return result


async def get_product_by_sku(database_path: Path, sku_internal: str) -> Optional[dict[str, Any]]:
    try:
        async with aiosqlite.connect(database_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM products WHERE sku_internal = ? AND active = 1",
                (sku_internal,),
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        "id": row["id"],
                        "sku_internal": row["sku_internal"],
                        "title": row["title"],
                        "wb_barcode": row["wb_barcode"],
                        "ozon_offer_id": row["ozon_offer_id"],
                        "active": bool(row["active"]),
                    }
                return None
    except Exception as exc:
        logger.exception("Failed to fetch product by SKU: %s", sku_internal)
        raise DatabaseError(f"Database lookup failed: {exc}") from exc


async def upsert_product(
        database_path: Path,
        sku_internal: str,
        title: str = "",
        wb_barcode: Optional[str] = None,
        ozon_offer_id: Optional[str] = None,
        active: bool = True,
) -> None:

    try:
        async with aiosqlite.connect(database_path) as db:
            async with db.transaction():
                await db.execute(
                    """
                    INSERT OR REPLACE INTO products 
                    (sku_internal, title, wb_barcode, ozon_offer_id, active, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (sku_internal, title, wb_barcode, ozon_offer_id, active),
                )
    except Exception as exc:
        logger.exception("Failed to upsert product: %s", sku_internal)
        raise DatabaseError(f"Database upsert failed: {exc}") from exc
