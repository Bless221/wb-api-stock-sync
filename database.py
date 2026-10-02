from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any, Optional

import aiosqlite

logger = logging.getLogger(__name__)


class DatabaseError(Exception):


async def init_database(database_path: Path, mapping_path: Path) -> None:
    # Ensure parent directory exists
    database_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        async with aiosqlite.connect(database_path) as db:
            db.isolation_level = None  # autocommit mode

            # Create products table
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

            # Create indices for fast lookups
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_sku_internal 
                ON products (sku_internal)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_wb_barcode 
                ON products (wb_barcode)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_ozon_offer_id 
                ON products (ozon_offer_id)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_active 
                ON products (active)
            """)

            await db.commit()

        logger.info("Database schema initialized: %s", database_path)

        # Perform migration from mapping.json if it exists and table is empty
        await _migrate_from_json(database_path, mapping_path)

    except Exception as exc:
        logger.exception("Failed to initialize database")
        raise DatabaseError(f"Database initialization failed: {exc}") from exc


async def _migrate_from_json(database_path: Path, mapping_path: Path) -> None:
    if not mapping_path.exists():
        logger.debug("mapping.json not found, skipping migration")
        return

    # Check if database already has products
    async with aiosqlite.connect(database_path) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM products")
        row = await cursor.fetchone()
        if row and row[0] > 0:
            logger.info("Database already contains %d products, skipping migration", row[0])
            return

    logger.info("Starting migration from mapping.json to SQLite")

    try:
        # Read mapping.json
        mapping_data = json.loads(mapping_path.read_text(encoding="utf-8"))
        items = mapping_data.get("items") if isinstance(mapping_data, dict) else mapping_data

        if not isinstance(items, list):
            logger.warning("Invalid mapping.json structure, skipping migration")
            return

        if not items:
            logger.info("mapping.json is empty, skipping migration")
            return

        # Insert all products into database
        async with aiosqlite.connect(database_path) as db:
            db.isolation_level = None  # autocommit

            for index, item in enumerate(items):
                if not isinstance(item, dict):
                    logger.warning("Item #%d is not a dict, skipping", index)
                    continue

                sku_internal = str(item.get("sku_internal", "")).strip()
                if not sku_internal:
                    logger.warning("Item #%d has empty sku_internal, skipping", index)
                    continue

                title = str(item.get("title", "")).strip()
                wb_barcode = _clean_optional(item.get("wb_barcode"))
                ozon_offer_id = _clean_optional(item.get("ozon_offer_id"))
                active = bool(item.get("active", True))

                try:
                    await db.execute(
                        """
                        INSERT INTO products 
                        (sku_internal, title, wb_barcode, ozon_offer_id, active)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (sku_internal, title, wb_barcode, ozon_offer_id, active),
                    )
                except aiosqlite.IntegrityError as exc:
                    logger.warning(
                        "Duplicate sku_internal '%s' during migration: %s", sku_internal, exc
                    )

            await db.commit()

        # Backup the original mapping.json
        backup_path = mapping_path.with_suffix(".json.bak")
        shutil.copy2(mapping_path, backup_path)
        logger.info("Original mapping.json backed up to: %s", backup_path)

        # Count migrated records
        async with aiosqlite.connect(database_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM products")
            row = await cursor.fetchone()
            migrated_count = row[0] if row else 0

        logger.info("Migration completed: %d products imported from mapping.json", migrated_count)

    except Exception as exc:
        logger.exception("Migration from mapping.json failed")
        raise DatabaseError(f"Migration failed: {exc}") from exc


async def get_all_products(database_path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}

    try:
        async with aiosqlite.connect(database_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM products WHERE active = 1")
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

    logger.debug("Loaded %d active products from database", len(result))
    return result


async def get_product_by_sku(database_path: Path, sku_internal: str) -> Optional[dict[str, Any]]:
    try:
        async with aiosqlite.connect(database_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM products WHERE sku_internal = ? AND active = 1",
                (sku_internal,),
            )
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
            await db.execute(
                """
                INSERT OR REPLACE INTO products 
                (sku_internal, title, wb_barcode, ozon_offer_id, active)
                VALUES (?, ?, ?, ?, ?)
                """,
                (sku_internal, title, wb_barcode, ozon_offer_id, active),
            )
            await db.commit()

    except Exception as exc:
        logger.exception("Failed to upsert product: %s", sku_internal)
        raise DatabaseError(f"Upsert failed: {exc}") from exc


def _clean_optional(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None