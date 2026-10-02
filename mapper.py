from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from database import DatabaseError, get_all_products

logger = logging.getLogger(__name__)

SKU_COLUMN = "item_sku"
QTY_COLUMN = "quantity"


class MappingError(Exception):
    pass


# ----------------------------------------------------------------------
# Marketplace payload models
# ----------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class WBStockItem:
    sku: str
    amount: int

    def to_payload(self) -> dict[str, Any]:
        return {"sku": self.sku, "amount": self.amount}


@dataclass(frozen=True, slots=True)
class OzonStockItem:
    offer_id: str
    stock: int
    warehouse_id: Optional[int] = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"offer_id": self.offer_id, "stock": self.stock}
        if self.warehouse_id is not None:
            payload["warehouse_id"] = self.warehouse_id
        return payload


@dataclass(slots=True)
class MappingResult:
    wb_items: list[WBStockItem] = field(default_factory=list)
    ozon_items: list[OzonStockItem] = field(default_factory=list)
    unknown_skus: list[str] = field(default_factory=list)
    inactive_skus: list[str] = field(default_factory=list)
    invalid_rows: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        return (
            f"WB={len(self.wb_items)} | Ozon={len(self.ozon_items)} | "
            f"unknown={len(self.unknown_skus)} | inactive={len(self.inactive_skus)} | "
            f"invalid={len(self.invalid_rows)}"
        )


# ----------------------------------------------------------------------
# Mapper with SQLite backend and in-memory cache
# ----------------------------------------------------------------------
class ProductMapper:

    def __init__(
            self,
            database_path: Path,
            ozon_warehouse_id: Optional[int] = None,
    ) -> None:
        self._database_path = Path(database_path)
        self._ozon_warehouse_id = ozon_warehouse_id
        self._cache: dict[str, dict[str, Any]] = {}
        self._reverse_cache_wb: dict[str, str] = {}  # barcode -> sku_internal
        self._reverse_cache_ozon: dict[str, str] = {}  # offer_id -> sku_internal

    async def load(self) -> None:
        try:
            products = await get_all_products(self._database_path)
        except DatabaseError as exc:
            raise MappingError(f"Failed to load products from database: {exc}") from exc

        self._cache = products
        self._reverse_cache_wb.clear()
        self._reverse_cache_ozon.clear()

        for sku_internal, product in products.items():
            if product.get("active"):
                if product.get("wb_barcode"):
                    self._reverse_cache_wb[product["wb_barcode"]] = sku_internal
                if product.get("ozon_offer_id"):
                    self._reverse_cache_ozon[product["ozon_offer_id"]] = sku_internal

        logger.info(
            "Mapper cache loaded: %d products (wb_barcodes=%d, ozon_offers=%d)",
            len(self._cache),
            len(self._reverse_cache_wb),
            len(self._reverse_cache_ozon),
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return len(self._cache)

    def get_by_sku(self, sku_internal: str) -> Optional[dict[str, Any]]:
        return self._cache.get(sku_internal)

    def get_by_wb_barcode(self, barcode: str) -> Optional[dict[str, Any]]:
        sku_internal = self._reverse_cache_wb.get(barcode)
        return self._cache.get(sku_internal) if sku_internal else None

    def get_by_ozon_offer_id(self, offer_id: str) -> Optional[dict[str, Any]]:
        sku_internal = self._reverse_cache_ozon.get(offer_id)
        return self._cache.get(sku_internal) if sku_internal else None

    # ------------------------------------------------------------------
    # Core translation (vectorized for DataFrames)
    # ------------------------------------------------------------------
    def map_dataframe(self, df: pd.DataFrame) -> MappingResult:
        self._validate_frame(df)
        result = MappingResult()

        if df.empty:
            return result

        initial_rows = len(df)

        df = df.copy()
        df[SKU_COLUMN] = df[SKU_COLUMN].astype(str).str.strip()
        df = df[df[SKU_COLUMN] != ""]
        df = df.dropna(subset=[SKU_COLUMN, QTY_COLUMN])

        valid_rows = len(df)
        invalid_count = initial_rows - valid_rows
        if invalid_count > 0:
            logger.info("Dropped %d rows with null/empty SKU or quantity", invalid_count)

        df[QTY_COLUMN] = pd.to_numeric(df[QTY_COLUMN], errors="coerce")
        df = df.dropna(subset=[QTY_COLUMN])
        df[QTY_COLUMN] = df[QTY_COLUMN].astype(int)
        df = df[df[QTY_COLUMN] >= 0]

        coerced_rows = len(df)
        if coerced_rows < valid_rows:
            logger.info(
                "Dropped %d rows with non-numeric or negative quantity",
                valid_rows - coerced_rows,
            )

        df = df.drop_duplicates(subset=[SKU_COLUMN], keep="last")
        deduped_rows = len(df)
        if deduped_rows < coerced_rows:
            logger.info(
                "Deduplicated %d duplicate SKUs, keeping last", coerced_rows - deduped_rows
            )

        for row in df.itertuples(index=False):
            sku_internal = str(getattr(row, SKU_COLUMN)).strip()
            quantity = max(0, int(getattr(row, QTY_COLUMN)))

            product = self.get_by_sku(sku_internal)
            if product is None:
                result.unknown_skus.append(sku_internal)
                continue

            if not product.get("active", False):
                result.inactive_skus.append(sku_internal)
                continue

            if product.get("wb_barcode"):
                result.wb_items.append(
                    WBStockItem(sku=product["wb_barcode"], amount=quantity)
                )

            if product.get("ozon_offer_id"):
                result.ozon_items.append(
                    OzonStockItem(
                        offer_id=product["ozon_offer_id"],
                        stock=quantity,
                        warehouse_id=self._ozon_warehouse_id,
                    )
                )

        if result.unknown_skus:
            logger.warning(
                "Unmapped SKUs skipped (%d): %s",
                len(result.unknown_skus),
                ", ".join(result.unknown_skus[:15]),
            )
        if result.inactive_skus:
            logger.info("Inactive products skipped: %d", len(result.inactive_skus))

        logger.info("Mapping result: %s (from %d CSV rows)", result.summary, initial_rows)
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _validate_frame(df: pd.DataFrame) -> None:
        missing = [column for column in (SKU_COLUMN, QTY_COLUMN) if column not in df.columns]
        if missing:
            raise MappingError(
                f"Stock file must contain columns {SKU_COLUMN!r} and {QTY_COLUMN!r}; "
                f"missing: {missing}"
            )
