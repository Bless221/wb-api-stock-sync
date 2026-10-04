from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from database import DatabaseError, get_all_products, DBProduct

logger = logging.getLogger(__name__)

SKU_COLUMN = "item_sku"
QTY_COLUMN = "quantity"


class MappingError(Exception):
    pass


@dataclass(frozen=True)
class WBStockItem:
    sku: str
    amount: int

    def to_payload(self) -> dict[str, Any]:
        return {"sku": self.sku, "amount": self.amount}


@dataclass(frozen=True)
class OzonStockItem:
    offer_id: str
    stock: int
    warehouse_id: Optional[int] = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"offer_id": self.offer_id, "stock": self.stock}
        if self.warehouse_id is not None:
            payload["warehouse_id"] = self.warehouse_id
        return payload


@dataclass
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


class ProductMapper:

    def __init__(self, database_path: Path, ozon_warehouse_id: Optional[int] = None) -> None:
        self._database_path = Path(database_path)
        self._ozon_warehouse_id = ozon_warehouse_id
        self._cache: dict[str, DBProduct] = {}
        self._reverse_cache_wb: dict[str, str] = {}
        self._reverse_cache_ozon: dict[str, str] = {}

    async def load(self) -> None:
        try:
            products = await get_all_products(self._database_path)
        except DatabaseError as exc:
            raise MappingError(f"Failed to load products from database: {exc}") from exc

        self._cache = products
        self._reverse_cache_wb.clear()
        self._reverse_cache_ozon.clear()

        for sku_internal, product in products.items():
            if product.active:
                wb_bc = product.wb_barcode
                if wb_bc:
                    self._reverse_cache_wb[str(wb_bc).strip()] = sku_internal

                ozon_id = product.ozon_offer_id
                if ozon_id:
                    self._reverse_cache_ozon[str(ozon_id).strip()] = sku_internal

        logger.info(
            "Mapper cache successfully loaded: %d products (wb_barcodes=%d, ozon_offers=%d)",
            len(self._cache),
            len(self._reverse_cache_wb),
            len(self._reverse_cache_ozon),
        )

    def __len__(self) -> int:
        return len(self._cache)

    def get_by_sku(self, sku_internal: str) -> Optional[DBProduct]:
        return self._cache.get(sku_internal)

    def map_dataframe(self, df: pd.DataFrame) -> MappingResult:
        self._validate_frame(df)
        result = MappingResult()

        if df.empty:
            return result

        initial_rows = len(df)
        df = df.copy()
        df[SKU_COLUMN] = df[SKU_COLUMN].astype(str).str.strip()
        df = df.dropna(subset=[SKU_COLUMN, QTY_COLUMN])

        df[QTY_COLUMN] = pd.to_numeric(df[QTY_COLUMN], errors="coerce")
        df = df.dropna(subset=[QTY_COLUMN])
        df[QTY_COLUMN] = df[QTY_COLUMN].astype(int)

        negative_mask = df[QTY_COLUMN] < 0
        if negative_mask.any():
            invalid_skus = df[negative_mask][SKU_COLUMN].tolist()
            result.invalid_rows.extend(invalid_skus)
            df = df[~negative_mask]

        df = df.drop_duplicates(subset=[SKU_COLUMN], keep="last")

        for row in df.itertuples(index=False):
            sku_internal = str(getattr(row, SKU_COLUMN)).strip()
            quantity = int(getattr(row, QTY_COLUMN))

            product = self.get_by_sku(sku_internal)
            if product is None:
                result.unknown_skus.append(sku_internal)
                continue

            if not product.active:
                result.inactive_skus.append(sku_internal)
                continue

            wb_bc = product.wb_barcode
            if wb_bc and str(wb_bc).strip().lower() != "none":
                result.wb_items.append(WBStockItem(sku=str(wb_bc).strip(), amount=quantity))

            ozon_id = product.ozon_offer_id
            if ozon_id and str(ozon_id).strip().lower() != "none":
                result.ozon_items.append(
                    OzonStockItem(
                        offer_id=str(ozon_id).strip(),
                        stock=quantity,
                        warehouse_id=self._ozon_warehouse_id,
                    )
                )

        return result

    @staticmethod
    def _validate_frame(df: pd.DataFrame) -> None:
        missing = [column for column in (SKU_COLUMN, QTY_COLUMN) if column not in df.columns]
        if missing:
            raise MappingError(f"Stock file missing fields: {missing}")
