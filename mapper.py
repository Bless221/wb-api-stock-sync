from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

from exceptions import MappingError

logger = logging.getLogger(__name__)

SKU_COLUMN = "item_sku"
QTY_COLUMN = "quantity"


# ----------------------------------------------------------------------
# Marketplace payload models
# ----------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class WBStockItem:
    """Single Wildberries stock record (API v3 expects barcode + amount)."""

    sku: str
    amount: int

    def to_payload(self) -> dict[str, Any]:
        """Serialize into the WB ``PUT /api/v3/stocks/{warehouseId}`` format."""
        return {"sku": self.sku, "amount": self.amount}


@dataclass(frozen=True, slots=True)
class OzonStockItem:
    """Single Ozon stock record (offer_id + stock, optional warehouse)."""

    offer_id: str
    stock: int
    warehouse_id: Optional[int] = None

    def to_payload(self) -> dict[str, Any]:
        """Serialize into the Ozon ``/v1/product/import/stocks`` format."""
        payload: dict[str, Any] = {"offer_id": self.offer_id, "stock": self.stock}
        if self.warehouse_id is not None:
            payload["warehouse_id"] = self.warehouse_id
        return payload


@dataclass(slots=True)
class MappingResult:
    """Outcome of a single mapping pass over the stock table."""

    wb_items: list[WBStockItem] = field(default_factory=list)
    ozon_items: list[OzonStockItem] = field(default_factory=list)
    unknown_skus: list[str] = field(default_factory=list)
    inactive_skus: list[str] = field(default_factory=list)
    invalid_rows: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        """Human readable one-line summary for logs."""
        return (
            f"WB={len(self.wb_items)} | Ozon={len(self.ozon_items)} | "
            f"unknown={len(self.unknown_skus)} | inactive={len(self.inactive_skus)} | "
            f"invalid={len(self.invalid_rows)}"
        )


@dataclass(frozen=True, slots=True)
class MappingEntry:
    """One row of ``mapping.json`` with full bidirectional indexing."""

    sku_internal: str
    wb_barcode: Optional[str]
    ozon_offer_id: Optional[str]
    title: str = ""
    active: bool = True


# ----------------------------------------------------------------------
# Mapper with O(1) lookups
# ----------------------------------------------------------------------
class ProductMapper:
    """Load ``mapping.json`` and convert stock rows into marketplace payloads.

    Maintains multiple indices (sku_internal, wb_barcode, ozon_offer_id) for
    O(1) forward and reverse lookups. No linear searches.
    """

    def __init__(self, mapping_path: Path, ozon_warehouse_id: Optional[int] = None) -> None:
        self._mapping_path = Path(mapping_path)
        self._ozon_warehouse_id = ozon_warehouse_id

        # O(1) lookup indices
        self._by_sku: dict[str, MappingEntry] = {}
        self._by_wb_barcode: dict[str, MappingEntry] = {}
        self._by_ozon_offer_id: dict[str, MappingEntry] = {}

        self.load()

    # ------------------------------------------------------------------
    # Loading and indexing
    # ------------------------------------------------------------------
    def load(self) -> None:
        """Read and validate the mapping file into multiple O(1) indices."""
        if not self._mapping_path.exists():
            raise MappingError(f"Mapping file not found: {self._mapping_path}")

        try:
            raw = json.loads(self._mapping_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise MappingError(f"Invalid JSON in {self._mapping_path}: {exc}") from exc

        items = raw.get("items") if isinstance(raw, dict) else raw
        if not isinstance(items, list):
            raise MappingError("Mapping file must contain a list under the 'items' key")

        entries_by_sku: dict[str, MappingEntry] = {}
        entries_by_wb: dict[str, MappingEntry] = {}
        entries_by_ozon: dict[str, MappingEntry] = {}

        for index, row in enumerate(items):
            if not isinstance(row, dict):
                raise MappingError(f"Mapping item #{index} is not an object")

            sku_internal = str(row.get("sku_internal", "")).strip()
            if not sku_internal:
                raise MappingError(f"Mapping item #{index} has an empty 'sku_internal'")
            if sku_internal in entries_by_sku:
                raise MappingError(f"Duplicated sku_internal in mapping: {sku_internal}")

            wb_barcode = self._clean_optional(row.get("wb_barcode"))
            ozon_offer_id = self._clean_optional(row.get("ozon_offer_id"))

            if wb_barcode is None and ozon_offer_id is None:
                logger.warning(
                    "Mapping entry '%s' has neither wb_barcode nor ozon_offer_id", sku_internal
                )

            entry = MappingEntry(
                sku_internal=sku_internal,
                wb_barcode=wb_barcode,
                ozon_offer_id=ozon_offer_id,
                title=str(row.get("title", "")).strip(),
                active=bool(row.get("active", True)),
            )

            entries_by_sku[sku_internal] = entry

            if wb_barcode:
                if wb_barcode in entries_by_wb:
                    logger.warning(
                        "Duplicated wb_barcode '%s' in mapping (sku_internal=%s)",
                        wb_barcode,
                        sku_internal,
                    )
                entries_by_wb[wb_barcode] = entry

            if ozon_offer_id:
                if ozon_offer_id in entries_by_ozon:
                    logger.warning(
                        "Duplicated ozon_offer_id '%s' in mapping (sku_internal=%s)",
                        ozon_offer_id,
                        sku_internal,
                    )
                entries_by_ozon[ozon_offer_id] = entry

        self._by_sku = entries_by_sku
        self._by_wb_barcode = entries_by_wb
        self._by_ozon_offer_id = entries_by_ozon

        logger.info(
            "Mapping loaded: %d products, indices: sku=%d, wb_barcode=%d, ozon_offer_id=%d",
            len(entries_by_sku),
            len(entries_by_wb),
            len(entries_by_ozon),
        )

    @staticmethod
    def _clean_optional(value: Any) -> Optional[str]:
        """Normalise an optional identifier into a non-empty string or ``None``."""
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return len(self._by_sku)

    def get_by_sku(self, sku_internal: str) -> Optional[MappingEntry]:
        """O(1) lookup by internal SKU."""
        return self._by_sku.get(sku_internal)

    def get_by_wb_barcode(self, barcode: str) -> Optional[MappingEntry]:
        """O(1) reverse lookup by Wildberries barcode."""
        return self._by_wb_barcode.get(barcode)

    def get_by_ozon_offer_id(self, offer_id: str) -> Optional[MappingEntry]:
        """O(1) reverse lookup by Ozon offer_id."""
        return self._by_ozon_offer_id.get(offer_id)

    def known_skus(self) -> Iterable[str]:
        """Iterate over all internal SKUs known to the mapper."""
        return self._by_sku.keys()

    # ------------------------------------------------------------------
    # Core translation (vectorized for DataFrames)
    # ------------------------------------------------------------------
    def map_dataframe(self, df: pd.DataFrame) -> MappingResult:
        """Convert a stock ``DataFrame`` into WB and Ozon payload items.

        Vectorized operation: validates columns, coerces types once,
        dedups efficiently, then iterates through rows with O(1) lookups.
        """
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
                "Dropped %d rows with non-numeric or negative quantity", valid_rows - coerced_rows
            )

        df = df.drop_duplicates(subset=[SKU_COLUMN], keep="last")
        deduped_rows = len(df)
        if deduped_rows < coerced_rows:
            logger.info("Deduplicated %d duplicate SKUs, keeping last", coerced_rows - deduped_rows)

        for row in df.itertuples(index=False):
            sku_internal = str(getattr(row, SKU_COLUMN)).strip()
            quantity = max(0, int(getattr(row, QTY_COLUMN)))

            entry = self._by_sku.get(sku_internal)
            if entry is None:
                result.unknown_skus.append(sku_internal)
                continue

            if not entry.active:
                result.inactive_skus.append(sku_internal)
                continue

            if entry.wb_barcode:
                result.wb_items.append(WBStockItem(sku=entry.wb_barcode, amount=quantity))
            if entry.ozon_offer_id:
                result.ozon_items.append(
                    OzonStockItem(
                        offer_id=entry.ozon_offer_id,
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

        logger.info(
            "Mapping result: %s (from %d CSV rows)", result.summary, initial_rows
        )
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _validate_frame(df: pd.DataFrame) -> None:
        """Ensure the stock frame exposes the required columns."""
        missing = [column for column in (SKU_COLUMN, QTY_COLUMN) if column not in df.columns]
        if missing:
            raise MappingError(
                f"Stock file must contain columns {SKU_COLUMN!r} and {QTY_COLUMN!r}; "
                f"missing: {missing}"
            )