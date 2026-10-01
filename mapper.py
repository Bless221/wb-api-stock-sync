"""Product mapping layer.

Translates a warehouse stock table (Pandas ``DataFrame`` built from
``stocks.csv``) into marketplace-specific payload structures:

* Wildberries API v3 requires **barcodes** (``sku`` field);
* Ozon Seller API requires **text offer ids** (``offer_id`` field).

``mapping.json`` is the single source of truth that binds both identifiers
to one internal warehouse SKU.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

logger = logging.getLogger(__name__)

SKU_COLUMN = "item_sku"
QTY_COLUMN = "quantity"


class MappingError(Exception):
    """Raised when the mapping file is missing, malformed or inconsistent."""


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
    """One row of ``mapping.json``."""

    sku_internal: str
    wb_barcode: Optional[str]
    ozon_offer_id: Optional[str]
    title: str = ""
    active: bool = True


# ----------------------------------------------------------------------
# Mapper
# ----------------------------------------------------------------------
class ProductMapper:
    """Load ``mapping.json`` and convert stock rows into marketplace payloads."""

    def __init__(self, mapping_path: Path, ozon_warehouse_id: Optional[int] = None) -> None:
        self._mapping_path = Path(mapping_path)
        self._ozon_warehouse_id = ozon_warehouse_id
        self._entries: dict[str, MappingEntry] = {}
        self.load()

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------
    def load(self) -> None:
        """Read and validate the mapping file into an in-memory index."""
        if not self._mapping_path.exists():
            raise MappingError(f"Mapping file not found: {self._mapping_path}")

        try:
            raw = json.loads(self._mapping_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise MappingError(f"Invalid JSON in {self._mapping_path}: {exc}") from exc

        items = raw.get("items") if isinstance(raw, dict) else raw
        if not isinstance(items, list):
            raise MappingError("Mapping file must contain a list under the 'items' key")

        entries: dict[str, MappingEntry] = {}
        for index, row in enumerate(items):
            if not isinstance(row, dict):
                raise MappingError(f"Mapping item #{index} is not an object")

            sku_internal = str(row.get("sku_internal", "")).strip()
            if not sku_internal:
                raise MappingError(f"Mapping item #{index} has an empty 'sku_internal'")
            if sku_internal in entries:
                raise MappingError(f"Duplicated sku_internal in mapping: {sku_internal}")

            wb_barcode = self._clean_optional(row.get("wb_barcode"))
            ozon_offer_id = self._clean_optional(row.get("ozon_offer_id"))
            if wb_barcode is None and ozon_offer_id is None:
                logger.warning(
                    "Mapping entry '%s' has neither wb_barcode nor ozon_offer_id", sku_internal
                )

            entries[sku_internal] = MappingEntry(
                sku_internal=sku_internal,
                wb_barcode=wb_barcode,
                ozon_offer_id=ozon_offer_id,
                title=str(row.get("title", "")).strip(),
                active=bool(row.get("active", True)),
            )

        self._entries = entries
        logger.info("Mapping loaded: %d products from %s", len(entries), self._mapping_path)

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
        return len(self._entries)

    def get(self, sku_internal: str) -> Optional[MappingEntry]:
        """Return the mapping entry for an internal SKU, if present."""
        return self._entries.get(sku_internal)

    def known_skus(self) -> Iterable[str]:
        """Iterate over all internal SKUs known to the mapper."""
        return self._entries.keys()

    # ------------------------------------------------------------------
    # Core translation
    # ------------------------------------------------------------------
    def map_dataframe(self, df: pd.DataFrame) -> MappingResult:
        """Convert a stock ``DataFrame`` into WB and Ozon payload items.

        The frame must contain the ``item_sku`` and ``quantity`` columns.
        Rows with unknown SKUs, inactive products or invalid quantities are
        collected into the result instead of raising, so a single bad line
        never breaks the whole synchronisation cycle.
        """
        self._validate_frame(df)
        result = MappingResult()

        for row in df.itertuples(index=False):
            raw_sku = getattr(row, SKU_COLUMN)
            raw_qty = getattr(row, QTY_COLUMN)

            sku_internal = str(raw_sku).strip()
            if not sku_internal:
                result.invalid_rows.append("<empty item_sku>")
                continue

            quantity = self._coerce_quantity(raw_qty)
            if quantity is None:
                result.invalid_rows.append(sku_internal)
                continue

            entry = self._entries.get(sku_internal)
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
                ", ".join(result.unknown_skus[:10]),
            )
        if result.invalid_rows:
            logger.warning(
                "Rows with invalid quantity skipped (%d): %s",
                len(result.invalid_rows),
                ", ".join(result.invalid_rows[:10]),
            )

        logger.info("Mapping result: %s", result.summary)
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

    @staticmethod
    def _coerce_quantity(value: Any) -> Optional[int]:
        """Cast a raw cell value into a non-negative integer quantity."""
        if pd.isna(value):
            return None
        try:
            quantity = int(float(value))
        except (TypeError, ValueError):
            return None
        return max(quantity, 0)