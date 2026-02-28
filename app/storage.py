import csv
import os
import re
from typing import List, Optional, Tuple
from .models import Product

_PRICE_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)")


def _price_to_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    m = _PRICE_RE.search(str(s).replace(",", ""))
    return float(m.group(1)) if m else None


def _to_float(s: Optional[str]) -> Optional[float]:
    if s is None or str(s).strip() == "":
        return None
    try:
        return float(str(s).strip())
    except Exception:
        return None


def _to_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    t = str(s).strip().replace(",", "")
    return int(t) if t.isdigit() else None


def load_products(csv_path: str) -> List[Product]:
    """
    Data cleaning:
    - price: '$24.68' -> 24.68 (float) for filter/sort
    - rating: string -> float
    - review_count: string -> int
    - missing fields kept as None
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    items: List[Product] = []
    with open(csv_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            items.append(
                Product(
                    asin=row.get("asin") or None,
                    title=row.get("title") or None,
                    price=_price_to_float(row.get("price")),
                    rating=_to_float(row.get("rating")),
                    review_count=_to_int(row.get("review_count")),
                    image=row.get("image") or None,
                    link=row.get("link") or None,
                )
            )
    return items


def filter_sort(
    items: List[Product],
    min_rating: Optional[float],
    max_price: Optional[float],
    sort_by: Optional[str],
    order: str,
) -> List[Product]:
    out = items

    if min_rating is not None:
        out = [p for p in out if p.rating is not None and p.rating >= min_rating]

    if max_price is not None:
        out = [p for p in out if p.price is not None and p.price <= max_price]

    if sort_by in ("rating", "price"):
        reverse = (order or "asc").lower() == "desc"

        def key(p: Product) -> Tuple[int, float]:
            v = getattr(p, sort_by)
            return (1, 0.0) if v is None else (0, float(v))

        out = sorted(out, key=key, reverse=reverse)

    return out

