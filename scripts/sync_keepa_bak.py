# -*- coding: utf-8 -*-
"""
Sync products from Keepa into PostgreSQL.

Writes into:
- platforms
- products
- prices (append-only)
- ratings (append-only)
- sales_rank_history (append-only)

Run (from project root):
  source venv/bin/activate
  python -m scripts.sync_keepa --asins-file asins.txt

Env:
  DATABASE_URL    required (SQLAlchemy URL, postgresql+psycopg2://...)
  PLATFORM_NAME   default: amazon_us
  KEEPA_API_KEY   required
"""

import os
import argparse
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

from sqlalchemy import text
from app.db import get_engine
from app.keepa_client import KeepaClient


# ----------------------------
# keepa helpers
# ----------------------------

KEEPA_BASE = datetime(2011, 1, 1, tzinfo=timezone.utc)

def keepa_minutes_to_ts(minute: int) -> datetime:
    return KEEPA_BASE + timedelta(minutes=int(minute))

def keepa_price_to_float(v: Any) -> Optional[float]:
    """
    Keepa price often in cents; <=0 means no data.
    """
    try:
        if v is None:
            return None
        iv = int(v)
        if iv <= 0:
            return None
        return round(iv / 100.0, 2)
    except Exception:
        return None


# ----------------------------
# file utils
# ----------------------------

def read_asins(path: str) -> List[str]:
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                out.append(s)
    return out

def chunk(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


# ----------------------------
# parse keepa product
# ----------------------------

def get_snapshot_fields(p: Dict[str, Any]) -> Dict[str, Any]:
    asin = p.get("asin") or None
    title = p.get("title") or None

    # Keepa url may be missing; fallback dp link
    product_url = p.get("url") or (f"https://www.amazon.com/dp/{asin}" if asin else None)

    image = p.get("image") or None
    brand = p.get("brand") or None

    category = None
    tree = p.get("categoryTree")
    if isinstance(tree, list) and tree:
        last = tree[-1]
        if isinstance(last, dict):
            category = last.get("name") or None

    stats = p.get("stats") or {}
    review_count = stats.get("reviewCount") or stats.get("reviewsCount")
    try:
        review_count = int(review_count) if review_count is not None else None
    except Exception:
        review_count = None

    review_rating = stats.get("rating") or stats.get("reviewRating")
    try:
        review_rating = float(review_rating) if review_rating is not None else None
    except Exception:
        review_rating = None

    buybox_price = keepa_price_to_float(stats.get("buyBoxPrice") or stats.get("buyboxPrice"))

    # current price snapshot from csv[0] last value
    price = None
    csv = p.get("csv")
    if isinstance(csv, list) and csv and isinstance(csv[0], list) and len(csv[0]) >= 2:
        price = keepa_price_to_float(csv[0][-1])

    return {
        "asin": asin,
        "title": title,
        "product_url": product_url,
        "image_url": image,
        "brand": brand,
        "category": category,
        "review_count": review_count,
        "review_rating": review_rating,
        "buybox_price": buybox_price,
        "price": price,
    }

def parse_price_points(p: Dict[str, Any]) -> List[Tuple[datetime, Optional[float]]]:
    """
    Use csv[0] as Amazon price series (minute, price_cents) pairs.
    """
    out: List[Tuple[datetime, Optional[float]]] = []
    csv = p.get("csv")
    if not (isinstance(csv, list) and csv and isinstance(csv[0], list)):
        return out
    series = csv[0]
    for i in range(0, len(series) - 1, 2):
        try:
            minute = int(series[i])
            price = keepa_price_to_float(series[i + 1])
            ts = keepa_minutes_to_ts(minute)
            out.append((ts, price))
        except Exception:
            continue
    return out

def parse_rating_point(p: Dict[str, Any]) -> Optional[Tuple[datetime, Optional[float], Optional[int]]]:
    """
    Keepa rating time-series不是每个商品都稳定；MVP 先写一个“当前快照点”。
    """
    stats = p.get("stats") or {}
    rating = stats.get("rating") or stats.get("reviewRating")
    review_count = stats.get("reviewCount") or stats.get("reviewsCount")

    try:
        rating = float(rating) if rating is not None else None
    except Exception:
        rating = None

    try:
        review_count = int(review_count) if review_count is not None else None
    except Exception:
        review_count = None

    if rating is None and review_count is None:
        return None

    return (datetime.now(tz=timezone.utc), rating, review_count)

def parse_rank_points(p: Dict[str, Any]) -> List[Tuple[datetime, int, str]]:
    """
    salesRanks: dict(categoryId -> [minute, rank, minute, rank...])
    取第一条序列写入 sales_rank_history（category 用 key 字符串）。
    """
    out: List[Tuple[datetime, int, str]] = []
    sr = p.get("salesRanks")
    if not isinstance(sr, dict) or not sr:
        return out

    first_key = next(iter(sr.keys()))
    series = sr.get(first_key)
    if not isinstance(series, list):
        return out

    category = str(first_key) if first_key is not None else "default"

    for i in range(0, len(series) - 1, 2):
        try:
            minute = int(series[i])
            rank = int(series[i + 1])
            if rank <= 0:
                continue
            ts = keepa_minutes_to_ts(minute)
            out.append((ts, rank, category))
        except Exception:
            continue
    return out


# ----------------------------
# DB write helpers (no app.repo dependency)
# ----------------------------

def ensure_platform(conn, platform_name: str) -> int:
    conn.execute(
        text("INSERT INTO platforms(name) VALUES(:name) ON CONFLICT(name) DO NOTHING"),
        {"name": platform_name},
    )
    pid = conn.execute(
        text("SELECT id FROM platforms WHERE name = :name"),
        {"name": platform_name},
    ).scalar_one()
    return int(pid)

def upsert_product(conn, platform_id: int, snap: Dict[str, Any]) -> int:
    """
    products unique is (platform_id, asin) in your DB.
    Upsert and return products.id
    """
    sql = """
    INSERT INTO products (platform_id, asin, title, brand, image_url, product_url, category, review_count, review_rating, buybox_price, price, updated_at)
    VALUES (:platform_id, :asin, :title, :brand, :image_url, :product_url, :category, :review_count, :review_rating, :buybox_price, :price, NOW())
    ON CONFLICT (platform_id, asin)
    DO UPDATE SET
      title = EXCLUDED.title,
      brand = EXCLUDED.brand,
      image_url = EXCLUDED.image_url,
      product_url = EXCLUDED.product_url,
      category = EXCLUDED.category,
      review_count = EXCLUDED.review_count,
      review_rating = EXCLUDED.review_rating,
      buybox_price = EXCLUDED.buybox_price,
      price = EXCLUDED.price,
      updated_at = NOW()
    RETURNING id
    """
    pid = conn.execute(text(sql), {"platform_id": platform_id, **snap}).scalar_one()
    return int(pid)

def insert_prices(conn, product_id: int, points: List[Tuple[datetime, Optional[float]]], buybox_price: Optional[float]) -> int:
    """
    Append into prices(product_id, ts, price, buybox_price, currency)
    Unique constraint should be (product_id, ts) in your schema.
    """
    if not points:
        return 0
    added = 0
    sql = """
    INSERT INTO prices (product_id, ts, price, buybox_price)
    VALUES (:product_id, :ts, :price, :buybox_price)
    ON CONFLICT (product_id, ts) DO NOTHING
    """
    for ts, price in points:
        r = conn.execute(text(sql), {"product_id": product_id, "ts": ts, "price": price, "buybox_price": buybox_price})
        # rowcount is 1 if inserted, 0 if conflict
        try:
            added += int(r.rowcount or 0)
        except Exception:
            pass
    return added

def insert_ratings(conn, product_id: int, point: Optional[Tuple[datetime, Optional[float], Optional[int]]]) -> int:
    """
    Append into ratings(product_id, ts, rating, review_count)
    """
    if not point:
        return 0
    ts, rating, review_count = point
    sql = """
    INSERT INTO ratings (product_id, ts, rating, review_count)
    VALUES (:product_id, :ts, :rating, :review_count)
    ON CONFLICT (product_id, ts) DO NOTHING
    """
    r = conn.execute(text(sql), {"product_id": product_id, "ts": ts, "rating": rating, "review_count": review_count})
    try:
        return int(r.rowcount or 0)
    except Exception:
        return 0

def insert_ranks(conn, product_id: int, points: List[Tuple[datetime, int, str]]) -> int:
    """
    Append into sales_rank_history(product_id, ts, rank, category)
    Unique constraint is typically (product_id, ts, category) (you后面已修复 schema.sql)
    """
    if not points:
        return 0
    added = 0
    sql = """
    INSERT INTO sales_rank_history (product_id, ts, rank, category)
    VALUES (:product_id, :ts, :rank, :category)
    ON CONFLICT (product_id, ts, category) DO NOTHING
    """
    for ts, rank, category in points:
        r = conn.execute(text(sql), {"product_id": product_id, "ts": ts, "rank": rank, "category": category})
        try:
            added += int(r.rowcount or 0)
        except Exception:
            pass
    return added


# ----------------------------
# main
# ----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--asins-file", required=True)
    ap.add_argument("--batch", type=int, default=20)
    ap.add_argument("--stats", type=int, default=1)
    ap.add_argument("--buybox", type=int, default=1)
    args = ap.parse_args()

    asins = read_asins(args.asins_file)
    if not asins:
        raise SystemExit("asins-file empty")

    engine = get_engine()
    platform_name = os.getenv("PLATFORM_NAME", "amazon_us")

    client = KeepaClient(domain=1)

    products_ok = 0
    prices_added = 0
    ratings_added = 0
    ranks_added = 0

    for group in chunk(asins, args.batch):
        payload = client.fetch_products(group, stats=args.stats, buybox=args.buybox)
        products = payload.get("products") or payload.get("Products") or []
        if not isinstance(products, list):
            continue

        with engine.begin() as conn:
            platform_id = ensure_platform(conn, platform_name)

            for p in products:
                snap = get_snapshot_fields(p)
                asin = snap.get("asin")
                if not asin:
                    continue

                product_id = upsert_product(conn, platform_id, snap)

                # append histories
                buybox = snap.get("buybox_price")

                ph = parse_price_points(p)
                prices_added += insert_prices(conn, product_id, ph, buybox)

                rp = parse_rating_point(p)
                ratings_added += insert_ratings(conn, product_id, rp)

                rk = parse_rank_points(p)
                ranks_added += insert_ranks(conn, product_id, rk)

                products_ok += 1

    print(
        f"OK sync_keepa: products={products_ok} "
        f"prices_added={prices_added} ratings_added={ratings_added} ranks_added={ranks_added}"
    )

if __name__ == "__main__":
    main()
