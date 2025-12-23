import os
import argparse
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timezone, timedelta

from sqlalchemy import text
from app.db import get_engine
from app.keepa_client import KeepaClient

KEEPA_EPOCH = datetime(2011, 1, 1, tzinfo=timezone.utc)


def keepa_minutes_to_dt(minute: int) -> datetime:
    return KEEPA_EPOCH + timedelta(minutes=int(minute))


def price_from_keepa(v) -> Optional[float]:
    """Keepa price is usually cents*100. Invalid values are <=0 (e.g. -2)."""
    if v is None:
        return None
    try:
        x = float(v)
    except Exception:
        return None
    if x <= 0:
        return None
    return round(x / 100.0, 2)


def clean_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def clean_rating(v) -> Optional[float]:
    """Keepa rating might be 0~5 or 0~50 (47 => 4.7)."""
    if v is None:
        return None
    try:
        r = float(v)
    except Exception:
        return None
    if r <= 0:
        return None
    if r > 5:
        r = r / 10.0
    if not (0 <= r <= 5):
        return None
    return round(r, 2)


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
        yield lst[i:i + n]


def parse_price_history(p: Dict[str, Any]) -> List[Tuple[datetime, float]]:
    """Use csv[0] as price series: [minute, price, minute, price, ...]"""
    out: List[Tuple[datetime, float]] = []
    csv = p.get("csv")
    if not isinstance(csv, list) or not csv or not isinstance(csv[0], list):
        return out
    series = csv[0]
    for i in range(0, len(series) - 1, 2):
        try:
            minute = int(series[i])
            price = price_from_keepa(series[i + 1])
            if price is None:
                continue
            out.append((keepa_minutes_to_dt(minute), price))
        except Exception:
            continue
    return out


def parse_rank_history(p: Dict[str, Any]) -> List[Tuple[datetime, int]]:
    out: List[Tuple[datetime, int]] = []
    sr = p.get("salesRanks")
    if not isinstance(sr, dict) or not sr:
        return out
    first_key = next(iter(sr.keys()))
    series = sr.get(first_key)
    if not isinstance(series, list):
        return out
    for i in range(0, len(series) - 1, 2):
        try:
            minute = int(series[i])
            rank = int(series[i + 1])
            if rank <= 0:
                continue
            out.append((keepa_minutes_to_dt(minute), rank))
        except Exception:
            continue
    return out


def get_snapshot_fields(p: Dict[str, Any]):
    title = p.get("title") or None
    link = p.get("url") or None
    image = p.get("image") or None
    brand = p.get("brand") or None

    category = None
    tree = p.get("categoryTree")
    if isinstance(tree, list) and tree and isinstance(tree[-1], dict):
        category = tree[-1].get("name") or None

    stats = p.get("stats") or {}
    review_count = clean_int(stats.get("reviewCount") or stats.get("reviewsCount"))
    review_rating = clean_rating(stats.get("rating") or stats.get("reviewRating"))

    buybox_price = price_from_keepa(stats.get("buyBoxPrice") or stats.get("buyboxPrice"))

    # current price snapshot: last point in csv[0]
    price = None
    csv = p.get("csv")
    if isinstance(csv, list) and csv and isinstance(csv[0], list) and len(csv[0]) >= 2:
        price = price_from_keepa(csv[0][-1])

    return title, link, image, brand, category, review_count, review_rating, buybox_price, price


# ---------------- DB ops (no repo dependency) ----------------

def ensure_platform(conn, name: str) -> int:
    row = conn.execute(text("SELECT id FROM platforms WHERE name=:n LIMIT 1"), {"n": name}).mappings().first()
    if row:
        return int(row["id"])
    row2 = conn.execute(text("INSERT INTO platforms(name) VALUES(:n) RETURNING id"), {"n": name}).mappings().first()
    return int(row2["id"])


def upsert_product(conn, *, platform_id: int, asin: str, title: str, link: str, image: str,
                  brand: Optional[str], category: Optional[str],
                  review_count: Optional[int], review_rating: Optional[float],
                  buybox_price: Optional[float], price: Optional[float]) -> None:
    conn.execute(
        text("""
        INSERT INTO products (platform_id, asin, title, product_url, image_url, brand, category,
                              review_count, review_rating, buybox_price, price, updated_at)
        VALUES (:platform_id, :asin, :title, :product_url, :image_url, :brand, :category,
                :review_count, :review_rating, :buybox_price, :price, NOW())
        ON CONFLICT (platform_id, asin) DO UPDATE SET
          title = EXCLUDED.title,
          product_url = EXCLUDED.product_url,
          image_url = EXCLUDED.image_url,
          brand = EXCLUDED.brand,
          category = EXCLUDED.category,
          review_count = EXCLUDED.review_count,
          review_rating = EXCLUDED.review_rating,
          buybox_price = EXCLUDED.buybox_price,
          price = EXCLUDED.price,
          updated_at = NOW()
        """),
        {
            "platform_id": platform_id,
            "asin": asin,
            "title": title,
            "product_url": link,
            "image_url": image,
            "brand": brand,
            "category": category,
            "review_count": review_count,
            "review_rating": review_rating,
            "buybox_price": buybox_price,
            "price": price,
        }
    )


def get_product_id(conn, platform_id: int, asin: str) -> Optional[int]:
    row = conn.execute(
        text("SELECT id FROM products WHERE platform_id=:pid AND asin=:asin LIMIT 1"),
        {"pid": platform_id, "asin": asin},
    ).mappings().first()
    return int(row["id"]) if row else None


def max_ts_prices(conn, product_id: int) -> Optional[datetime]:
    row = conn.execute(text("SELECT MAX(ts) AS m FROM prices WHERE product_id=:id"), {"id": product_id}).mappings().first()
    return row["m"] if row else None


def max_ts_ratings(conn, product_id: int) -> Optional[datetime]:
    row = conn.execute(text("SELECT MAX(ts) AS m FROM ratings WHERE product_id=:id"), {"id": product_id}).mappings().first()
    return row["m"] if row else None


def insert_prices(conn, product_id: int, rows: List[Tuple[datetime, float]], min_ts: Optional[datetime]) -> int:
    n = 0
    for ts, price in rows:
        if min_ts and ts <= min_ts:
            continue
        conn.execute(
            text("""
            INSERT INTO prices(product_id, ts, price)
            VALUES(:pid, :ts, :price)
            ON CONFLICT DO NOTHING
            """),
            {"pid": product_id, "ts": ts, "price": price},
        )
        n += 1
    return n


def insert_ratings(conn, product_id: int, ts: datetime, rating: Optional[float], review_count: Optional[int],
                   min_ts: Optional[datetime]) -> int:
    if rating is None and review_count is None:
        return 0
    if min_ts and ts <= min_ts:
        return 0
    conn.execute(
        text("""
        INSERT INTO ratings(product_id, ts, rating, review_count)
        VALUES(:pid, :ts, :rating, :review_count)
        ON CONFLICT DO NOTHING
        """),
        {"pid": product_id, "ts": ts, "rating": rating, "review_count": review_count},
    )
    return 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--asins-file", required=True)
    ap.add_argument("--batch", type=int, default=20)
    ap.add_argument("--stats", type=int, default=1)
    ap.add_argument("--buybox", type=int, default=1)
    args = ap.parse_args()

    engine = get_engine()
    platform_name = os.getenv("PLATFORM_NAME", "amazon_us")

    asins = read_asins(args.asins_file)
    if not asins:
        raise SystemExit("asins-file empty")

    client = KeepaClient(domain=1)

    ok = 0
    add_price = 0
    add_rating = 0

    with engine.begin() as conn:
        platform_id = ensure_platform(conn, platform_name)

        for group in chunk(asins, args.batch):
            payload = client.fetch_products(group, stats=args.stats, buybox=args.buybox)
            products = payload.get("products") or payload.get("Products") or []
            if not isinstance(products, list):
                print("[sync] keepa payload has no products list, skip batch")
                continue

            for p in products:
                asin = p.get("asin")
                if not asin:
                    continue

                title, link, image, brand, category, review_count, review_rating, buybox_price, price = get_snapshot_fields(p)

                upsert_product(
                    conn,
                    platform_id=platform_id,
                    asin=asin,
                    title=title,
                    link=link,
                    image=image,
                    brand=brand,
                    category=category,
                    review_count=review_count,
                    review_rating=review_rating,
                    buybox_price=buybox_price,
                    price=price,
                )

                pid = get_product_id(conn, platform_id, asin)
                if not pid:
                    continue

                # append history
                price_rows = parse_price_history(p)
                min_price_ts = max_ts_prices(conn, pid)
                add_price += insert_prices(conn, pid, price_rows, min_price_ts)

                # rating snapshot into ratings table (ts=now)
                now_ts = datetime.now(timezone.utc)
                min_rating_ts = max_ts_ratings(conn, pid)
                add_rating += insert_ratings(conn, pid, now_ts, review_rating, review_count, min_rating_ts)

                ok += 1

    print(f"OK sync_keepa: products={ok} prices_added={add_price} ratings_added={add_rating}")


if __name__ == "__main__":
    main()
