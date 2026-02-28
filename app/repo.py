from typing import Optional, Literal, Dict, Any, List
from sqlalchemy import text
from sqlalchemy.engine import Engine

SortBy = Optional[Literal["rating", "price"]]
OrderBy = Literal["asc", "desc"]


def list_products(
    engine: Engine,
    platform_name: str,
    min_rating: Optional[float],
    max_price: Optional[float],
    sort_by: SortBy,
    order: OrderBy,
    page: int,
    page_size: int,
) -> Dict[str, Any]:
    """
    List products with pagination + optional filter/sort.

    DB (current):
      - products(pr.id, asin, title, brand, category, image_url, product_url, review_rating, review_count, price, platform_id, updated_at)
      - prices(product_id, ts, price, buybox_price, currency)
      - ratings(product_id, ts, rating, review_count)
      - platforms(id, name)
    """
    page = page if page and page > 0 else 1
    page_size = page_size if page_size and page_size > 0 else 20
    offset = (page - 1) * page_size

    order = (order or "asc").lower()
    if order not in ("asc", "desc"):
        order = "asc"
    sort_by = sort_by if sort_by in ("rating", "price") else None

    latest_price_sql = """
    SELECT p1.product_id, p1.price
    FROM prices p1
    JOIN (SELECT product_id, MAX(ts) AS ts FROM prices GROUP BY product_id) p2
      ON p1.product_id = p2.product_id AND p1.ts = p2.ts
    """

    latest_rating_sql = """
    SELECT r1.product_id, r1.rating, r1.review_count
    FROM ratings r1
    JOIN (SELECT product_id, MAX(ts) AS ts FROM ratings GROUP BY product_id) r2
      ON r1.product_id = r2.product_id AND r1.ts = r2.ts
    """

    where = ["pl.name = :platform_name"]
    params: Dict[str, Any] = {
        "platform_name": platform_name,
        "min_rating": min_rating,
        "max_price": max_price,
        "limit": page_size,
        "offset": offset,
    }

    if min_rating is not None and min_rating > 0:
        where.append("""
        (
          COALESCE(pr.review_rating, lr.rating) IS NOT NULL
          AND COALESCE(pr.review_rating, lr.rating) >= :min_rating
        )
        """)

    if max_price is not None and max_price > 0:
        where.append("""
        (
          COALESCE(pr.price, lp.price) IS NOT NULL
          AND COALESCE(pr.price, lp.price) <= :max_price
        )
        """)

    where_sql = " AND ".join(where)

    if sort_by == "rating":
        order_sql = f"COALESCE(pr.review_rating, lr.rating) {order} NULLS LAST, pr.id ASC"
    elif sort_by == "price":
        order_sql = f"COALESCE(pr.price, lp.price) {order} NULLS LAST, pr.id ASC"
    else:
        order_sql = "pr.id ASC"

    count_sql = f"""
    WITH lp AS ({latest_price_sql}), lr AS ({latest_rating_sql})
    SELECT COUNT(*) AS total
    FROM products pr
    JOIN platforms pl ON pr.platform_id = pl.id
    LEFT JOIN lp ON lp.product_id = pr.id
    LEFT JOIN lr ON lr.product_id = pr.id
    WHERE {where_sql}
    """

    list_sql = f"""
    WITH lp AS ({latest_price_sql}), lr AS ({latest_rating_sql})
    SELECT
      pr.asin,
      pr.title,
      pr.brand,
      pr.category,
      pr.image_url,
      pr.product_url,
      pl.name AS platform,
      COALESCE(pr.price, lp.price) AS price,
      COALESCE(pr.review_rating, lr.rating) AS rating,
      COALESCE(pr.review_count, lr.review_count) AS review_count,
      pr.updated_at
    FROM products pr
    JOIN platforms pl ON pr.platform_id = pl.id
    LEFT JOIN lp ON lp.product_id = pr.id
    LEFT JOIN lr ON lr.product_id = pr.id
    WHERE {where_sql}
    ORDER BY {order_sql}
    LIMIT :limit OFFSET :offset
    """

    with engine.begin() as conn:
        total = conn.execute(text(count_sql), params).scalar_one()
        rows = conn.execute(text(list_sql), params).mappings().all()

    items = []
    for r in rows:
        items.append({
            "asin": r["asin"],
            "title": r["title"],
            "price": float(r["price"]) if r["price"] is not None else None,
            "rating": float(r["rating"]) if r["rating"] is not None else None,
            "review_count": int(r["review_count"]) if r["review_count"] is not None else None,
            "brand": r["brand"],
            "category": r["category"],
            "image": r["image_url"],
            "link": r["product_url"],
            "platform": r["platform"],
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] is not None else None,
        })

    return {"page": page, "page_size": page_size, "total": int(total), "items": items}


def get_product_by_asin(engine: Engine, platform_name: str, asin: str) -> Optional[Dict[str, Any]]:
    sql = """
    WITH lp AS (
      SELECT p1.product_id, p1.price
      FROM prices p1
      JOIN (SELECT product_id, MAX(ts) AS ts FROM prices GROUP BY product_id) p2
        ON p1.product_id = p2.product_id AND p1.ts = p2.ts
    ),
    lr AS (
      SELECT r1.product_id, r1.rating, r1.review_count
      FROM ratings r1
      JOIN (SELECT product_id, MAX(ts) AS ts FROM ratings GROUP BY product_id) r2
        ON r1.product_id = r2.product_id AND r1.ts = r2.ts
    )
    SELECT
      pr.asin,
      pr.title,
      pr.brand,
      pr.category,
      pr.image_url,
      pr.product_url,
      pl.name AS platform,
      COALESCE(pr.price, lp.price) AS price,
      COALESCE(pr.review_rating, lr.rating) AS rating,
      COALESCE(pr.review_count, lr.review_count) AS review_count,
      pr.updated_at
    FROM products pr
    JOIN platforms pl ON pr.platform_id = pl.id
    LEFT JOIN lp ON lp.product_id = pr.id
    LEFT JOIN lr ON lr.product_id = pr.id
    WHERE pl.name = :platform_name AND pr.asin = :asin
    LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"platform_name": platform_name, "asin": asin}).mappings().first()

    if not row:
        return None

    return {
        "asin": row["asin"],
        "title": row["title"],
        "price": float(row["price"]) if row["price"] is not None else None,
        "rating": float(row["rating"]) if row["rating"] is not None else None,
        "review_count": int(row["review_count"]) if row["review_count"] is not None else None,
        "brand": row["brand"],
        "category": row["category"],
        "image": row["image_url"],
        "link": row["product_url"],
        "platform": row["platform"],
        "updated_at": row["updated_at"].isoformat() if row["updated_at"] is not None else None,
    }


def get_product_history(
    engine: Engine,
    platform_name: str,
    asin: str,
    limit: int = 2000,
) -> Optional[Dict[str, Any]]:
    """
    Return time-series history for a product.
    Source tables:
      - prices: price history + buybox history (by ts)
      - sales_rank_history: ranking history (by ts)
    """
    # 1) resolve product_id + updated_at, validate platform+asin
    meta_sql = """
    SELECT pr.id AS product_id, pr.updated_at
    FROM products pr
    JOIN platforms pl ON pr.platform_id = pl.id
    WHERE pl.name = :platform_name AND pr.asin = :asin
    LIMIT 1
    """
    with engine.begin() as conn:
        meta = conn.execute(text(meta_sql), {"platform_name": platform_name, "asin": asin}).mappings().first()
        if not meta:
            return None

        product_id = int(meta["product_id"])
        updated_at = meta["updated_at"]

        # 2) price + buybox history
        price_sql = """
        SELECT ts, price, buybox_price, currency
        FROM prices
        WHERE product_id = :product_id
        ORDER BY ts ASC
        LIMIT :limit
        """
        price_rows = conn.execute(
            text(price_sql),
            {"product_id": product_id, "limit": limit},
        ).mappings().all()

        # 3) rank history (all categories)
        rank_sql = """
        SELECT ts, category, rank
        FROM sales_rank_history
        WHERE product_id = :product_id
        ORDER BY ts ASC
        LIMIT :limit
        """
        rank_rows = conn.execute(
            text(rank_sql),
            {"product_id": product_id, "limit": limit},
        ).mappings().all()

    price_history: List[Dict[str, Any]] = []
    buybox_history: List[Dict[str, Any]] = []
    for r in price_rows:
        ts = r["ts"]
        price_history.append({
            "ts": ts.isoformat() if ts is not None else None,
            "price": float(r["price"]) if r["price"] is not None else None,
            "currency": r.get("currency") or None,
        })
        buybox_history.append({
            "ts": ts.isoformat() if ts is not None else None,
            "buybox_price": float(r["buybox_price"]) if r["buybox_price"] is not None else None,
            "currency": r.get("currency") or None,
        })

    ranking_history: List[Dict[str, Any]] = []
    for r in rank_rows:
        ts = r["ts"]
        ranking_history.append({
            "ts": ts.isoformat() if ts is not None else None,
            "category": r["category"],
            "rank": int(r["rank"]) if r["rank"] is not None else None,
        })

    return {
        "asin": asin,
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "price_history": price_history,
        "buybox_history": buybox_history,
        "ranking_history": ranking_history,
    }
