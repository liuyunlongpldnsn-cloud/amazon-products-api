import argparse
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


ASIN_RE = re.compile(r"/dp/([A-Z0-9]{10})", re.IGNORECASE)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_asin(url: str | None, fallback_index: int) -> str:
    if not url:
        return f"LOCAL{fallback_index:06d}"
    parsed = urlparse(url)
    target = parsed.path or ""
    if parsed.path.startswith("/sspa/click"):
        nested = parse_qs(parsed.query).get("url", [])
        if nested:
            target = unquote(nested[0])
    match = ASIN_RE.search(target)
    if match:
        return match.group(1).upper()
    return f"LOCAL{fallback_index:06d}"


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS platforms (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS categories (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS products (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          asin TEXT NOT NULL,
          title TEXT,
          brand TEXT,
          category TEXT,
          image_url TEXT,
          product_url TEXT,
          review_count INTEGER,
          review_rating REAL,
          buybox_price REAL,
          price REAL,
          platform_id INTEGER NOT NULL,
          category_id INTEGER,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(platform_id) REFERENCES platforms(id),
          FOREIGN KEY(category_id) REFERENCES categories(id),
          UNIQUE(platform_id, asin)
        );

        CREATE TABLE IF NOT EXISTS prices (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          product_id INTEGER NOT NULL,
          ts TEXT NOT NULL,
          price REAL,
          buybox_price REAL,
          currency TEXT,
          FOREIGN KEY(product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS ratings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          product_id INTEGER NOT NULL,
          ts TEXT NOT NULL,
          rating REAL,
          review_count INTEGER,
          FOREIGN KEY(product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS sales_rank_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          product_id INTEGER NOT NULL,
          ts TEXT NOT NULL,
          category TEXT NOT NULL DEFAULT 'default',
          rank INTEGER,
          created_at TEXT NOT NULL,
          FOREIGN KEY(product_id) REFERENCES products(id)
        );

        CREATE INDEX IF NOT EXISTS idx_products_platform_asin ON products(platform_id, asin);
        CREATE INDEX IF NOT EXISTS idx_products_updated_at ON products(updated_at);
        CREATE INDEX IF NOT EXISTS idx_prices_product_ts ON prices(product_id, ts);
        CREATE INDEX IF NOT EXISTS idx_ratings_product_ts ON ratings(product_id, ts);
        CREATE INDEX IF NOT EXISTS idx_rank_product_ts ON sales_rank_history(product_id, ts);
        """
    )
    conn.commit()


def upsert_platform(conn: sqlite3.Connection, name: str) -> int:
    conn.execute("INSERT OR IGNORE INTO platforms(name) VALUES (?)", (name,))
    row = conn.execute("SELECT id FROM platforms WHERE name = ?", (name,)).fetchone()
    if not row:
        raise RuntimeError("Failed to ensure platform row")
    return int(row[0])


def import_clean_json(
    input_file: Path, db_file: Path, platform_name: str, reset_products: bool
) -> dict[str, Any]:
    payload = json.loads(input_file.read_text(encoding="utf-8"))
    items = payload.get("items", [])
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_file))
    try:
        init_schema(conn)
        platform_id = upsert_platform(conn, platform_name)

        if reset_products:
            conn.execute("DELETE FROM sales_rank_history")
            conn.execute("DELETE FROM ratings")
            conn.execute("DELETE FROM prices")
            conn.execute("DELETE FROM products")
            conn.commit()

        imported = 0
        ts = now_iso()
        for idx, item in enumerate(items, start=1):
            asin = extract_asin(item.get("url"), idx)
            title = item.get("title")
            brand = item.get("brand")
            category = "wireless earbuds"
            url = item.get("url")
            price = item.get("price")
            rating = item.get("rating")
            review_count = item.get("review_count")

            conn.execute(
                """
                INSERT INTO products (
                  asin, title, brand, category, image_url, product_url, review_count,
                  review_rating, buybox_price, price, platform_id, category_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?, ?, NULL, ?, ?)
                ON CONFLICT(platform_id, asin) DO UPDATE SET
                  title=excluded.title,
                  brand=excluded.brand,
                  category=excluded.category,
                  product_url=excluded.product_url,
                  review_count=excluded.review_count,
                  review_rating=excluded.review_rating,
                  price=excluded.price,
                  updated_at=excluded.updated_at
                """,
                (
                    asin,
                    title,
                    brand,
                    category,
                    url,
                    review_count,
                    rating,
                    price,
                    platform_id,
                    ts,
                    ts,
                ),
            )
            product_row = conn.execute(
                "SELECT id FROM products WHERE platform_id = ? AND asin = ?",
                (platform_id, asin),
            ).fetchone()
            if not product_row:
                continue
            product_id = int(product_row[0])

            conn.execute(
                """
                INSERT INTO prices (product_id, ts, price, buybox_price, currency)
                VALUES (?, ?, ?, NULL, 'USD')
                """,
                (product_id, ts, price),
            )
            conn.execute(
                """
                INSERT INTO ratings (product_id, ts, rating, review_count)
                VALUES (?, ?, ?, ?)
                """,
                (product_id, ts, rating, review_count),
            )
            imported += 1

        conn.commit()
        products_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        return {
            "input_count": len(items),
            "imported_rows": imported,
            "products_total": int(products_count),
            "platform": platform_name,
            "db_file": str(db_file),
        }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Import clean earbuds JSON into SQLite DB")
    parser.add_argument("--input", default="data/clean/clean.json", help="Clean JSON file")
    parser.add_argument("--db", default="data/app.db", help="SQLite DB file path")
    parser.add_argument("--platform", default="amazon_us", help="Platform name")
    parser.add_argument(
        "--reset-products",
        action="store_true",
        help="Delete existing product/history rows before import",
    )
    args = parser.parse_args()

    result = import_clean_json(
        input_file=Path(args.input),
        db_file=Path(args.db),
        platform_name=args.platform,
        reset_products=args.reset_products,
    )
    print(
        f"Imported {result['imported_rows']}/{result['input_count']} rows "
        f"into {result['db_file']} (products total={result['products_total']})"
    )


if __name__ == "__main__":
    main()
