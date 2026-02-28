import argparse
import csv
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import requests


KNOWN_BRANDS = {
    "apple",
    "samsung",
    "jbl",
    "sony",
    "soundcore",
    "anker",
    "beats",
    "bose",
    "jlab",
    "tagry",
}

SPEC_PATTERNS = [
    (re.compile(r"noise[\s-]?cancelling|anc", re.IGNORECASE), "ANC"),
    (re.compile(r"bluetooth", re.IGNORECASE), "Bluetooth"),
    (re.compile(r"waterproof|ipx\d", re.IGNORECASE), "Waterproof"),
    (re.compile(r"gaming|low[\s-]?latency", re.IGNORECASE), "Low latency"),
    (re.compile(r"wireless", re.IGNORECASE), "Wireless"),
    (re.compile(r"touch", re.IGNORECASE), "Touch controls"),
    (re.compile(r"charging|battery|playtime", re.IGNORECASE), "Long battery life"),
]

ASIN_RE = re.compile(r"/dp/([A-Z0-9]{10})", re.IGNORECASE)


def parse_float(value: str | None) -> float | None:
    if not value:
        return None
    cleaned = re.sub(r"[^0-9.]", "", value)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    if not value:
        return None
    cleaned = re.sub(r"[^0-9]", "", value)
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def extract_asin(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    target = parsed.path or ""
    if parsed.path.startswith("/sspa/click"):
        raw = parse_qs(parsed.query).get("url", [])
        if raw:
            target = unquote(raw[0])
    match = ASIN_RE.search(target)
    if not match:
        return None
    return match.group(1).upper()


def canonicalize_amazon_url(url: str | None) -> str | None:
    asin = extract_asin(url)
    if asin:
        return f"https://www.amazon.com/dp/{asin}"
    if not url:
        return None
    return url.strip() or None


def recover_title_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    path = parsed.path or ""

    if parsed.path.startswith("/sspa/click"):
        raw = parse_qs(parsed.query).get("url", [])
        if raw:
            path = unquote(raw[0])

    segments = [seg for seg in path.split("/") if seg]
    if not segments:
        return None

    if segments and segments[0].lower() == "dp":
        return None

    slug = segments[0]
    if slug.lower() in {"gp", "sspa", "a1"} and len(segments) > 1:
        slug = segments[1]

    if slug.lower() in {"dp", "ref"}:
        return None

    title = slug.replace("-", " ").replace("+", " ").strip()
    return re.sub(r"\s+", " ", title) if title else None


def infer_brand(title: str | None) -> str | None:
    if not title:
        return None
    first = title.split()[0].strip(" ,.-").lower() if title.split() else ""
    if first in KNOWN_BRANDS:
        return first.capitalize()
    if first and first.isalpha() and len(first) >= 2:
        return first.capitalize()
    return None


def infer_key_specs(text: str) -> list[str]:
    specs: list[str] = []
    for pattern, label in SPEC_PATTERNS:
        if pattern.search(text):
            specs.append(label)
    if len(specs) < 2:
        for fallback in ("Wireless", "Bluetooth", "In-ear"):
            if fallback not in specs:
                specs.append(fallback)
            if len(specs) >= 2:
                break
    return specs[:5]


def query_match(record_text: str, query: str) -> bool:
    tokens = [t.lower() for t in query.split() if t.strip()]
    if not tokens:
        return True
    haystack = record_text.lower()
    return any(token in haystack for token in tokens)


def enrich_review_fields(url: str | None) -> tuple[float | None, int | None]:
    if not url:
        return None, None

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        response = requests.get(url, headers=headers, timeout=6)
        if response.status_code >= 400:
            return None, None
        html = response.text
    except requests.RequestException:
        return None, None

    rating: float | None = None
    review_count: int | None = None

    rating_match = re.search(r"([0-5](?:\.[0-9])?)\s+out of 5 stars", html, re.IGNORECASE)
    if rating_match:
        try:
            rating = float(rating_match.group(1))
        except ValueError:
            rating = None

    count_match = re.search(
        r'id="acrCustomerReviewText"[^>]*>\s*([\d,]+)\s*(?:ratings|rating)',
        html,
        re.IGNORECASE,
    )
    if not count_match:
        count_match = re.search(r"([\d,]+)\s+(?:ratings|rating)", html, re.IGNORECASE)
    if count_match:
        review_count = parse_int(count_match.group(1))

    return rating, review_count


def load_db_review_stats(db_path: Path) -> dict[str, dict[str, Any]]:
    if not db_path.exists():
        return {}
    query = """
    SELECT
      pr.asin,
      COALESCE(pr.review_rating, lr.rating) AS rating,
      COALESCE(pr.review_count, lr.review_count) AS review_count
    FROM products pr
    LEFT JOIN (
      SELECT r1.product_id, r1.rating, r1.review_count
      FROM ratings r1
      JOIN (
        SELECT product_id, MAX(ts) AS ts
        FROM ratings
        GROUP BY product_id
      ) r2 ON r1.product_id = r2.product_id AND r1.ts = r2.ts
    ) lr ON lr.product_id = pr.id
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        out: dict[str, dict[str, Any]] = {}
        for row in conn.execute(query):
            asin = (row["asin"] or "").strip().upper()
            if not asin:
                continue
            rating = row["rating"]
            review_count = row["review_count"]
            out[asin] = {
                "rating": float(rating) if rating is not None else None,
                "review_count": int(review_count) if review_count is not None else None,
            }
        return out
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def collect(
    query: str,
    limit: int,
    source_file: Path,
    output_file: Path,
    sqlite_db_file: Path | None = None,
    enrich_missing_reviews: bool = True,
    enrich_max_items: int = 10,
) -> dict[str, Any]:
    db_stats = load_db_review_stats(sqlite_db_file) if sqlite_db_file else {}
    rows: list[dict[str, Any]] = []
    with source_file.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            original_url = (row.get("link") or "").strip()
            canonical_url = canonicalize_amazon_url(original_url)
            title = (row.get("title") or "").strip() or recover_title_from_url(original_url)
            brand = infer_brand(title)
            combined_text = " ".join(filter(None, [title, brand, original_url]))
            if not query_match(combined_text, query):
                continue

            product = {
                "query": query,
                "source": "amazon",
                "title": title or None,
                "brand": brand,
                "price": parse_float(row.get("price")),
                "url": canonical_url or original_url,
                "rating": parse_float(row.get("rating")),
                "review_count": parse_int(row.get("review_count")),
                "key_specs": infer_key_specs(combined_text),
            }
            asin = extract_asin(product.get("url"))
            if asin and asin in db_stats:
                if product["rating"] is None:
                    product["rating"] = db_stats[asin]["rating"]
                if product["review_count"] is None:
                    product["review_count"] = db_stats[asin]["review_count"]
            rows.append(product)
            if len(rows) >= limit:
                break

    if len(rows) < limit:
        with source_file.open(newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if len(rows) >= limit:
                    break
                original_url = (row.get("link") or "").strip()
                canonical_url = canonicalize_amazon_url(original_url)
                title = (row.get("title") or "").strip() or recover_title_from_url(original_url)
                brand = infer_brand(title)
                product = {
                    "query": query,
                    "source": "amazon",
                    "title": title or None,
                    "brand": brand,
                    "price": parse_float(row.get("price")),
                    "url": canonical_url or original_url,
                    "rating": parse_float(row.get("rating")),
                    "review_count": parse_int(row.get("review_count")),
                    "key_specs": infer_key_specs(
                        " ".join(filter(None, [title, brand, original_url]))
                    ),
                }
                asin = extract_asin(product.get("url"))
                if asin and asin in db_stats:
                    if product["rating"] is None:
                        product["rating"] = db_stats[asin]["rating"]
                    if product["review_count"] is None:
                        product["review_count"] = db_stats[asin]["review_count"]
                rows.append(product)

    if enrich_missing_reviews and rows:
        enriched = 0
        for product in rows:
            if enriched >= enrich_max_items:
                break
            needs_rating = product.get("rating") is None
            needs_count = product.get("review_count") is None
            if not (needs_rating or needs_count):
                continue
            live_rating, live_count = enrich_review_fields(product.get("url"))
            if needs_rating and live_rating is not None:
                product["rating"] = live_rating
            if needs_count and live_count is not None:
                product["review_count"] = live_count
            enriched += 1

    output_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "query": query,
        "limit": limit,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "review_enrichment": {
            "enabled": enrich_missing_reviews,
            "max_items": enrich_max_items,
        },
        "count": len(rows),
        "items": rows,
    }
    output_file.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect earbuds products to raw JSON")
    parser.add_argument("--query", required=True, help="Search keyword")
    parser.add_argument("--limit", type=int, default=20, help="Maximum products to collect")
    parser.add_argument(
        "--source-file",
        default="data/real_products.csv",
        help="Input CSV for v0 collection",
    )
    parser.add_argument("--output", default="data/raw/raw.json", help="Raw output JSON path")
    parser.add_argument(
        "--sqlite-db",
        default="data/app.db",
        help="Optional SQLite DB used to backfill stable rating/review_count",
    )
    parser.add_argument(
        "--no-enrich-reviews",
        action="store_true",
        help="Disable live review_count/rating enrichment from product page",
    )
    parser.add_argument(
        "--enrich-max-items",
        type=int,
        default=10,
        help="Maximum number of products to live-enrich for missing review fields",
    )
    args = parser.parse_args()

    payload = collect(
        query=args.query,
        limit=max(1, args.limit),
        source_file=Path(args.source_file),
        output_file=Path(args.output),
        sqlite_db_file=Path(args.sqlite_db) if args.sqlite_db else None,
        enrich_missing_reviews=not args.no_enrich_reviews,
        enrich_max_items=max(0, args.enrich_max_items),
    )
    print(f"Collected {payload['count']} records -> {args.output}")


if __name__ == "__main__":
    main()
