import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_FIELDS = [
    "query",
    "source",
    "title",
    "brand",
    "price",
    "url",
    "rating",
    "review_count",
    "key_specs",
]


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_specs(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        spec = item.strip()
        if not spec:
            continue
        key = spec.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(spec)
        if len(normalized) >= 5:
            break
    return normalized


def normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "query": item.get("query"),
        "source": item.get("source"),
        "title": item.get("title"),
        "brand": item.get("brand"),
        "price": to_float(item.get("price")),
        "url": item.get("url"),
        "rating": to_float(item.get("rating")),
        "review_count": to_int(item.get("review_count")),
        "key_specs": normalize_specs(item.get("key_specs")),
    }
    for key in REQUIRED_FIELDS:
        if key not in normalized:
            normalized[key] = None
    return normalized


def dedupe_key(item: dict[str, Any]) -> str:
    url = item.get("url")
    if isinstance(url, str) and url.strip():
        return f"url::{url.strip()}"
    title = item.get("title") or ""
    brand = item.get("brand") or ""
    return f"title_brand::{str(title).strip().lower()}::{str(brand).strip().lower()}"


def is_missing(item: dict[str, Any], field: str) -> bool:
    value = item.get(field)
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, list) and len(value) == 0:
        return True
    return False


def build_quality_report(items: list[dict[str, Any]]) -> str:
    total = len(items)
    valid_price = sum(1 for x in items if x.get("price") is not None)
    valid_rating = sum(1 for x in items if x.get("rating") is not None)
    missing_counter: Counter[str] = Counter()
    for field in REQUIRED_FIELDS:
        missing_counter[field] = sum(1 for x in items if is_missing(x, field))

    top_missing = missing_counter.most_common(3)
    lines = [
        "Earbuds Data Quality Report",
        f"Generated at: {datetime.now(timezone.utc).isoformat()}",
        "",
        f"Total records: {total}",
        f"Valid price ratio: {valid_price}/{total} ({(valid_price / total * 100) if total else 0:.2f}%)",
        f"Valid rating ratio: {valid_rating}/{total} ({(valid_rating / total * 100) if total else 0:.2f}%)",
        "",
        "Top 3 missing fields:",
    ]
    for name, count in top_missing:
        ratio = (count / total * 100) if total else 0
        lines.append(f"- {name}: {count}/{total} ({ratio:.2f}%)")
    return "\n".join(lines) + "\n"


def clean(raw_path: Path, clean_path: Path, report_path: Path) -> dict[str, Any]:
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    raw_items = payload.get("items", [])

    cleaned: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        normalized = normalize_item(item)
        key = dedupe_key(normalized)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(normalized)

    clean_payload = {
        "query": payload.get("query"),
        "cleaned_at": datetime.now(timezone.utc).isoformat(),
        "input_count": len(raw_items),
        "count": len(cleaned),
        "items": cleaned,
    }

    clean_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    clean_path.write_text(
        json.dumps(clean_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report_path.write_text(build_quality_report(cleaned), encoding="utf-8")
    return clean_payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean raw earbuds data")
    parser.add_argument("--input", default="data/raw/raw.json", help="Raw input JSON")
    parser.add_argument("--output", default="data/clean/clean.json", help="Clean output JSON")
    parser.add_argument(
        "--report",
        default="output/quality_report.txt",
        help="Quality report output path",
    )
    args = parser.parse_args()

    payload = clean(
        raw_path=Path(args.input),
        clean_path=Path(args.output),
        report_path=Path(args.report),
    )
    print(f"Cleaned {payload['input_count']} -> {payload['count']} records")
    print(f"Generated: {args.output}, {args.report}")


if __name__ == "__main__":
    main()
