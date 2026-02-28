import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import WEIGHTS


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def price_score(price: float | None, min_price: float, max_price: float) -> float:
    if price is None:
        return 0.0
    if max_price <= min_price:
        return 100.0
    return clamp((max_price - price) / (max_price - min_price) * 100.0)


def review_score(rating: float | None, review_count: int | None, max_review_count: int) -> float:
    if rating is None or review_count is None:
        return 0.0
    rating_part = clamp((rating / 5.0) * 100.0)
    if max_review_count <= 0:
        count_part = 0.0
    else:
        count_part = clamp(math.log1p(review_count) / math.log1p(max_review_count) * 100.0)
    return clamp(0.7 * rating_part + 0.3 * count_part)


def spec_score(specs: list[str] | None) -> float:
    if not specs:
        return 0.0
    bonus_map = {
        "anc": 10.0,
        "waterproof": 7.0,
        "low latency": 6.0,
        "long battery life": 8.0,
    }
    base = min(len(specs), 5) / 5.0 * 70.0
    bonus = 0.0
    for spec in specs:
        bonus += bonus_map.get(spec.strip().lower(), 0.0)
    return clamp(base + min(bonus, 30.0))


def score_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prices = [x.get("price") for x in items if isinstance(x.get("price"), (int, float))]
    min_price = min(prices) if prices else 0.0
    max_price = max(prices) if prices else 0.0
    max_review_count = max(
        [x.get("review_count") for x in items if isinstance(x.get("review_count"), int)] or [0]
    )

    scored: list[dict[str, Any]] = []
    for item in items:
        p_score = price_score(item.get("price"), min_price, max_price)
        r_score = review_score(item.get("rating"), item.get("review_count"), max_review_count)
        s_score = spec_score(item.get("key_specs"))
        total = (
            WEIGHTS["price_score"] * p_score
            + WEIGHTS["review_score"] * r_score
            + WEIGHTS["spec_score"] * s_score
        )
        out = dict(item)
        out["score_breakdown"] = {
            "price_score": round(p_score, 2),
            "review_score": round(r_score, 2),
            "spec_score": round(s_score, 2),
        }
        out["score_total"] = round(total, 2)
        scored.append(out)
    return scored


def run(input_path: Path, output_path: Path) -> dict[str, Any]:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    items = payload.get("items", [])
    scored_items = score_items(items)
    scored_items.sort(key=lambda x: x.get("score_total", 0.0), reverse=True)

    output = {
        "query": payload.get("query"),
        "scored_at": datetime.now(timezone.utc).isoformat(),
        "weights": WEIGHTS,
        "count": len(scored_items),
        "items": scored_items,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Score cleaned earbuds data")
    parser.add_argument("--input", default="data/clean/clean.json", help="Clean input JSON")
    parser.add_argument("--output", default="data/clean/scored.json", help="Scored output JSON")
    args = parser.parse_args()

    scored = run(Path(args.input), Path(args.output))
    print(f"Scored {scored['count']} records -> {args.output}")


if __name__ == "__main__":
    main()
