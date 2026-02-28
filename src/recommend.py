import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_reason(item: dict[str, Any], rank: int) -> str:
    title = item.get("title") or "This product"
    brand = item.get("brand") or "Unknown brand"
    price = item.get("price")
    rating = item.get("rating")
    specs = item.get("key_specs") or []
    score = item.get("score_total", 0)
    breakdown = item.get("score_breakdown", {})

    lines = [
        f"{title} ranked #{rank} in this query and reached a total score of {score}.",
        f"It is from {brand} and currently priced at ${price:.2f}." if isinstance(price, (int, float)) else f"It is from {brand} and its current price is unavailable.",
        f"The model favors it mainly on price_score={breakdown.get('price_score', 0)}, review_score={breakdown.get('review_score', 0)}, and spec_score={breakdown.get('spec_score', 0)}.",
    ]
    if isinstance(rating, (int, float)):
        lines.append(f"It has a visible rating of {rating:.1f}/5, which supports baseline product quality.")
    else:
        lines.append("Rating data is missing, so confidence is reduced and the review sub-score stays conservative.")
    if specs:
        lines.append(f"Key strengths include: {', '.join(specs[:5])}.")
    return " ".join(lines)


def to_top_item(item: dict[str, Any], rank: int) -> dict[str, Any]:
    return {
        "rank": rank,
        "product_name": item.get("title"),
        "brand": item.get("brand"),
        "price": item.get("price"),
        "url": item.get("url"),
        "rating": item.get("rating"),
        "review_count": item.get("review_count"),
        "key_specs": (item.get("key_specs") or [])[:5],
        "score_total": item.get("score_total"),
        "score_breakdown": item.get("score_breakdown"),
        "recommendation_reason": build_reason(item, rank),
    }


def render_markdown(query: str, items: list[dict[str, Any]]) -> str:
    lines = [f"# Top 3 Earbuds for `{query}`", ""]
    for item in items:
        lines.extend(
            [
                f"## #{item['rank']} {item.get('product_name') or 'Unknown title'}",
                f"- Brand: {item.get('brand') or 'Unknown'}",
                f"- Price: ${item['price']:.2f}" if isinstance(item.get("price"), (int, float)) else "- Price: N/A",
                f"- Rating: {item['rating']}/5" if item.get("rating") is not None else "- Rating: N/A",
                f"- Review count: {item['review_count']}" if item.get("review_count") is not None else "- Review count: N/A",
                f"- Key specs: {', '.join(item.get('key_specs') or [])}",
                f"- Score total: {item.get('score_total')}",
                (
                    f"- Score breakdown: price={item.get('score_breakdown', {}).get('price_score')}, "
                    f"review={item.get('score_breakdown', {}).get('review_score')}, "
                    f"spec={item.get('score_breakdown', {}).get('spec_score')}"
                ),
                f"- Link: {item.get('url')}",
                "- Recommendation reason:",
                f"  {item.get('recommendation_reason')}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def run(input_path: Path, output_json: Path, output_md: Path, top_n: int) -> dict[str, Any]:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    items = payload.get("items", [])
    top = [to_top_item(item, idx + 1) for idx, item in enumerate(items[: max(1, top_n)])]

    result = {
        "query": payload.get("query"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(top),
        "items": top,
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    output_md.write_text(render_markdown(payload.get("query") or "", top), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Top-N recommendations")
    parser.add_argument("--input", default="data/clean/scored.json", help="Scored input JSON")
    parser.add_argument("--top", type=int, default=3, help="Top N recommendations")
    parser.add_argument("--output-json", default="output/top3.json", help="Output JSON path")
    parser.add_argument("--output-md", default="output/top3.md", help="Output Markdown path")
    args = parser.parse_args()

    result = run(Path(args.input), Path(args.output_json), Path(args.output_md), args.top)
    print(
        f"Generated top {result['count']} recommendations -> {args.output_json}, {args.output_md}"
    )


if __name__ == "__main__":
    main()
