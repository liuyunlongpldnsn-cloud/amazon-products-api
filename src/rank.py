import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Show top-N ranked earbuds")
    parser.add_argument("--input", default="data/clean/scored.json", help="Scored input JSON")
    parser.add_argument("--top", type=int, default=10, help="Top N to print")
    args = parser.parse_args()

    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    items = payload.get("items", [])[: max(1, args.top)]

    print(f"Top {len(items)} results from {args.input}")
    for idx, item in enumerate(items, start=1):
        title = item.get("title") or "Unknown title"
        brand = item.get("brand") or "Unknown brand"
        price = item.get("price")
        score = item.get("score_total", 0)
        price_text = f"${price:.2f}" if isinstance(price, (int, float)) else "N/A"
        print(f"{idx:>2}. score={score:>6} | {brand} | {price_text} | {title}")


if __name__ == "__main__":
    main()
