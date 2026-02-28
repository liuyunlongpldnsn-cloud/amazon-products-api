import argparse
from pathlib import Path

from src.clean import clean
from src.collect import collect
from src.recommend import run as recommend_run
from src.scoring import run as score_run

def ensure_dirs(*paths: str) -> None:
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Earbuds ranking pipeline")
    parser.add_argument("--query", default="wireless earbuds", help="Search keyword")
    parser.add_argument("--limit", type=int, default=20, help="Collect limit")
    parser.add_argument("--top", type=int, default=3, help="Top N recommendations")
    parser.add_argument("--source-file", default="data/real_products.csv", help="Collection CSV source")
    args = parser.parse_args()

    ensure_dirs("data/raw", "data/clean", "output")
    collect(
        query=args.query,
        limit=max(1, args.limit),
        source_file=Path(args.source_file),
        output_file=Path("data/raw/raw.json"),
    )
    clean(
        raw_path=Path("data/raw/raw.json"),
        clean_path=Path("data/clean/clean.json"),
        report_path=Path("output/quality_report.txt"),
    )
    score_run(
        input_path=Path("data/clean/clean.json"),
        output_path=Path("data/clean/scored.json"),
    )
    result = recommend_run(
        input_path=Path("data/clean/scored.json"),
        output_json=Path("output/top3.json"),
        output_md=Path("output/top3.md"),
        top_n=max(1, args.top),
    )
    print(f"Pipeline completed for query: {args.query}")
    print(f"Collected limit: {args.limit}, recommended: {result['count']}")
    print("Generated: data/raw/raw.json, data/clean/clean.json, data/clean/scored.json")
    print("Generated: output/quality_report.txt, output/top3.json, output/top3.md")


if __name__ == "__main__":
    main()
