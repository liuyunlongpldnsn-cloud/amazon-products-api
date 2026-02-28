# Earbuds Aggregation MVP Scope (Day 1 Freeze)

## Goal
Build a small pipeline that takes a keyword (for example: `wireless earbuds`) and outputs Top 3 products in both JSON and Markdown.

## Category Scope
- In scope: wireless earbuds only.
- Out of scope (for now): headphones, speakers, other 3C categories.

## Data Sources (Phase 1)
- Primary source: Amazon.
- Secondary source: Best Buy.
- Day 1 implementation status: scaffold only, source adapters to be implemented from Day 2.

## Required Fields
Every product record should include:
- `title`
- `brand`
- `price`
- `url`
- `rating`
- `review_count`
- `key_specs` (2-5 items, e.g. battery/ANC/latency/waterproof)

## Output Contract
- `output/top3.json`
- `output/top3.md`

Each Top 3 item should include:
- product name, brand, price, link
- rating/review count (if available)
- key specs
- total score + sub-scores + recommendation reason

## Engineering Plan by Stage
- Day 1: scope + runnable scaffold
- Day 2: unified schema + collect v0 (>=20 rows)
- Day 3: clean + quality report
- Day 4: scoring model v1 (explainable)
- Day 5: Top 3 rendering and reasons
- Day 6: one-command pipeline (+ optional API)
- Day 7: reproducible package + risk log
