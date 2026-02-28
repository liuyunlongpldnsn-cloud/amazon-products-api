# Risks and Next Steps (Day 7)

## Current Risks
- Anti-bot and crawl limitations: direct web scraping may fail due to dynamic rendering, geo restrictions, and anti-automation policies.
- Field sparsity: `review_count` is mostly missing in current sample source, causing review sub-score to be conservative.
- Price volatility: price changes frequently and can alter rank output between runs.
- Source structure drift: if source HTML/CSV field layout changes, collector parsing may degrade.
- Brand inference noise: when title quality is low, brand extraction can be inaccurate.

## Recommended Priorities
1. Add second source adapter (Best Buy or Walmart) and merge by canonical product key.
2. Add caching/versioned snapshots for raw and clean data to improve reproducibility.
3. Improve spec extraction using richer dictionaries and regex rules for battery/latency/waterproof levels.
4. Add validation tests for schema and score pipeline to catch regressions.
5. Expose a minimal API endpoint `GET /recommend?query=...` that serves `top3.json` from latest run.
