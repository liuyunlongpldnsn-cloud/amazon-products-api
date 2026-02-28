import os
from pathlib import Path
from tempfile import TemporaryDirectory
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional

from .db import get_engine
from .repo import list_products, get_product_by_asin, get_product_history
from src.clean import clean
from src.collect import collect
from src.recommend import run as recommend_run
from src.scoring import run as score_run

app = FastAPI(title="Amazon Products API", version="2.2")

def required_api_key() -> str:
    return (os.getenv("X_API_KEY") or "").strip()

@app.middleware("http")
async def api_key_auth(request: Request, call_next):
    required = required_api_key()
    if required:
        got = request.headers.get("x-api-key", "")
        if got != required:
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)

engine = get_engine()
PLATFORM_NAME = os.getenv("PLATFORM_NAME", "amazon_us")

@app.get("/products")
def products(
    min_rating: Optional[float] = None,
    max_price: Optional[float] = None,
    sort_by: Optional[str] = Query(None),
    order: str = Query("asc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
):
    return list_products(
        engine=engine,
        platform_name=PLATFORM_NAME,
        min_rating=min_rating,
        max_price=max_price,
        sort_by=sort_by,
        order=order,
        page=page,
        page_size=page_size,
    )

@app.get("/products/{asin}")
def product_detail(asin: str):
    p = get_product_by_asin(engine, PLATFORM_NAME, asin)
    if not p:
        raise HTTPException(status_code=404, detail="ASIN not found")
    return p

@app.get("/products/{asin}/history")
def product_history(
    asin: str,
    limit: int = Query(2000, ge=1, le=20000),
):
    h = get_product_history(engine, PLATFORM_NAME, asin, limit=limit)
    if not h:
        raise HTTPException(status_code=404, detail="ASIN not found")
    return h

@app.get("/health")
def health():
    return {"ok": True}


@app.get("/recommend")
def recommend(
    query: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=100),
    top: int = Query(3, ge=1, le=20),
):
    base_dir = Path(__file__).resolve().parents[1]
    source_file = base_dir / "data" / "real_products.csv"
    if not source_file.exists():
        raise HTTPException(status_code=500, detail="Source dataset not found")

    with TemporaryDirectory(prefix="earbuds_pipeline_") as tmp:
        tmpdir = Path(tmp)
        raw_file = tmpdir / "raw.json"
        clean_file = tmpdir / "clean.json"
        scored_file = tmpdir / "scored.json"
        out_json = tmpdir / "top3.json"
        out_md = tmpdir / "top3.md"
        report_file = tmpdir / "quality_report.txt"

        collect(
            query=query,
            limit=limit,
            source_file=source_file,
            output_file=raw_file,
        )
        clean(raw_path=raw_file, clean_path=clean_file, report_path=report_file)
        score_run(input_path=clean_file, output_path=scored_file)
        result = recommend_run(
            input_path=scored_file,
            output_json=out_json,
            output_md=out_md,
            top_n=top,
        )
    return result
