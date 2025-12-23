import os
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional

from .db import get_engine
from .repo import list_products, get_product_by_asin, get_product_history

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
