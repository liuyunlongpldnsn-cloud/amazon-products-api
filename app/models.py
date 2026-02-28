from pydantic import BaseModel
from typing import Optional, List

class Product(BaseModel):
    asin: str
    title: Optional[str] = None
    price: Optional[float] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    image: Optional[str] = None
    link: Optional[str] = None
    platform: Optional[str] = None

class ProductsPage(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[Product]
