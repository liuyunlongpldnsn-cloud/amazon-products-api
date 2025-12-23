import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

def get_engine() -> Engine:
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL env var")
    return create_engine(DATABASE_URL, pool_pre_ping=True)
