-- schema.sql
-- PostgreSQL schema for amazon-products-api (Keepa backed)
-- Compatible with existing tables: products / prices / ratings / platforms / categories

BEGIN;

-- 1) core dimension tables
CREATE TABLE IF NOT EXISTS platforms (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);

-- 2) products (snapshot fields live here)
ALTER TABLE products
  ADD COLUMN IF NOT EXISTS title TEXT,
  ADD COLUMN IF NOT EXISTS brand TEXT,
  ADD COLUMN IF NOT EXISTS category TEXT,
  ADD COLUMN IF NOT EXISTS image_url TEXT,
  ADD COLUMN IF NOT EXISTS product_url TEXT,
  ADD COLUMN IF NOT EXISTS review_count INTEGER,
  ADD COLUMN IF NOT EXISTS review_rating NUMERIC(3,2),
  ADD COLUMN IF NOT EXISTS buybox_price NUMERIC(10,2),
  ADD COLUMN IF NOT EXISTS price NUMERIC(10,2);

-- created_at/updated_at 如果已存在则跳过
ALTER TABLE products
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- ensure foreign keys exist if missing
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'products_platform_id_fkey'
  ) THEN
    ALTER TABLE products
      ADD CONSTRAINT products_platform_id_fkey
      FOREIGN KEY (platform_id) REFERENCES platforms(id);
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'products_category_id_fkey'
  ) THEN
    ALTER TABLE products
      ADD CONSTRAINT products_category_id_fkey
      FOREIGN KEY (category_id) REFERENCES categories(id);
  END IF;
END$$;

-- 3) prices (history)
ALTER TABLE prices
  ADD COLUMN IF NOT EXISTS buybox_price NUMERIC(10,2),
  ADD COLUMN IF NOT EXISTS currency TEXT;

CREATE INDEX IF NOT EXISTS idx_prices_product_ts ON prices(product_id, ts DESC);

-- 4) ratings (history)
ALTER TABLE ratings
  ADD COLUMN IF NOT EXISTS review_count INTEGER;

CREATE INDEX IF NOT EXISTS idx_ratings_product_ts ON ratings(product_id, ts DESC);

-- 5) sales rank history (NEW)
-- NOTE: category is NOT NULL with default, so UNIQUE can use category directly (no COALESCE).
CREATE TABLE IF NOT EXISTS sales_rank_history (
  id BIGSERIAL PRIMARY KEY,
  product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL,
  category TEXT NOT NULL DEFAULT 'default',
  rank INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (product_id, ts, category)
);

CREATE INDEX IF NOT EXISTS idx_rank_product_ts ON sales_rank_history(product_id, ts DESC);

-- 6) snapshot indexes
CREATE INDEX IF NOT EXISTS idx_products_updated_at ON products(updated_at);
CREATE INDEX IF NOT EXISTS idx_products_platform_asin ON products(platform_id, asin);

COMMIT;
