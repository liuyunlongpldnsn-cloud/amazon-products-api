
cd /Users/project/amazon-products-api
bash run_pipeline.sh "wireless earbuds"
# or
python3 -m src.run_pipeline --query "wireless earbuds"
```

### Output path
- `output/top3.json`
- `output/top3.md`

### Day 2 collection command
```bash
cd /Users/project/amazon-products-api
python3 collect.py --query "wireless earbuds" --limit 20
```

Raw output:
- `data/raw/raw.json`

### Day 3 clean command
```bash
cd /Users/project/amazon-products-api
python3 clean.py --input data/raw/raw.json --output data/clean/clean.json --report output/quality_report.txt
```

Clean output:
- `data/clean/clean.json`
- `output/quality_report.txt`

### Day 4 scoring command
```bash
cd /Users/project/amazon-products-api
python3 scoring.py --input data/clean/clean.json --output data/clean/scored.json
python3 rank.py --input data/clean/scored.json --top 10
```

Scoring docs:
- `docs/SCORING_V1_DAY4.md`

### Day 5 recommendation command
```bash
cd /Users/project/amazon-products-api
python3 recommend.py --input data/clean/scored.json --top 3 --output-json output/top3.json --output-md output/top3.md
```

### Day 6 one-command pipeline
```bash
cd /Users/project/amazon-products-api
bash run_pipeline.sh "wireless earbuds"
```

### Day 6 optional API (`/recommend`)
Start API:
```bash
cd /Users/project/amazon-products-api
source venv/bin/activate
export DATABASE_URL="sqlite:///data/app.db"
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Test with curl:
```bash
curl -s "http://127.0.0.1:8000/recommend?query=wireless%20earbuds&limit=20&top=3" | python3 -m json.tool
```

### From 0 to run (repro steps)
```bash
cd /Users/project/amazon-products-api
source venv/bin/activate
pip install -r requirements.txt
bash run_pipeline.sh "wireless earbuds"
```

### Test keywords (copy and run)
```bash
bash run_pipeline.sh "wireless earbuds"
bash run_pipeline.sh "ANC earbuds"
bash run_pipeline.sh "sports earbuds"
```

### Demo outputs
- `data/raw/raw.json`
- `data/clean/clean.json`
- `data/clean/scored.json`
- `output/quality_report.txt`
- `output/top3.json`
- `output/top3.md`

### Import to SQLite DB (optional)
```bash
cd /Users/project/amazon-products-api
python3 import_to_db.py --input data/clean/clean.json --db data/app.db --platform amazon_us --reset-products
```

Verify DB:
```bash
sqlite3 data/app.db ".tables"
sqlite3 data/app.db "SELECT COUNT(*) AS total_products FROM products;"
sqlite3 data/app.db "SELECT asin, title, price, review_rating, review_count FROM products ORDER BY id DESC LIMIT 10;"
```

### Risk log and roadmap
- `docs/RISKS_NEXT_STEPS_DAY7.md`

### Planned folders
- `src/`
- `data/raw/`
- `data/clean/`
- `output/`

###进入目录
cd /Users/project/amazon-products-api/
###进入venv模式
source venv/bin/activate
###配置环境变量
ls -la .env
set -a
source .env
###查看目录
ls -lrt

###启动fast API
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

###测试健康性
curl -s http://127.0.0.1:8000/health


###环境依赖
# Keepa API
export KEEPA_API_KEY="YOUR_64_CHAR_KEEPA_KEY"

# 数据库（SQLAlchemy）
export DATABASE_URL="postgresql+psycopg2://user@127.0.0.1:5432/amazon_products"

# psql CLI（用于调试）
export PSQL_URL="postgresql://user@127.0.0.1:5432/amazon_products"



###初始化
psql "$PSQL_URL" -f schema.sql


###确认建表
psql "$PSQL_URL" -c "\dt"


###同步数据、同步执行

python -m scripts.sync_keepa --asins-file asins.txt --stats 0 --buybox 0


###启动API服务
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000


###curl 执行

curl -s "http://127.0.0.1:8000/products?page=1&page_size=5&sort_by=rating&order=desc" \
| python -m json.tool



ASIN=$(psql "$PSQL_URL" -t -A -c "select asin from products order by id desc limit 1;")

curl -s "http://127.0.0.1:8000/products/$ASIN" | python -m json.tool


curl -s "http://127.0.0.1:8000/products/$ASIN" | python -m json.tool
