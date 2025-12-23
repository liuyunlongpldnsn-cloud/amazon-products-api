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


