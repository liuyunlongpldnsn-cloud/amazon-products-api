## API Examples (curl)


###Start_api

python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000


### 1. List products (filter + sort + pagination)

```bash
curl -s "http://127.0.0.1:8000/products?min_rating=4.3&max_price=30&sort_by=rating&order=desc&page=1&page_size=5" \
| python -m json.tool


###Next page

curl -s "http://127.0.0.1:8000/products?min_rating=4.3&max_price=30&sort_by=rating&order=desc&page=2&page_size=5" \
| python -m json.tool



###Product detail (existing ASIN)

curl -s "http://127.0.0.1:8000/products/B09DT48V16"| python -m json.tool



###Product detail (404 example)

curl -i "http://127.0.0.1:8000/products/AAAAAAAAAA"

