# amazon-products-api

## Quick Start (3-5 commands)
```bash
cd /Users/project/amazon-products-api
source venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL="sqlite:///data/app.db"
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

## API Check
```bash
curl -s "http://127.0.0.1:8000/health"
curl -s -H "x-api-key: <YOUR_KEY>" "http://127.0.0.1:8000/recommend?query=wireless%20earbuds&limit=20&top=3" | python3 -m json.tool
```

## Pipeline
```bash
bash run_pipeline.sh "wireless earbuds"
```

## Secrets
- Put all keys in `.env`.
- `.env` must not be committed.
- `Key.md` is local-only and ignored by git.
- Use `.env.example` as template.
