# Phase 1+2 Client Deliverables

## 1) Private repo + latest commits
- Repo: `https://github.com/shiyi-glitch/amazon-products-api.git`
- Add collaborator (owner run once, replace `<github_username>`):
```bash
gh api \
  --method PUT \
  /repos/shiyi-glitch/amazon-products-api/collaborators/<github_username> \
  -f permission=push
```
- Latest commits (`git log --oneline -10`):

```bash
309b0f2 Add Render blueprint and start script
c0c59f9 Deploy-ready: Render blueprint + API stable + Keepa sync
6ebc43a chore: remove secrets/logs and add gitignore
f780423 init amazon products api
```

> Note: current history has 4 commits in this clone. No hidden commits were found locally.

## 2) 0->run reproducible path (shortest)
```bash
cd /Users/project/amazon-products-api
source venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL="sqlite:///data/app.db"
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

## 3) Output quality optimization

### 3.1 Standard product URL output (Amazon canonical)
- Implemented in [`src/collect.py`](/Users/project/amazon-products-api/src/collect.py):
  - Extract ASIN from long redirect/search URLs.
  - Normalize to `https://www.amazon.com/dp/<ASIN>`.
- Verification command:
```bash
cd /Users/project/amazon-products-api
python3 collect.py --query "wireless earbuds" --limit 20 --enrich-max-items 5
python3 - <<'PY'
import json
from pathlib import Path
obj=json.loads(Path('data/raw/raw.json').read_text())
items=obj['items']
ratio=sum(1 for x in items if x.get('url','').startswith('https://www.amazon.com/dp/'))
print('canonical_url_ratio=', ratio, '/', len(items))
print('sample_urls=', [x['url'] for x in items[:3]])
PY
```

### 3.2 review_count backfill strategy (at least one stable source)
- Implemented in [`src/collect.py`](/Users/project/amazon-products-api/src/collect.py):
  - Source A: local CSV raw field.
  - Source B (stable): `data/app.db` (`products` + latest `ratings`) backfill by ASIN.
  - Source C (optional): live product-page enrichment for missing fields.
- Command (enable both DB backfill + live enrichment):
```bash
cd /Users/project/amazon-products-api
python3 collect.py --query "wireless earbuds" --limit 20 --sqlite-db data/app.db --enrich-max-items 20
python3 - <<'PY'
import json
from pathlib import Path
obj=json.loads(Path('data/raw/raw.json').read_text())
n=sum(1 for x in obj['items'] if x.get('review_count') is not None)
print('review_count_non_null=', n, '/', len(obj['items']))
PY
```

## 4) VPS deployment validation (systemd)

### 4.1 Service file
Create `/etc/systemd/system/amazon-products-api.service`:
```ini
[Unit]
Description=Amazon Products API
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/opt/amazon-products-api
EnvironmentFile=/opt/amazon-products-api/.env
ExecStart=/opt/amazon-products-api/venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

### 4.2 Start and check
```bash
sudo systemctl daemon-reload
sudo systemctl enable amazon-products-api
sudo systemctl restart amazon-products-api
sudo systemctl status amazon-products-api --no-pager
```

### 4.3 Curl verification
```bash
curl -v "http://127.0.0.1:8000/health"
curl -s -H "x-api-key: <YOUR_KEY>" "http://127.0.0.1:8000/recommend?query=wireless%20earbuds&limit=20&top=3" | python3 -m json.tool
```

Observed successful validation example:
- `GET /recommend?...` returned `HTTP/1.1 200 OK`
- Response includes `query/generated_at/count/items` and `count=3`.

## Secret handling policy
- Use `.env` for all secrets.
- `.env` is ignored by git.
- `Key.md` is ignored by git from now on.
- `.env.example` keeps only placeholders/non-sensitive defaults.
