# VPS Deploy (systemd)

## 1) Prepare runtime
```bash
cd /opt/amazon-products-api
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env`:
```bash
DATABASE_URL=sqlite:///data/app.db
X_API_KEY=replace-with-your-real-key
```

## 2) systemd unit
`/etc/systemd/system/amazon-products-api.service`

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

## 3) Start service
```bash
sudo systemctl daemon-reload
sudo systemctl enable amazon-products-api
sudo systemctl restart amazon-products-api
sudo systemctl status amazon-products-api --no-pager
```

## 4) Verify
```bash
curl -v "http://127.0.0.1:8000/health"
curl -v -H "x-api-key: <YOUR_KEY>" "http://127.0.0.1:8000/recommend?query=wireless%20earbuds&limit=20&top=3"
```

Expected:
- `/health` returns `200` and body similar to `{"ok": true}`.
- `/recommend` returns `200` and JSON with `query/generated_at/count/items`.
