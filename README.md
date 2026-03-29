# CorePortal (Deploy)

Simple deploy folder for the personal finance and portfolio web app.

## What you get

- `coreportal.py` - app entrypoint
- `requirements.txt` - Python deps
- `coreportal.service` - optional systemd service
- `VPM/virtual_portfolio.db` - SQLite data file (created/used by app)

## Main pages

- `/` - Home
- `/VPM` - Virtual Portfolio Manager
- `/BAT` - bank ledger + owner overview + net worth view
- `/NWD` - compatibility alias to `/BAT`
- `/OTD` - Out-the-Door Estimator (original tool)
- `/CVP` - Car Value Planner (new buy/sell + TCO planner)

## CVP acronym

- `CVP` = `Car Value Planner`
- CVP is separate from OTD and focuses on buy/sell planning with TCO estimates

## Quick run

```bash
cd deploy/coreportal
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 coreportal.py --host 0.0.0.0 --port 8081
```

Open:

```text
http://<server-ip>:8081
```

## Optional systemd service

```bash
sudo cp coreportal.service /etc/systemd/system/coreportal.service
sudo systemctl daemon-reload
sudo systemctl enable coreportal
sudo systemctl start coreportal
```

## Deploy to a web server at /coreportal

Copy the folder to the server:

```bash
scp -r deploy/coreportal mnayman@<server-ip>:/var/www/html/coreportal
```

SSH to the server and run:

```bash
cd /var/www/html/coreportal
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Create the service:

```bash
sudo tee /etc/systemd/system/coreportal.service >/dev/null <<'EOF'
[Unit]
Description=CorePortal
After=network.target

[Service]
Type=simple
User=mnayman
WorkingDirectory=/var/www/html/coreportal
Environment=PORT=8081
Environment=COREPORTAL_BASE_PATH=/coreportal
ExecStart=/var/www/html/coreportal/.venv/bin/python /var/www/html/coreportal/coreportal.py --host 127.0.0.1 --port ${PORT}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now coreportal
```

Enable Apache proxy:

```bash
sudo a2enmod proxy proxy_http headers
sudo tee /etc/apache2/conf-available/coreportal-proxy.conf >/dev/null <<'EOF'
ProxyPreserveHost On
ProxyPass /coreportal http://127.0.0.1:8081/coreportal
ProxyPassReverse /coreportal http://127.0.0.1:8081/coreportal
RequestHeader set X-Forwarded-Proto "http"
RequestHeader set X-Forwarded-Prefix "/coreportal"
EOF

sudo a2enconf coreportal-proxy
sudo systemctl reload apache2
```

Open:

```text
http://<server-ip>/coreportal
```

## Notes

- BAT is DB-first (SQLite is source of truth)
- CSV should be import/export only, not live sync
- VPM stays separate by design
- Keep `.venv` for reliability and dependency isolation on the server
- To reduce folder bloat/noise in preprod, run:

```bash
./scripts/venv_maintenance.sh --status
./scripts/venv_maintenance.sh --prune
```

## License

This deploy bundle is proprietary.

See:

- `LICENSE-PROPRIETARY.md`
- `COPYRIGHT`
