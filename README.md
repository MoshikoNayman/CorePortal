# CorePortal

A personal finance & portfolio web app (Starlette + Uvicorn, SQLite-backed)
that bundles several tools behind one launcher.

## Tools

- `/` - Home launcher
- `/VPM` - **Virtual Portfolio Manager**: multi-tenant paper trading with
  owners, portfolios, cash ledger, simulated trades, live quotes, charts, and
  stock analysis (`/analyze`).
- `/BAT` - **Bank Account Tracker**: ledger, salary/spending, net-worth series,
  and transfer-to-VPM. (`/NWD` is a compatibility alias that redirects here.)
- `/OTD` - Out-the-Door vehicle pricing estimator.
- `/CVP` - Car Value / buy-sell TCO planner.

Operational endpoint:

- `/healthz` - JSON health probe (checks the database; `503` if unreachable).

## Layout

```
coreportal.py            Deployable ASGI entrypoint (the source of truth)
coreportal_core/         Layered, documented view of the same app:
  config.py                constants, paths, HTTP session, logging, quote cache
  db.py                    SQLite connection, schema, snapshots, CRUD
  quotes.py                market data (quotes/charts/news) + formatting
  services.py              orchestration bridging db + quotes
  views.py                 HTML rendering + shared stylesheet
  routes.py                request handlers, middleware, the ASGI app
apps/                    OTD/CVP static tools + shared theme CSS
VPM/                     SQLite database + portfolio backups (runtime state)
tests/                   unittest suite (no third-party deps)
```

Dependency direction is strictly downward:
`config → db → quotes → services → views → routes`.

Both `coreportal:app` and `coreportal_core:app` are valid ASGI targets and
refer to the same application instance.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 coreportal.py --host 0.0.0.0 --port 8081
```

Open http://localhost:8081

## Configuration

All settings are environment variables (see `.env.example`); sensible defaults
apply when unset.

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8081` | TCP port to bind |
| `COREPORTAL_BASE_PATH` | _(empty)_ | URL prefix behind a reverse proxy (e.g. `/coreportal`) |
| `COREPORTAL_HTTP_TIMEOUT` | `8` | Timeout (s) for outbound market-data calls |
| `COREPORTAL_QUOTE_TTL` | `60` | Current-quote cache lifetime (s) |
| `COREPORTAL_LOG_LEVEL` | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR` |
| `ALPHAVANTAGE_API_KEY` | _(empty)_ | Enables Alpha Vantage fallback paths |

## Tests

```bash
python3 -m unittest discover -s tests
```

The suite drives the ASGI app in-process (no sockets, no network) and covers
route smoke tests, the DB/formatting helpers, base-path logic, and the quote
cache.

## Deploy (systemd + Apache at /coreportal)

```bash
scp -r coreportal mnayman@<server-ip>:/var/www/html/coreportal
ssh mnayman@<server-ip>
cd /var/www/html/coreportal
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

sudo cp coreportal.service /etc/systemd/system/coreportal.service
sudo systemctl daemon-reload
sudo systemctl enable --now coreportal
```

Apache reverse proxy:

```apache
ProxyPreserveHost On
ProxyPass /coreportal http://127.0.0.1:8081/coreportal
ProxyPassReverse /coreportal http://127.0.0.1:8081/coreportal
RequestHeader set X-Forwarded-Proto "http"
RequestHeader set X-Forwarded-Prefix "/coreportal"
```

```bash
sudo a2enmod proxy proxy_http headers
sudo systemctl reload apache2
```

Open http://<server-ip>/coreportal

## Notes

- BAT is DB-first (SQLite is the source of truth); CSV is import/export only.
- VPM stays separate by design.
- Keep `.venv` for dependency isolation on the server. The bundled `.venv` is
  Linux-specific - recreate it locally on macOS if you develop here.
- To reduce preprod folder bloat:
  ```bash
  ./scripts/venv_maintenance.sh --status
  ./scripts/venv_maintenance.sh --prune
  ```

## License

Proprietary. See `LICENSE-PROPRIETARY.md` and `COPYRIGHT`.
