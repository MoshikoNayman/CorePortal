from __future__ import annotations

import csv
import calendar
import html
import io
import json
import os
import shutil
import sqlite3
import subprocess
import time as time_module
from collections import defaultdict
from datetime import date
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode

import requests
from starlette.applications import Starlette
from starlette.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from starlette.routing import Route

APP_ROOT = Path(__file__).parent
VPM_DIR_CANDIDATES = [
    APP_ROOT / "VPM",
    APP_ROOT / "apps" / "VPM",
]
OTD_HTML_CANDIDATES = [
    APP_ROOT / "apps" / "OTD" / "otd_estimator.html",
    APP_ROOT / "OTD" / "otd_estimator.html",
    APP_ROOT / "tools" / "OTD" / "otd_estimator.html",
]
OTD_POLICY_CANDIDATES = [
    APP_ROOT / "apps" / "OTD" / "policy_years.json",
    APP_ROOT / "OTD" / "policy_years.json",
    APP_ROOT / "tools" / "OTD" / "policy_years.json",
]
CVP_HTML_CANDIDATES = [
    APP_ROOT / "apps" / "CVP" / "cvp_planner.html",
    APP_ROOT / "CVP" / "cvp_planner.html",
]
CVP_POLICY_CANDIDATES = [
    APP_ROOT / "apps" / "CVP" / "policy_years.json",
    APP_ROOT / "CVP" / "policy_years.json",
    APP_ROOT / "apps" / "OTD" / "policy_years.json",
]
THEME_CSS_CANDIDATES = [
    APP_ROOT / "apps" / "shared" / "coreportal_theme.css",
    APP_ROOT / "shared" / "coreportal_theme.css",
]
APP_TITLE = "VPM · Virtual Portfolio Manager"
APP_AUTHOR = "by Moshiko Nayman"
APP_HOME_TITLE = "CorePortal"
APP_COPYRIGHT = "© 2026 Moshiko Nayman · Proprietary License · All rights reserved."
COMMON_PAGE_MAX_WIDTH = 1240


def normalize_base_path(raw_path: str) -> str:
    clean = (raw_path or "").strip()
    if not clean or clean == "/":
        return ""
    if not clean.startswith("/"):
        clean = f"/{clean}"
    return clean.rstrip("/")


def with_base_path(path: str) -> str:
    clean = path if path.startswith("/") else f"/{path}"
    if BASE_PATH:
        return BASE_PATH if clean == "/" else f"{BASE_PATH}{clean}"
    return clean


BASE_PATH = normalize_base_path(os.getenv("COREPORTAL_BASE_PATH", ""))
ROOT_PATH = with_base_path("/")
ASSET_THEME_PATH = with_base_path("/assets/coreportal_theme.css")
OPEN_APP_PATH = with_base_path("/apps/open/{app_id}")
VPM_PATH = with_base_path("/VPM")
CVP_PATH = with_base_path("/CVP")
OTD_PATH = with_base_path("/OTD")
TRACKER_PATH = with_base_path("/BAT")
DEFAULT_PORTFOLIO_NAME = "Main Portfolio"
DEFAULT_TENANTS = ("Moshiko",)
MONEY_QUANT = Decimal("0.01")
SHARE_QUANT = Decimal("0.0001")

ANALYSIS_MODULES = [
    "price",
    "summaryDetail",
    "financialData",
    "defaultKeyStatistics",
    "assetProfile",
    "earnings",
    "earningsTrend",
    "incomeStatementHistoryQuarterly",
    "incomeStatementHistory",
    "cashflowStatementHistory",
    "cashflowStatementHistoryQuarterly",
    "balanceSheetHistory",
    "balanceSheetHistoryQuarterly",
    "recommendationTrend",
    "calendarEvents",
]

ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()


def resolve_vpm_dir() -> Path:
    for candidate in VPM_DIR_CANDIDATES:
        if candidate.exists():
            return candidate
    return VPM_DIR_CANDIDATES[0]


VPM_DIR = resolve_vpm_dir()
DB_PATH = VPM_DIR / "virtual_portfolio.db"
BACKUP_DIR = VPM_DIR / "portfolio_backups"
LEGACY_DB_PATHS = [APP_ROOT / "virtual_portfolio.db"]
LEGACY_BACKUP_DIRS = [APP_ROOT / "portfolio_backups"]

def resolve_otd_html_path() -> Path:
    for candidate in OTD_HTML_CANDIDATES:
        if candidate.exists():
            return candidate
    return OTD_HTML_CANDIDATES[0]


def resolve_otd_policy_path() -> Path:
    for candidate in OTD_POLICY_CANDIDATES:
        if candidate.exists():
            return candidate
    return OTD_POLICY_CANDIDATES[0]


def resolve_cvp_html_path() -> Path:
    for candidate in CVP_HTML_CANDIDATES:
        if candidate.exists():
            return candidate
    return CVP_HTML_CANDIDATES[0]


def resolve_cvp_policy_path() -> Path:
    for candidate in CVP_POLICY_CANDIDATES:
        if candidate.exists():
            return candidate
    return CVP_POLICY_CANDIDATES[0]


def resolve_theme_css_path() -> Path:
    for candidate in THEME_CSS_CANDIDATES:
        if candidate.exists():
            return candidate
    return THEME_CSS_CANDIDATES[0]


OTD_HTML_PATH = resolve_otd_html_path()
OTD_POLICY_PATH = resolve_otd_policy_path()
CVP_HTML_PATH = resolve_cvp_html_path()
CVP_POLICY_PATH = resolve_cvp_policy_path()
THEME_CSS_PATH = resolve_theme_css_path()


def shared_theme_css() -> str:
    return (
        f"                                @import url('{ASSET_THEME_PATH}');\n"
        + """
        :root {
          --bg: #f4f7fb;
          --card: #ffffff;
          --ink: #11203b;
          --muted: #5f6b85;
          --line: #d9e1ee;
          --brand: #2952ff;
          --brand-soft: #edf2ff;
          --gain: #0b7d23;
          --loss: #b22121;
          --shadow: 0 10px 28px rgba(20, 42, 90, 0.08);
        }
        * { box-sizing: border-box; }
        body { margin: 0; font-family: var(--font-ui); background: var(--bg); color: var(--ink); }
        .footer { margin-top: 18px; padding: 14px 4px 6px 4px; text-align: center; color: var(--muted); font-size: 12px; }
    """
    )


def ensure_vpm_storage_layout() -> None:
    VPM_DIR.mkdir(parents=True, exist_ok=True)


def migrate_legacy_vpm_storage() -> None:
    ensure_vpm_storage_layout()

    if not DB_PATH.exists():
        for legacy_db in LEGACY_DB_PATHS:
            if legacy_db.exists() and legacy_db != DB_PATH:
                shutil.move(str(legacy_db), str(DB_PATH))
                break

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    for legacy_backup_dir in LEGACY_BACKUP_DIRS:
        if not legacy_backup_dir.exists() or legacy_backup_dir == BACKUP_DIR:
            continue

        for item in legacy_backup_dir.iterdir():
            target = BACKUP_DIR / item.name
            if target.exists():
                continue
            shutil.move(str(item), str(target))

        try:
            legacy_backup_dir.rmdir()
        except OSError:
            pass


APP_REGISTRY: list[dict[str, Any]] = [
    {
        "id": "tracker",
        "name": "BAT · Bank Account Tracker",
        "description": "Bank ledger, cashflow workspace, and built-in owner overview for balances and net worth.",
        "type": "internal",
        "open_path": TRACKER_PATH,
    },
    {
        "id": "portfolio",
        "name": "VPM · Virtual Portfolio Manager",
        "description": "Paper trading workspace with owners, portfolios, simulated orders, and stock analysis.",
        "type": "internal",
        "open_path": VPM_PATH,
    },
    {
        "id": "otd",
        "name": "OTD · Out-the-Door Estimator",
        "description": "Out-the-door vehicle pricing calculator. Independent from the portfolio app.",
        "type": "static_html",
        "open_path": OTD_PATH,
        "file_path": OTD_HTML_PATH,
    },
    {
        "id": "cvp",
        "name": "CVP · Buy/Sell TCO Planner",
        "description": "Vehicle buy/replace planner with out-the-door math and ownership guidance.",
        "type": "static_html",
        "open_path": CVP_PATH,
        "file_path": CVP_HTML_PATH,
    },
]

APP_PROCESSES: dict[str, subprocess.Popen[Any]] = {}


def to_decimal(value: Any, quant: Decimal | None = None) -> Decimal:
    if isinstance(value, Decimal):
        converted = value
    else:
        converted = Decimal(str(value))
    if quant is not None:
        return converted.quantize(quant)
    return converted


def parse_positive_decimal(value: str, quant: Decimal | None = None) -> Decimal:
    try:
        parsed = to_decimal(value, quant)
    except (InvalidOperation, ValueError) as error:
        raise ValueError("Invalid number format") from error
    if parsed <= 0:
        raise ValueError("Value must be greater than zero")
    return parsed


def parse_optional_int(value: str | None) -> int | None:
    if value in {None, ""}:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def shift_months(input_date: date, months: int) -> date:
    year = input_date.year + (input_date.month - 1 + months) // 12
    month = (input_date.month - 1 + months) % 12 + 1
    day = min(input_date.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def db_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def column_exists(connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row["name"] == column_name for row in rows)


def ensure_default_portfolio(connection: sqlite3.Connection, tenant_name: str) -> tuple[int, int]:
    clean_name = tenant_name.strip()
    if not clean_name:
        raise ValueError("Tenant name is required")

    existing_tenant = connection.execute(
        "SELECT id, name FROM tenants WHERE LOWER(name)=LOWER(?)",
        (clean_name,),
    ).fetchone()
    if existing_tenant is None:
        connection.execute("INSERT INTO tenants (name) VALUES (?)", (clean_name,))
        tenant_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    else:
        tenant_id = int(existing_tenant["id"])

    existing_portfolio = connection.execute(
        "SELECT id FROM portfolios WHERE tenant_id=? AND LOWER(name)=LOWER(?)",
        (tenant_id, DEFAULT_PORTFOLIO_NAME),
    ).fetchone()
    if existing_portfolio is None:
        connection.execute(
            "INSERT INTO portfolios (tenant_id, name) VALUES (?, ?)",
            (tenant_id, DEFAULT_PORTFOLIO_NAME),
        )
        portfolio_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    else:
        portfolio_id = int(existing_portfolio["id"])

    return tenant_id, portfolio_id


def ensure_default_bank_account(connection: sqlite3.Connection, tenant_id: int) -> int:
    existing = connection.execute(
        "SELECT id FROM bank_accounts WHERE tenant_id=? AND LOWER(name)=LOWER(?)",
        (tenant_id, "Main Account"),
    ).fetchone()
    if existing is not None:
        return int(existing["id"])

    connection.execute(
        "INSERT INTO bank_accounts (tenant_id, name, account_type) VALUES (?, ?, ?)",
        (tenant_id, "Main Account", "checking"),
    )
    return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])


def init_db() -> None:
    with db_connection() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS tenants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS portfolios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tenant_id, name),
                FOREIGN KEY (tenant_id) REFERENCES tenants(id)
            );

            CREATE TABLE IF NOT EXISTS cash_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER,
                amount TEXT NOT NULL,
                entry_date TEXT NOT NULL,
                note TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (portfolio_id) REFERENCES portfolios(id)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL CHECK(side IN ('buy', 'sell')),
                quantity TEXT NOT NULL,
                price TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (portfolio_id) REFERENCES portfolios(id)
            );

            CREATE TABLE IF NOT EXISTS bank_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                account_type TEXT NOT NULL DEFAULT 'checking',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tenant_id, name),
                FOREIGN KEY (tenant_id) REFERENCES tenants(id)
            );

            CREATE TABLE IF NOT EXISTS bank_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                amount TEXT NOT NULL,
                entry_date TEXT NOT NULL,
                category TEXT,
                note TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (account_id) REFERENCES bank_accounts(id)
            );
            """
        )

        if not column_exists(connection, "cash_ledger", "portfolio_id"):
            connection.execute("ALTER TABLE cash_ledger ADD COLUMN portfolio_id INTEGER")

        if not column_exists(connection, "trades", "portfolio_id"):
            connection.execute("ALTER TABLE trades ADD COLUMN portfolio_id INTEGER")

        for tenant_name in DEFAULT_TENANTS:
            tenant_id, _ = ensure_default_portfolio(connection, tenant_name)
            ensure_default_bank_account(connection, tenant_id)

        tenant_rows = connection.execute("SELECT id FROM tenants").fetchall()
        for tenant in tenant_rows:
            ensure_default_bank_account(connection, int(tenant["id"]))

        _, default_portfolio_id = ensure_default_portfolio(connection, DEFAULT_TENANTS[0])
        connection.execute(
            "UPDATE cash_ledger SET portfolio_id=? WHERE portfolio_id IS NULL",
            (default_portfolio_id,),
        )
        connection.execute(
            "UPDATE trades SET portfolio_id=? WHERE portfolio_id IS NULL",
            (default_portfolio_id,),
        )


def ensure_backup_dir() -> None:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def list_backups(limit: int = 20) -> list[str]:
    ensure_backup_dir()
    files = [path.name for path in BACKUP_DIR.glob("*.sqlite3") if path.is_file()]
    files.sort(reverse=True)
    return files[:limit]


def create_db_snapshot(prefix: str = "snapshot") -> str:
    ensure_backup_dir()
    safe_prefix = "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in prefix).strip("-") or "snapshot"
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    snapshot_name = f"{safe_prefix}-{timestamp}.sqlite3"
    snapshot_path = BACKUP_DIR / snapshot_name

    with sqlite3.connect(DB_PATH) as source, sqlite3.connect(snapshot_path) as destination:
        source.backup(destination)

    return snapshot_name


def restore_db_snapshot(snapshot_name: str) -> tuple[bool, str, int | None, int | None]:
    available = set(list_backups(limit=200))
    if snapshot_name not in available:
        return False, "Selected snapshot was not found", None, None

    snapshot_path = BACKUP_DIR / snapshot_name
    safety_snapshot = create_db_snapshot("pre-restore")
    temp_path = BACKUP_DIR / f"restore-check-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.sqlite3"

    try:
        shutil.copy2(snapshot_path, temp_path)
        with sqlite3.connect(temp_path) as connection:
            check = connection.execute("PRAGMA quick_check").fetchone()
            if not check or str(check[0]).lower() != "ok":
                return False, "Selected snapshot failed integrity check", None, None

        shutil.copy2(snapshot_path, DB_PATH)
        init_db()
        with db_connection() as connection:
            tenants, current_tenant, portfolios, current_portfolio = resolve_selection(connection, None, None)
            return (
                True,
                f"Snapshot restored: {snapshot_name} (safety snapshot: {safety_snapshot})",
                int(current_tenant["id"]),
                int(current_portfolio["id"]),
            )
    except Exception as error:
        return False, f"Snapshot restore failed: {error}", None, None
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def restore_default_state() -> tuple[bool, str, int | None, int | None]:
    safety_snapshot = create_db_snapshot("pre-defaults")
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_db()
    with db_connection() as connection:
        tenants, current_tenant, portfolios, current_portfolio = resolve_selection(connection, None, None)
        return (
            True,
            f"Defaults restored (safety snapshot: {safety_snapshot})",
            int(current_tenant["id"]),
            int(current_portfolio["id"]),
        )


def zeroize_portfolio(portfolio_id: int) -> tuple[bool, str]:
    safety_snapshot = create_db_snapshot("pre-zeroize")
    with db_connection() as connection:
        portfolio = connection.execute("SELECT id FROM portfolios WHERE id=?", (portfolio_id,)).fetchone()
        if portfolio is None:
            return False, "Selected portfolio was not found"

        connection.execute("DELETE FROM trades WHERE portfolio_id=?", (portfolio_id,))
        connection.execute("DELETE FROM cash_ledger WHERE portfolio_id=?", (portfolio_id,))

    return True, f"Current portfolio zeroized (safety snapshot: {safety_snapshot})"


def zeroize_bank_account(account_id: int) -> tuple[bool, str]:
    safety_snapshot = create_db_snapshot("pre-bat-zeroize")
    with db_connection() as connection:
        account = connection.execute("SELECT id, name FROM bank_accounts WHERE id=?", (account_id,)).fetchone()
        if account is None:
            return False, "Selected account was not found"

        connection.execute("DELETE FROM bank_ledger WHERE account_id=?", (account_id,))

    return True, f"BAT account ledger zeroized (safety snapshot: {safety_snapshot})"


def load_tenants(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute("SELECT id, name FROM tenants ORDER BY name COLLATE NOCASE ASC, id ASC").fetchall()


def load_portfolios(connection: sqlite3.Connection, tenant_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        "SELECT id, tenant_id, name FROM portfolios WHERE tenant_id=? ORDER BY name COLLATE NOCASE ASC, id ASC",
        (tenant_id,),
    ).fetchall()


def resolve_selection(
    connection: sqlite3.Connection,
    selected_tenant_id: int | None,
    selected_portfolio_id: int | None,
) -> tuple[list[sqlite3.Row], sqlite3.Row, list[sqlite3.Row], sqlite3.Row]:
    tenants = load_tenants(connection)
    if not tenants:
        ensure_default_portfolio(connection, DEFAULT_TENANTS[0])
        tenants = load_tenants(connection)

    current_tenant = next((tenant for tenant in tenants if int(tenant["id"]) == selected_tenant_id), tenants[0])
    portfolios = load_portfolios(connection, int(current_tenant["id"]))
    if not portfolios:
        ensure_default_portfolio(connection, str(current_tenant["name"]))
        portfolios = load_portfolios(connection, int(current_tenant["id"]))

    current_portfolio = next(
        (portfolio for portfolio in portfolios if int(portfolio["id"]) == selected_portfolio_id),
        portfolios[0],
    )
    return tenants, current_tenant, portfolios, current_portfolio


def create_tenant(tenant_name: str) -> tuple[bool, str, int | None, int | None]:
    clean_name = tenant_name.strip()
    if not clean_name:
        return False, "Owner name is required", None, None

    with db_connection() as connection:
        existing = connection.execute(
            "SELECT id, name FROM tenants WHERE LOWER(name)=LOWER(?)",
            (clean_name,),
        ).fetchone()
        if existing is not None:
            portfolios = load_portfolios(connection, int(existing["id"]))
            portfolio_id = int(portfolios[0]["id"]) if portfolios else None
            return False, "Owner already exists", int(existing["id"]), portfolio_id

        tenant_id, portfolio_id = ensure_default_portfolio(connection, clean_name)
        ensure_default_bank_account(connection, tenant_id)

    return True, "Owner created", tenant_id, portfolio_id


def create_portfolio(tenant_id: int, portfolio_name: str) -> tuple[bool, str, int | None]:
    clean_name = portfolio_name.strip()
    if not clean_name:
        return False, "Portfolio name is required", None

    with db_connection() as connection:
        tenant = connection.execute("SELECT id FROM tenants WHERE id=?", (tenant_id,)).fetchone()
        if tenant is None:
            return False, "Selected tenant was not found", None

        existing = connection.execute(
            "SELECT id FROM portfolios WHERE tenant_id=? AND LOWER(name)=LOWER(?)",
            (tenant_id, clean_name),
        ).fetchone()
        if existing is not None:
            return False, "Portfolio already exists for that tenant", int(existing["id"])

        connection.execute(
            "INSERT INTO portfolios (tenant_id, name) VALUES (?, ?)",
            (tenant_id, clean_name),
        )
        portfolio_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    return True, "Portfolio created", portfolio_id


def delete_owner(owner_id: int) -> tuple[bool, str, int | None, int | None]:
    with db_connection() as connection:
        owners = load_tenants(connection)
        if len(owners) <= 1:
            return False, "You must keep at least one owner", None, None

        owner = connection.execute("SELECT id FROM tenants WHERE id=?", (owner_id,)).fetchone()
        if owner is None:
            fallback_owner = owners[0]
            fallback_portfolios = load_portfolios(connection, int(fallback_owner["id"]))
            fallback_portfolio_id = int(fallback_portfolios[0]["id"]) if fallback_portfolios else None
            return False, "Owner not found", int(fallback_owner["id"]), fallback_portfolio_id

        portfolio_rows = load_portfolios(connection, owner_id)
        portfolio_ids = [int(row["id"]) for row in portfolio_rows]

        if portfolio_ids:
            placeholders = ",".join("?" for _ in portfolio_ids)
            connection.execute(f"DELETE FROM cash_ledger WHERE portfolio_id IN ({placeholders})", portfolio_ids)
            connection.execute(f"DELETE FROM trades WHERE portfolio_id IN ({placeholders})", portfolio_ids)

        bank_accounts = connection.execute(
            "SELECT id FROM bank_accounts WHERE tenant_id=?",
            (owner_id,),
        ).fetchall()
        account_ids = [int(row["id"]) for row in bank_accounts]
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            connection.execute(f"DELETE FROM bank_ledger WHERE account_id IN ({placeholders})", account_ids)
        connection.execute("DELETE FROM bank_accounts WHERE tenant_id=?", (owner_id,))

        connection.execute("DELETE FROM portfolios WHERE tenant_id=?", (owner_id,))
        connection.execute("DELETE FROM tenants WHERE id=?", (owner_id,))

        remaining = load_tenants(connection)
        if not remaining:
            new_owner_id, new_portfolio_id = ensure_default_portfolio(connection, DEFAULT_TENANTS[0])
            return True, "Owner deleted", new_owner_id, new_portfolio_id

        next_owner = remaining[0]
        next_portfolios = load_portfolios(connection, int(next_owner["id"]))
        if not next_portfolios:
            ensure_default_portfolio(connection, str(next_owner["name"]))
            next_portfolios = load_portfolios(connection, int(next_owner["id"]))
        next_portfolio_id = int(next_portfolios[0]["id"]) if next_portfolios else None
        return True, "Owner deleted", int(next_owner["id"]), next_portfolio_id


def remove_legacy_default_owner_maya() -> None:
    if "Maya" in DEFAULT_TENANTS:
        return

    maya_owner_id: int | None = None
    with db_connection() as connection:
        owners = load_tenants(connection)
        maya_row = next((row for row in owners if str(row["name"]).strip().lower() == "maya"), None)
        if maya_row is None:
            return
        maya_owner_id = int(maya_row["id"])

        if len(owners) <= 1:
            tenant_id, _ = ensure_default_portfolio(connection, DEFAULT_TENANTS[0])
            ensure_default_bank_account(connection, tenant_id)

    if maya_owner_id is not None:
        delete_owner(maya_owner_id)


def get_cash_added(connection: sqlite3.Connection, portfolio_id: int) -> Decimal:
    row = connection.execute(
        "SELECT COALESCE(SUM(CAST(amount AS REAL)), 0) AS total FROM cash_ledger WHERE portfolio_id=?",
        (portfolio_id,),
    ).fetchone()
    return to_decimal(row["total"], MONEY_QUANT)


def get_trade_totals(connection: sqlite3.Connection, portfolio_id: int) -> tuple[Decimal, Decimal]:
    row = connection.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN side='buy' THEN CAST(quantity AS REAL) * CAST(price AS REAL) END), 0) AS total_buys,
          COALESCE(SUM(CASE WHEN side='sell' THEN CAST(quantity AS REAL) * CAST(price AS REAL) END), 0) AS total_sells
        FROM trades
        WHERE portfolio_id=?
        """,
        (portfolio_id,),
    ).fetchone()
    total_buys = to_decimal(row["total_buys"], MONEY_QUANT)
    total_sells = to_decimal(row["total_sells"], MONEY_QUANT)
    return total_buys, total_sells


def get_cash_balance(connection: sqlite3.Connection, portfolio_id: int) -> Decimal:
    cash_added = get_cash_added(connection, portfolio_id)
    total_buys, total_sells = get_trade_totals(connection, portfolio_id)
    return to_decimal(cash_added - total_buys + total_sells, MONEY_QUANT)


def get_open_quantity(connection: sqlite3.Connection, portfolio_id: int, symbol: str) -> Decimal:
    row = connection.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN side='buy' THEN CAST(quantity AS REAL) ELSE -CAST(quantity AS REAL) END), 0) AS net_qty
        FROM trades
        WHERE portfolio_id=? AND UPPER(symbol)=UPPER(?)
        """,
        (portfolio_id, symbol),
    ).fetchone()
    return to_decimal(row["net_qty"], SHARE_QUANT)


def fetch_quotes(symbols: list[str]) -> dict[str, Decimal]:
    unique_symbols = sorted({symbol.upper().strip() for symbol in symbols if symbol.strip()})
    if not unique_symbols:
        return {}

    try:
        response = requests.get(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": ",".join(unique_symbols)},
            timeout=6,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        payload = {}

    quotes: dict[str, Decimal] = {}
    results = payload.get("quoteResponse", {}).get("result", [])
    for item in results:
        symbol = str(item.get("symbol", "")).upper().strip()
        price = item.get("regularMarketPrice")
        if symbol and price is not None:
            try:
                quotes[symbol] = to_decimal(price, MONEY_QUANT)
            except (InvalidOperation, ValueError):
                continue

    for symbol in unique_symbols:
        if symbol not in quotes:
            fallback = fetch_current_quote_stooq(symbol)
            if fallback is not None:
                quotes[symbol] = fallback
    return quotes


def stooq_symbol(symbol: str) -> str:
    clean = symbol.upper().strip()
    if not clean:
        return clean
    if "." in clean:
        return clean.lower()
    return f"{clean.lower()}.us"


def fetch_current_quote_stooq(symbol: str) -> Decimal | None:
    stooq = stooq_symbol(symbol)
    try:
        response = requests.get(
            "https://stooq.com/q/l/",
            params={"s": stooq, "f": "sd2t2ohlcv", "h": "", "e": "csv"},
            timeout=6,
        )
        response.raise_for_status()
        lines = response.text.strip().splitlines()
        if len(lines) < 2:
            return None
        values = lines[1].split(",")
        if len(values) < 7:
            return None
        close_value = values[6]
        if close_value in {"", "N/D", "0"}:
            return None
        return to_decimal(close_value, MONEY_QUANT)
    except Exception:
        return None


def fetch_historical_close_stooq(symbol: str, trade_day: str) -> Decimal | None:
    stooq = stooq_symbol(symbol)
    try:
        target_day = date.fromisoformat(trade_day)
    except ValueError:
        return None

    try:
        response = requests.get(
            "https://stooq.com/q/d/l/",
            params={"s": stooq, "i": "d"},
            timeout=8,
        )
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        chosen_day: date | None = None
        chosen_close: Decimal | None = None
        for row in reader:
            day_value = row.get("Date", "")
            if not day_value:
                continue
            try:
                row_day = date.fromisoformat(day_value)
            except ValueError:
                continue
            if row_day > target_day:
                continue

            close_value = row.get("Close", "")
            if close_value in {"", "N/D", "0"}:
                continue

            try:
                parsed_close = to_decimal(close_value, MONEY_QUANT)
            except (InvalidOperation, ValueError):
                continue

            if chosen_day is None or row_day > chosen_day:
                chosen_day = row_day
                chosen_close = parsed_close

        return chosen_close
    except Exception:
        return None


def fetch_chart_series_stooq(symbol: str, max_points: int = 1500) -> tuple[list[int], list[float]]:
    stooq = stooq_symbol(symbol)
    try:
        response = requests.get(
            "https://stooq.com/q/d/l/",
            params={"s": stooq, "i": "d"},
            timeout=8,
        )
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        points: list[tuple[int, float]] = []
        for row in reader:
            day_value = row.get("Date", "")
            close_value = row.get("Close", "")
            if not day_value or close_value in {"", "N/D", "0"}:
                continue

            try:
                row_day = date.fromisoformat(day_value)
            except ValueError:
                continue

            parsed_close = as_float(close_value)
            if parsed_close is None:
                continue

            ts = int(datetime.combine(row_day, time.min, tzinfo=timezone.utc).timestamp())
            points.append((ts, parsed_close))

        if not points:
            return [], []

        points.sort(key=lambda item: item[0])
        if max_points > 0 and len(points) > max_points:
            points = points[-max_points:]

        return [item[0] for item in points], [item[1] for item in points]
    except Exception:
        return [], []


def fetch_historical_close(symbol: str, trade_day: str) -> Decimal | None:
    clean_symbol = symbol.upper().strip()
    if not clean_symbol:
        return None

    try:
        parsed_date = date.fromisoformat(trade_day)
    except ValueError:
        return None

    day_start = datetime.combine(parsed_date, time.min, tzinfo=timezone.utc)
    next_day_start = datetime.combine(parsed_date + timedelta(days=1), time.min, tzinfo=timezone.utc)
    window_start = datetime.combine(parsed_date - timedelta(days=14), time.min, tzinfo=timezone.utc)

    try:
        response = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{clean_symbol}",
            params={
                "period1": int(day_start.timestamp()),
                "period2": int(next_day_start.timestamp()),
                "interval": "1d",
                "events": "history",
                "includePrePost": "false",
            },
            timeout=6,
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("chart", {}).get("result", [{}])[0]
        closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
        timestamps = result.get("timestamp", [])
    except Exception:
        closes = []
        timestamps = []

    chosen_close: Decimal | None = None
    chosen_day: date | None = None
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        quote_day = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        if quote_day <= parsed_date:
            if chosen_day is None or quote_day > chosen_day:
                try:
                    chosen_close = to_decimal(close, MONEY_QUANT)
                    chosen_day = quote_day
                except (InvalidOperation, ValueError):
                    continue

    if chosen_close is not None:
        return chosen_close

    try:
        response = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{clean_symbol}",
            params={
                "period1": int(window_start.timestamp()),
                "period2": int(next_day_start.timestamp()),
                "interval": "1d",
                "events": "history",
                "includePrePost": "false",
            },
            timeout=6,
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("chart", {}).get("result", [{}])[0]
        closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
        timestamps = result.get("timestamp", [])
        chosen_close = None
        chosen_day = None
        for ts, close in zip(timestamps, closes):
            if close is None:
                continue
            quote_day = datetime.fromtimestamp(ts, tz=timezone.utc).date()
            if quote_day <= parsed_date:
                if chosen_day is None or quote_day > chosen_day:
                    try:
                        chosen_close = to_decimal(close, MONEY_QUANT)
                        chosen_day = quote_day
                    except (InvalidOperation, ValueError):
                        continue
        if chosen_close is not None:
            return chosen_close
    except Exception:
        pass

    return fetch_historical_close_stooq(clean_symbol, trade_day)


def unwrap_value(value: Any) -> Any:
    if isinstance(value, dict):
        if "raw" in value:
            return value.get("raw")
        if "fmt" in value:
            return value.get("fmt")
    return value


def nested_get(data: Any, *path: Any) -> Any:
    current = data
    for key in path:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and isinstance(key, int):
            if key < 0 or key >= len(current):
                return None
            current = current[key]
        else:
            return None
        if current is None:
            return None
    return unwrap_value(current)


def as_float(value: Any) -> float | None:
    value = unwrap_value(value)
    if value in {None, "", "N/A"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_compact_number(value: float | None) -> str:
    if value is None:
        return "N/A"
    absolute = abs(value)
    if absolute >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"
    if absolute >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if absolute >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    return f"${value:,.2f}"


def format_plain_number(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{value:,.{digits}f}"


def format_percent_float(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:.{digits}f}%"


def format_currency(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"${value:,.2f}"


def compute_return_pct(current_price: float | None, previous_price: float | None) -> str:
    if current_price is None or previous_price is None or previous_price == 0:
        return "N/A"
    return f"{((current_price - previous_price) / previous_price) * 100:.2f}%"


def sma(values: list[float], window: int) -> float | None:
    if len(values) < window or window <= 0:
        return None
    subset = values[-window:]
    if not subset:
        return None
    return sum(subset) / len(subset)


def extract_series_from_chart(payload: dict[str, Any]) -> tuple[list[int], list[float]]:
    timestamps = nested_get(payload, "chart", "result", 0, "timestamp") or []
    closes = nested_get(payload, "chart", "result", 0, "indicators", "quote", 0, "close") or []
    points: list[tuple[int, float]] = []
    for ts, close in zip(timestamps, closes):
        c = as_float(close)
        if c is not None:
            points.append((int(ts), c))
    if not points:
        return [], []
    return [item[0] for item in points], [item[1] for item in points]


def previous_price_by_days(timestamps: list[int], closes: list[float], days_back: int) -> float | None:
    if not timestamps or not closes or days_back <= 0:
        return None
    target_ts = timestamps[-1] - (days_back * 86400)
    chosen: float | None = None
    for ts, close in zip(timestamps, closes):
        if ts <= target_ts:
            chosen = close
        else:
            break
    return chosen


def format_series_labels(timestamps: list[int]) -> list[str]:
    labels: list[str] = []
    for ts in timestamps:
        try:
            labels.append(datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"))
        except Exception:
            labels.append("")
    return labels


def fetch_quote_summary(symbol: str) -> dict[str, Any]:
    clean_symbol = symbol.upper().strip()
    if not clean_symbol:
        return {}
    try:
        response = requests.get(
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{clean_symbol}",
            params={"modules": ",".join(ANALYSIS_MODULES)},
            timeout=8,
        )
        response.raise_for_status()
        return response.json()
    except Exception:
        return {}


def fetch_quote_snapshot(symbol: str) -> dict[str, Any]:
    clean_symbol = symbol.upper().strip()
    if not clean_symbol:
        return {}

    try:
        response = requests.get(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": clean_symbol},
            timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
        results = payload.get("quoteResponse", {}).get("result", [])
        if results:
            return results[0]
    except Exception:
        return {}

    return {}


def fetch_alpha_overview(symbol: str) -> dict[str, Any]:
    if not ALPHAVANTAGE_API_KEY:
        return {}

    clean_symbol = symbol.upper().strip()
    if not clean_symbol:
        return {}

    try:
        response = requests.get(
            "https://www.alphavantage.co/query",
            params={"function": "OVERVIEW", "symbol": clean_symbol, "apikey": ALPHAVANTAGE_API_KEY},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("Symbol"):
            return payload
    except Exception:
        return {}

    return {}


def fetch_alpha_quote(symbol: str) -> dict[str, Any]:
    if not ALPHAVANTAGE_API_KEY:
        return {}

    clean_symbol = symbol.upper().strip()
    if not clean_symbol:
        return {}

    try:
        response = requests.get(
            "https://www.alphavantage.co/query",
            params={"function": "GLOBAL_QUOTE", "symbol": clean_symbol, "apikey": ALPHAVANTAGE_API_KEY},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        quote = payload.get("Global Quote", {}) if isinstance(payload, dict) else {}
        return quote if isinstance(quote, dict) else {}
    except Exception:
        return {}


def fetch_chart(symbol: str, range_value: str = "1y", interval: str = "1d") -> dict[str, Any]:
    clean_symbol = symbol.upper().strip()
    if not clean_symbol:
        return {}
    try:
        response = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{clean_symbol}",
            params={"range": range_value, "interval": interval, "events": "history", "includePrePost": "false"},
            timeout=8,
        )
        response.raise_for_status()
        return response.json()
    except Exception:
        return {}


def fetch_recent_news(symbol: str) -> list[dict[str, str]]:
    clean_symbol = symbol.upper().strip()
    if not clean_symbol:
        return []
    try:
        response = requests.get(
            "https://query1.finance.yahoo.com/v1/finance/search",
            params={"q": clean_symbol, "quotesCount": 1, "newsCount": 5},
            timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return []

    items = []
    for row in payload.get("news", [])[:5]:
        title = str(row.get("title", "")).strip()
        publisher = str(row.get("publisher", "")).strip()
        link = str(row.get("link", "")).strip()
        if title:
            items.append({"title": title, "publisher": publisher or "Unknown", "link": link})
    return items


def resolve_symbol_input(raw_input: str) -> tuple[str | None, str]:
    candidate = raw_input.upper().strip()
    if not candidate:
        return None, ""

    search_failed = False
    try:
        response = requests.get(
            "https://query1.finance.yahoo.com/v1/finance/search",
            params={"q": raw_input.strip(), "quotesCount": 8, "newsCount": 0},
            timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        payload = {}
        search_failed = True

    quotes = payload.get("quotes", [])
    for quote in quotes:
        symbol = str(quote.get("symbol", "")).upper().strip()
        if symbol == candidate:
            return symbol, ""

    for quote in quotes:
        symbol = str(quote.get("symbol", "")).upper().strip()
        quote_type = str(quote.get("quoteType", "")).upper().strip()
        if symbol and quote_type in {"EQUITY", "ETF"}:
            return symbol, f"Interpreted '{raw_input}' as '{symbol}'."

    if 1 <= len(candidate) <= 4 and candidate.replace(".", "").isalnum():
        return candidate, ""

    return None, ""


def build_stock_analysis(symbol: str) -> dict[str, Any]:
    clean_symbol = symbol.upper().strip()
    if not clean_symbol:
        return {"symbol": "", "error": "Symbol is required"}

    summary_payload = fetch_quote_summary(clean_symbol)
    quote_snapshot = fetch_quote_snapshot(clean_symbol)
    alpha_overview = fetch_alpha_overview(clean_symbol)
    alpha_quote = fetch_alpha_quote(clean_symbol)
    used_quote_summary = bool(summary_payload)
    used_quote_snapshot = bool(quote_snapshot)
    used_alpha = bool(alpha_overview) or bool(alpha_quote)
    summary = nested_get(summary_payload, "quoteSummary", "result", 0) or {}
    chart_payload = fetch_chart(clean_symbol, range_value="1y", interval="1d")
    ts_1y, closes_1y = extract_series_from_chart(chart_payload)

    current_price = as_float(nested_get(summary, "price", "regularMarketPrice"))
    if current_price is None:
        current_price = as_float(quote_snapshot.get("regularMarketPrice"))
    if current_price is None:
        current_price = as_float(alpha_quote.get("05. price"))
    if current_price is None and closes_1y:
        current_price = closes_1y[-1]
    if current_price is None:
        q = fetch_quotes([clean_symbol])
        if clean_symbol in q:
            current_price = float(q[clean_symbol])

    chart_3mo = fetch_chart(clean_symbol, range_value="3mo", interval="1d")
    _, closes_3mo = extract_series_from_chart(chart_3mo)

    chart_5y = fetch_chart(clean_symbol, range_value="5y", interval="1d")
    ts_5y, closes_5y = extract_series_from_chart(chart_5y)
    chart_10y = fetch_chart(clean_symbol, range_value="10y", interval="1d")
    ts_10y, closes_10y = extract_series_from_chart(chart_10y)
    series_ts = ts_10y if ts_10y else (ts_5y if ts_5y else ts_1y)
    series_close = closes_10y if closes_10y else (closes_5y if closes_5y else closes_1y)

    if not series_ts or not series_close:
        stooq_ts, stooq_close = fetch_chart_series_stooq(clean_symbol)
        if stooq_ts and stooq_close:
            series_ts = stooq_ts
            series_close = stooq_close

    chart_source = "Unavailable"
    if series_ts and series_close:
        if ts_10y or ts_5y or ts_1y:
            chart_source = "Yahoo"
        else:
            chart_source = "Stooq"

    price_3m_base = closes_3mo[0] if closes_3mo else previous_price_by_days(series_ts, series_close, 90)
    price_1y_base = closes_1y[0] if closes_1y else previous_price_by_days(series_ts, series_close, 365)

    ytd_start = None
    current_year = datetime.now(timezone.utc).year
    if series_ts and series_close:
        for ts, close in zip(series_ts, series_close):
            point_year = datetime.fromtimestamp(ts, tz=timezone.utc).year
            if point_year == current_year:
                ytd_start = close
                break

    fallback_3m = None
    fallback_ytd = None
    fallback_1y = None
    today = date.today()
    if current_price is not None:
        fallback_3m = as_float(fetch_historical_close(clean_symbol, (today - timedelta(days=90)).isoformat()))
        fallback_ytd = as_float(fetch_historical_close(clean_symbol, date(today.year, 1, 1).isoformat()))
        fallback_1y = as_float(fetch_historical_close(clean_symbol, (today - timedelta(days=365)).isoformat()))

    return_3m = compute_return_pct(current_price, price_3m_base if price_3m_base is not None else fallback_3m)
    return_ytd = compute_return_pct(current_price, ytd_start if ytd_start is not None else fallback_ytd)
    return_1y = compute_return_pct(current_price, price_1y_base if price_1y_base is not None else fallback_1y)

    return_1w = compute_return_pct(current_price, previous_price_by_days(series_ts, series_close, 7))
    return_1m = compute_return_pct(current_price, previous_price_by_days(series_ts, series_close, 30))
    return_6m = compute_return_pct(current_price, previous_price_by_days(series_ts, series_close, 180))
    return_2y = compute_return_pct(current_price, previous_price_by_days(series_ts, series_close, 730))
    return_5y = compute_return_pct(current_price, previous_price_by_days(series_ts, series_close, 1825))
    return_10y = compute_return_pct(current_price, previous_price_by_days(series_ts, series_close, 3650))

    market_cap = as_float(nested_get(summary, "price", "marketCap"))
    if market_cap is None:
        market_cap = as_float(quote_snapshot.get("marketCap"))
    if market_cap is None:
        market_cap = as_float(alpha_overview.get("MarketCapitalization"))

    forward_pe = as_float(nested_get(summary, "financialData", "forwardPE"))
    if forward_pe is None:
        forward_pe = as_float(quote_snapshot.get("forwardPE"))
    if forward_pe is None:
        forward_pe = as_float(alpha_overview.get("ForwardPE"))
    trailing_pe_fallback = as_float(alpha_overview.get("PERatio"))

    revenue_growth_yoy = as_float(nested_get(summary, "financialData", "revenueGrowth"))
    if revenue_growth_yoy is None:
        revenue_growth_yoy = as_float(alpha_overview.get("QuarterlyRevenueGrowthYOY"))

    net_margin = as_float(nested_get(summary, "financialData", "profitMargins"))
    if net_margin is None:
        net_margin = as_float(alpha_overview.get("ProfitMargin"))

    free_cash_flow = as_float(nested_get(summary, "financialData", "freeCashflow"))
    if free_cash_flow is None:
        free_cash_flow = as_float(alpha_overview.get("FreeCashFlowTTM"))

    debt_to_equity = as_float(nested_get(summary, "financialData", "debtToEquity"))
    if debt_to_equity is None:
        debt_to_equity = as_float(alpha_overview.get("DebtToEquity"))

    target_price = as_float(nested_get(summary, "financialData", "targetMeanPrice"))
    if target_price is None:
        target_price = as_float(quote_snapshot.get("targetMeanPrice"))
    if target_price is None:
        target_price = as_float(alpha_overview.get("AnalystTargetPrice"))

    trend_closes = closes_1y if closes_1y else series_close
    ma50 = sma(trend_closes, 50)
    ma200 = sma(trend_closes, 200)
    if ma50 is None:
        ma50 = as_float(quote_snapshot.get("fiftyDayAverage"))
    if ma200 is None:
        ma200 = as_float(quote_snapshot.get("twoHundredDayAverage"))

    earnings_history = nested_get(summary, "earnings", "earningsChart", "quarterly") or []
    eps_rows: list[dict[str, str]] = []
    for row in earnings_history[-4:]:
        actual = as_float(row.get("actual"))
        estimate = as_float(row.get("estimate"))
        beat = "N/A"
        if actual is not None and estimate is not None:
            beat = "Beat" if actual >= estimate else "Miss"
        eps_rows.append(
            {
                "quarter": str(nested_get(row, "date") or ""),
                "actual": format_plain_number(actual),
                "estimate": format_plain_number(estimate),
                "result": beat,
            }
        )

    q_income_rows = nested_get(summary, "incomeStatementHistoryQuarterly", "incomeStatementHistory") or []
    quarterly_rev_profit: list[dict[str, str]] = []
    for row in q_income_rows[:4]:
        quarter_label = str(nested_get(row, "endDate", "fmt") or nested_get(row, "endDate") or "")
        quarterly_rev_profit.append(
            {
                "period": quarter_label,
                "revenue": format_compact_number(as_float(nested_get(row, "totalRevenue"))),
                "profit": format_compact_number(as_float(nested_get(row, "netIncome"))),
            }
        )

    annual_income_rows = nested_get(summary, "incomeStatementHistory", "incomeStatementHistory") or []
    annual_growth_rows: list[dict[str, str]] = []
    for idx, row in enumerate(annual_income_rows[:3]):
        period = str(nested_get(row, "endDate", "fmt") or nested_get(row, "endDate") or "")
        revenue = as_float(nested_get(row, "totalRevenue"))
        profit = as_float(nested_get(row, "netIncome"))
        prev_revenue = as_float(nested_get(annual_income_rows[idx + 1], "totalRevenue")) if idx + 1 < len(annual_income_rows) else None
        prev_profit = as_float(nested_get(annual_income_rows[idx + 1], "netIncome")) if idx + 1 < len(annual_income_rows) else None
        rev_growth = compute_return_pct(revenue, prev_revenue) if revenue is not None else "N/A"
        profit_growth = compute_return_pct(profit, prev_profit) if profit is not None else "N/A"
        annual_growth_rows.append(
            {
                "period": period,
                "revenue": format_compact_number(revenue),
                "profit": format_compact_number(profit),
                "revenue_growth": rev_growth,
                "profit_growth": profit_growth,
            }
        )

    gross_margin = as_float(nested_get(summary, "financialData", "grossMargins"))
    operating_margin = as_float(nested_get(summary, "financialData", "operatingMargins"))
    roe = as_float(nested_get(summary, "financialData", "returnOnEquity"))
    roa = as_float(nested_get(summary, "financialData", "returnOnAssets"))
    roic = as_float(nested_get(summary, "financialData", "returnOnInvestment"))

    cfs_annual_rows = nested_get(summary, "cashflowStatementHistory", "cashflowStatements") or []
    fcf_latest = free_cash_flow
    fcf_prev = None
    if cfs_annual_rows:
        def calc_fcf(row: Any) -> float | None:
            op_cf = as_float(nested_get(row, "totalCashFromOperatingActivities"))
            capex = as_float(nested_get(row, "capitalExpenditures"))
            if op_cf is None or capex is None:
                return None
            return op_cf + capex

        computed_latest = calc_fcf(cfs_annual_rows[0])
        computed_prev = calc_fcf(cfs_annual_rows[1]) if len(cfs_annual_rows) > 1 else None
        if computed_latest is not None:
            fcf_latest = computed_latest
        fcf_prev = computed_prev
    fcf_growth = compute_return_pct(fcf_latest, fcf_prev)

    employees = as_float(nested_get(summary, "assetProfile", "fullTimeEmployees"))
    latest_rev = as_float(nested_get(annual_income_rows, 0, "totalRevenue"))
    latest_profit = as_float(nested_get(annual_income_rows, 0, "netIncome"))
    rev_per_employee = (latest_rev / employees) if latest_rev is not None and employees not in {None, 0} else None
    profit_per_employee = (latest_profit / employees) if latest_profit is not None and employees not in {None, 0} else None

    debt_trend_rows = []
    annual_balance_rows = nested_get(summary, "balanceSheetHistory", "balanceSheetStatements") or []
    for row in annual_balance_rows[:4]:
        debt_trend_rows.append(
            {
                "period": str(nested_get(row, "endDate", "fmt") or nested_get(row, "endDate") or ""),
                "debt": format_compact_number(as_float(nested_get(row, "totalDebt"))),
            }
        )

    total_debt = as_float(nested_get(summary, "financialData", "totalDebt"))
    total_cash = as_float(nested_get(summary, "financialData", "totalCash"))
    cash_vs_debt_ratio = (total_cash / total_debt) if total_cash is not None and total_debt not in {None, 0} else None

    short_interest = as_float(nested_get(summary, "defaultKeyStatistics", "shortPercentOfFloat"))
    insider_ownership = as_float(nested_get(summary, "defaultKeyStatistics", "heldPercentInsiders"))
    institutional_ownership = as_float(nested_get(summary, "defaultKeyStatistics", "heldPercentInstitutions"))

    trailing_pe = as_float(nested_get(summary, "summaryDetail", "trailingPE"))
    if trailing_pe is None:
        trailing_pe = trailing_pe_fallback

    peg_ratio = as_float(nested_get(summary, "defaultKeyStatistics", "pegRatio"))
    if peg_ratio is None:
        peg_ratio = as_float(alpha_overview.get("PEGRatio"))

    price_to_sales = as_float(nested_get(summary, "summaryDetail", "priceToSalesTrailing12Months"))
    if price_to_sales is None:
        price_to_sales = as_float(alpha_overview.get("PriceToSalesRatioTTM"))
    price_to_fcf = (market_cap / fcf_latest) if market_cap is not None and fcf_latest not in {None, 0} else None

    trend_rows = nested_get(summary, "earningsTrend", "trend") or []
    guidance_next_q = "N/A"
    guidance_next_y = "N/A"
    if trend_rows:
        next_q = trend_rows[0] if len(trend_rows) > 0 else None
        next_y = trend_rows[2] if len(trend_rows) > 2 else None
        if isinstance(next_q, dict):
            g = as_float(nested_get(next_q, "growth"))
            guidance_next_q = format_percent_float(g) if g is not None else "N/A"
        if isinstance(next_y, dict):
            g = as_float(nested_get(next_y, "growth"))
            guidance_next_y = format_percent_float(g) if g is not None else "N/A"

    long_summary = str(nested_get(summary, "assetProfile", "longBusinessSummary") or "").strip()
    risk_hint = "Review earnings call and filings for guidance risks."
    if long_summary:
        risk_hint = (long_summary[:300] + "...") if len(long_summary) > 300 else long_summary

    sector = str(nested_get(summary, "assetProfile", "sector") or "N/A")
    industry = str(nested_get(summary, "assetProfile", "industry") or "N/A")
    recent_news = fetch_recent_news(clean_symbol)

    quick = {
        "Price": format_currency(current_price),
        "1W Return": return_1w,
        "1M Return": return_1m,
        "3M Return": return_3m,
        "6M Return": return_6m,
        "YTD Return": return_ytd,
        "1Y Return": return_1y,
        "2Y Return": return_2y,
        "5Y Return": return_5y,
        "10Y Return": return_10y,
        "Market Cap": format_compact_number(market_cap),
        "Forward P/E": format_plain_number(forward_pe),
        "Revenue Growth (YoY)": format_percent_float(revenue_growth_yoy),
        "Net Margin": format_percent_float(net_margin),
        "Free Cash Flow (latest)": format_compact_number(fcf_latest),
        "Debt-to-Equity": format_plain_number(debt_to_equity),
        "1Y Analyst Price Target": format_currency(target_price),
    }

    deep = {
        "trend": {
            "Current Price": format_currency(current_price),
            "MA50": format_currency(ma50),
            "MA200": format_currency(ma200),
            "EPS (last 4 quarters)": eps_rows,
            "Quarterly Revenue / Profit": quarterly_rev_profit,
        },
        "quality": {
            "Annual Revenue & Profit Growth": annual_growth_rows,
            "Gross Margin": format_percent_float(gross_margin),
            "Operating Margin": format_percent_float(operating_margin),
            "Net Margin": format_percent_float(net_margin),
            "ROE": format_percent_float(roe),
            "ROA": format_percent_float(roa),
            "ROIC": format_percent_float(roic),
            "Free Cash Flow": format_compact_number(fcf_latest),
            "FCF YoY Growth": fcf_growth,
            "Revenue per Employee": format_compact_number(rev_per_employee),
            "Profit per Employee": format_compact_number(profit_per_employee),
        },
        "risk": {
            "Debt-to-Equity": format_plain_number(debt_to_equity),
            "Total Debt Trend": debt_trend_rows,
            "Cash on Hand": format_compact_number(total_cash),
            "Cash vs Debt": format_plain_number(cash_vs_debt_ratio),
            "Short Interest %": format_percent_float(short_interest),
            "Insider Ownership %": format_percent_float(insider_ownership),
            "Institutional Ownership %": format_percent_float(institutional_ownership),
        },
        "valuation": {
            "P/E (TTM)": format_plain_number(trailing_pe),
            "P/E (Forward)": format_plain_number(forward_pe),
            "PEG": format_plain_number(peg_ratio),
            "Price-to-Sales": format_plain_number(price_to_sales),
            "Price-to-FCF": format_plain_number(price_to_fcf),
            "Analyst Target (1Y)": format_currency(target_price),
        },
        "context": {
            "Next-Quarter Guidance (growth)": guidance_next_q,
            "Next-Year Guidance (growth)": guidance_next_y,
            "Competitive Position": f"Sector: {sector} | Industry: {industry}",
            "Major Risk Hint": risk_hint,
            "Recent Material News": recent_news,
        },
    }

    return {
        "symbol": clean_symbol,
        "quick": quick,
        "deep": deep,
        "chart": {
            "labels": format_series_labels(series_ts),
            "prices": [round(value, 4) for value in series_close],
        },
        "sources": {
            "chart": chart_source,
            "fundamentals": "Yahoo" if used_quote_summary else ("Yahoo Snapshot" if used_quote_snapshot else ("Alpha Vantage" if used_alpha else "Limited")),
            "fallback": "Alpha Vantage" if used_alpha else "Stooq",
        },
        "error": "" if any(value != "N/A" for value in quick.values()) else "Limited data available for this symbol.",
    }


def add_cash_entry(portfolio_id: int, amount: Decimal, entry_date: str, note: str) -> None:
    with db_connection() as connection:
        connection.execute(
            "INSERT INTO cash_ledger (portfolio_id, amount, entry_date, note) VALUES (?, ?, ?, ?)",
            (portfolio_id, str(amount), entry_date, note.strip()[:250]),
        )


def add_trade(
    portfolio_id: int,
    symbol: str,
    side: str,
    quantity: Decimal,
    price: Decimal,
    trade_date: str,
) -> tuple[bool, str]:
    clean_symbol = symbol.upper().strip()
    if not clean_symbol:
        return False, "Symbol is required"

    with db_connection() as connection:
        if side == "buy":
            total_cost = to_decimal(quantity * price, MONEY_QUANT)
            if get_cash_balance(connection, portfolio_id) < total_cost:
                return False, "Insufficient virtual cash for this buy order"
        elif side == "sell":
            available_qty = get_open_quantity(connection, portfolio_id, clean_symbol)
            if available_qty < quantity:
                return False, f"Not enough shares to sell. Available: {available_qty}"
        else:
            return False, "Invalid trade side"

        connection.execute(
            "INSERT INTO trades (portfolio_id, symbol, side, quantity, price, trade_date) VALUES (?, ?, ?, ?, ?, ?)",
            (portfolio_id, clean_symbol, side, str(quantity), str(price), trade_date),
        )

    return True, "Trade recorded"


def load_cash_ledger(connection: sqlite3.Connection, portfolio_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT id, amount, entry_date, note, created_at
        FROM cash_ledger
        WHERE portfolio_id=?
        ORDER BY entry_date DESC, id DESC
        LIMIT 25
        """,
        (portfolio_id,),
    ).fetchall()


def load_trades(connection: sqlite3.Connection, portfolio_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT id, symbol, side, quantity, price, trade_date, created_at
        FROM trades
        WHERE portfolio_id=?
        ORDER BY trade_date DESC, id DESC
        LIMIT 50
        """,
        (portfolio_id,),
    ).fetchall()


def build_positions(connection: sqlite3.Connection, portfolio_id: int) -> list[dict[str, Any]]:
    trade_rows = connection.execute(
        """
        SELECT symbol, side, quantity, price, trade_date
        FROM trades
        WHERE portfolio_id=?
        ORDER BY trade_date ASC, id ASC
        """,
        (portfolio_id,),
    ).fetchall()

    aggregates: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "buy_qty": Decimal("0"),
            "buy_cost": Decimal("0"),
            "sell_qty": Decimal("0"),
            "sell_value": Decimal("0"),
            "first_buy_date": None,
            "last_trade_price": Decimal("0"),
        }
    )

    for row in trade_rows:
        symbol = row["symbol"].upper()
        side = row["side"]
        quantity = to_decimal(row["quantity"], SHARE_QUANT)
        price = to_decimal(row["price"], MONEY_QUANT)
        trade_value = to_decimal(quantity * price, MONEY_QUANT)
        bucket = aggregates[symbol]

        bucket["last_trade_price"] = price
        if side == "buy":
            bucket["buy_qty"] = to_decimal(bucket["buy_qty"] + quantity, SHARE_QUANT)
            bucket["buy_cost"] = to_decimal(bucket["buy_cost"] + trade_value, MONEY_QUANT)
            if bucket["first_buy_date"] is None:
                bucket["first_buy_date"] = row["trade_date"]
        else:
            bucket["sell_qty"] = to_decimal(bucket["sell_qty"] + quantity, SHARE_QUANT)
            bucket["sell_value"] = to_decimal(bucket["sell_value"] + trade_value, MONEY_QUANT)

    symbols = list(aggregates.keys())
    live_quotes = fetch_quotes(symbols)

    positions: list[dict[str, Any]] = []
    for symbol in sorted(symbols):
        item = aggregates[symbol]
        open_quantity = to_decimal(item["buy_qty"] - item["sell_qty"], SHARE_QUANT)
        if open_quantity <= 0:
            continue

        average_cost = Decimal("0")
        if item["buy_qty"] > 0:
            average_cost = to_decimal(item["buy_cost"] / item["buy_qty"], MONEY_QUANT)

        cost_basis = to_decimal(average_cost * open_quantity, MONEY_QUANT)
        current_price = live_quotes.get(symbol, item["last_trade_price"])
        current_value = to_decimal(current_price * open_quantity, MONEY_QUANT)
        unrealized_gain = to_decimal(current_value - cost_basis, MONEY_QUANT)
        unrealized_pct = Decimal("0")
        if cost_basis > 0:
            unrealized_pct = to_decimal((unrealized_gain / cost_basis) * Decimal("100"), Decimal("0.01"))

        positions.append(
            {
                "symbol": symbol,
                "quantity": open_quantity,
                "average_cost": average_cost,
                "cost_basis": cost_basis,
                "current_price": current_price,
                "current_value": current_value,
                "unrealized_gain": unrealized_gain,
                "unrealized_pct": unrealized_pct,
                "first_buy_date": item["first_buy_date"] or "-",
            }
        )

    return positions


def load_bank_accounts(connection: sqlite3.Connection, tenant_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT id, tenant_id, name, account_type, created_at
        FROM bank_accounts
        WHERE tenant_id=?
        ORDER BY name COLLATE NOCASE ASC, id ASC
        """,
        (tenant_id,),
    ).fetchall()


def get_bank_balance(connection: sqlite3.Connection, account_id: int) -> Decimal:
    """Return the account balance using only entries up to and including today.
    Future-dated entries (salary projections etc.) are excluded from the balance
    but are kept in the ledger for planning purposes."""
    today = date.today().isoformat()
    row = connection.execute(
        "SELECT COALESCE(SUM(CAST(amount AS REAL)), 0) AS total"
        " FROM bank_ledger WHERE account_id=? AND entry_date <= ?",
        (account_id, today),
    ).fetchone()
    return to_decimal(row["total"], MONEY_QUANT)


def load_bank_entries(connection: sqlite3.Connection, account_id: int, limit: int = 80) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT id, amount, entry_date, category, note, created_at
        FROM bank_ledger
        WHERE account_id=?
        ORDER BY entry_date DESC, id DESC
        LIMIT ?
        """,
        (account_id, limit),
    ).fetchall()


def create_bank_account(tenant_id: int, name: str, account_type: str) -> tuple[bool, str, int | None]:
    clean_name = name.strip()
    clean_type = account_type.strip().lower() or "checking"
    if not clean_name:
        return False, "Account name is required", None

    with db_connection() as connection:
        tenant = connection.execute("SELECT id FROM tenants WHERE id=?", (tenant_id,)).fetchone()
        if tenant is None:
            return False, "Owner not found", None

        existing = connection.execute(
            "SELECT id FROM bank_accounts WHERE tenant_id=? AND LOWER(name)=LOWER(?)",
            (tenant_id, clean_name),
        ).fetchone()
        if existing is not None:
            return False, "Account already exists", int(existing["id"])

        connection.execute(
            "INSERT INTO bank_accounts (tenant_id, name, account_type) VALUES (?, ?, ?)",
            (tenant_id, clean_name, clean_type[:40]),
        )
        account_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    return True, "Account created", account_id


def add_bank_entry(account_id: int, amount: Decimal, entry_date: str, category: str, note: str) -> None:
    with db_connection() as connection:
        connection.execute(
            "INSERT INTO bank_ledger (account_id, amount, entry_date, category, note) VALUES (?, ?, ?, ?, ?)",
            (account_id, str(amount), entry_date, category.strip()[:80], note.strip()[:250]),
        )


def add_bank_entries(account_id: int, entries: list[tuple[Decimal, str, str, str]]) -> int:
    if not entries:
        return 0
    with db_connection() as connection:
        connection.executemany(
            "INSERT INTO bank_ledger (account_id, amount, entry_date, category, note) VALUES (?, ?, ?, ?, ?)",
            [
                (account_id, str(amount), entry_date, category.strip()[:80], note.strip()[:250])
                for amount, entry_date, category, note in entries
            ],
        )
    return len(entries)


def monthly_spending_for_month(connection: sqlite3.Connection, tenant_id: int, year: int, month: int) -> Decimal:
    month_start = date(year, month, 1)
    next_month = shift_months(month_start, 1)
    row = connection.execute(
        """
        SELECT COALESCE(SUM(CASE WHEN CAST(bl.amount AS REAL) < 0 THEN -CAST(bl.amount AS REAL) ELSE 0 END), 0) AS spent
        FROM bank_ledger bl
        JOIN bank_accounts ba ON ba.id = bl.account_id
        WHERE ba.tenant_id=? AND bl.entry_date>=? AND bl.entry_date<?
        """,
        (tenant_id, month_start.isoformat(), next_month.isoformat()),
    ).fetchone()
    return to_decimal(row["spent"], MONEY_QUANT)


def build_monthly_spending_series(connection: sqlite3.Connection, tenant_id: int, months: int = 12) -> list[tuple[str, Decimal]]:
    months = max(1, min(months, 36))
    now = date.today().replace(day=1)
    first_month = shift_months(now, -(months - 1))
    result: list[tuple[str, Decimal]] = []
    for offset in range(months):
        period = shift_months(first_month, offset)
        label = period.strftime("%b %Y")
        spent = monthly_spending_for_month(connection, tenant_id, period.year, period.month)
        result.append((label, spent))
    return result


def build_networth_estimate_series(connection: sqlite3.Connection, tenant_id: int, months: int = 12) -> list[tuple[str, Decimal]]:
    months = max(1, min(months, 36))
    now = date.today().replace(day=1)
    first_month = shift_months(now, -(months - 1))

    portfolio_ids = [
        int(row["id"])
        for row in connection.execute("SELECT id FROM portfolios WHERE tenant_id=?", (tenant_id,)).fetchall()
    ]

    series: list[tuple[str, Decimal]] = []
    for offset in range(months):
        period = shift_months(first_month, offset)
        month_end = shift_months(period, 1) - timedelta(days=1)
        month_end_iso = month_end.isoformat()

        bank_row = connection.execute(
            """
            SELECT COALESCE(SUM(CAST(bl.amount AS REAL)), 0) AS total
            FROM bank_ledger bl
            JOIN bank_accounts ba ON ba.id = bl.account_id
            WHERE ba.tenant_id=? AND bl.entry_date<=?
            """,
            (tenant_id, month_end_iso),
        ).fetchone()
        bank_total = to_decimal(bank_row["total"], MONEY_QUANT)

        cash_total = Decimal("0")
        trades_cash_effect = Decimal("0")
        if portfolio_ids:
            placeholders = ",".join("?" for _ in portfolio_ids)
            cash_row = connection.execute(
                f"SELECT COALESCE(SUM(CAST(amount AS REAL)), 0) AS total FROM cash_ledger WHERE portfolio_id IN ({placeholders}) AND entry_date<=?",
                (*portfolio_ids, month_end_iso),
            ).fetchone()
            cash_total = to_decimal(cash_row["total"], MONEY_QUANT)

            trades_row = connection.execute(
                f"""
                SELECT COALESCE(SUM(CASE WHEN side='buy' THEN -CAST(quantity AS REAL) * CAST(price AS REAL)
                                         WHEN side='sell' THEN CAST(quantity AS REAL) * CAST(price AS REAL)
                                         ELSE 0 END), 0) AS total
                FROM trades
                WHERE portfolio_id IN ({placeholders}) AND trade_date<=?
                """,
                (*portfolio_ids, month_end_iso),
            ).fetchone()
            trades_cash_effect = to_decimal(trades_row["total"], MONEY_QUANT)

        estimate = to_decimal(bank_total + cash_total + trades_cash_effect, MONEY_QUANT)
        series.append((period.strftime("%b %Y"), estimate))

    return series


def transfer_bank_to_vpm(
    account_id: int,
    portfolio_id: int,
    amount: Decimal,
    entry_date: str,
    note: str,
) -> tuple[bool, str]:
    with db_connection() as connection:
        account = connection.execute("SELECT id, name FROM bank_accounts WHERE id=?", (account_id,)).fetchone()
        portfolio = connection.execute("SELECT id FROM portfolios WHERE id=?", (portfolio_id,)).fetchone()
        if account is None:
            return False, "Bank account not found"
        if portfolio is None:
            return False, "Portfolio not found"

        balance = get_bank_balance(connection, account_id)
        if balance < amount:
            return False, "Insufficient tracker balance for transfer"

        transfer_note = note.strip()[:200] or "Transfer to VPM"
        connection.execute(
            "INSERT INTO bank_ledger (account_id, amount, entry_date, category, note) VALUES (?, ?, ?, ?, ?)",
            (account_id, str(-amount), entry_date, "transfer", transfer_note),
        )
        connection.execute(
            "INSERT INTO cash_ledger (portfolio_id, amount, entry_date, note) VALUES (?, ?, ?, ?)",
            (portfolio_id, str(amount), entry_date, f"Tracker transfer: {transfer_note}"),
        )

    return True, "Transfer recorded"


def format_money(value: Decimal) -> str:
    sign = "-" if value < 0 else ""
    absolute = abs(value)
    return f"{sign}${absolute:,.2f}"


def format_percent(value: Decimal) -> str:
    return f"{value:.2f}%"


def format_shares(value: Decimal) -> str:
    if value == value.to_integral_value():
        return f"{int(value)}"
    return f"{value:.4f}".rstrip("0").rstrip(".")


def build_query_string(
    tenant_id: int | None,
    portfolio_id: int | None,
    message: str = "",
    extras: dict[str, str] | None = None,
) -> str:
    params: dict[str, str] = {}
    if tenant_id is not None:
        params["tenant_id"] = str(tenant_id)
    if portfolio_id is not None:
        params["portfolio_id"] = str(portfolio_id)
    if message:
        params["msg"] = message
    if extras:
        for key, value in extras.items():
            if value:
                params[key] = value
    return urlencode(params)


def get_app_by_id(app_id: str) -> dict[str, Any] | None:
    for app_item in APP_REGISTRY:
        if str(app_item.get("id", "")).strip().lower() == app_id.strip().lower():
            return app_item
    return None


def is_http_ready(url: str, timeout_seconds: float = 1.5) -> bool:
    try:
        response = requests.get(url, timeout=timeout_seconds)
        return response.status_code < 500
    except Exception:
        return False


def start_process_app(app_item: dict[str, Any]) -> tuple[bool, str]:
    app_id = str(app_item.get("id", "")).strip()
    existing = APP_PROCESSES.get(app_id)
    if existing is not None and existing.poll() is None:
        return True, ""

    launch_command = app_item.get("launch_command")
    if not isinstance(launch_command, list) or not launch_command:
        return False, "App process command is not configured"

    working_dir = str(app_item.get("working_dir") or Path(__file__).parent)
    try:
        process = subprocess.Popen(
            launch_command,
            cwd=working_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        APP_PROCESSES[app_id] = process
    except Exception as error:
        return False, f"Failed to start app process: {error}"

    health_url = str(app_item.get("health_url") or "").strip()
    wait_seconds = float(app_item.get("wait_seconds") or 6.0)
    if health_url:
        deadline = time_module.time() + max(wait_seconds, 0.5)
        while time_module.time() < deadline:
            if is_http_ready(health_url):
                return True, ""
            if process.poll() is not None:
                return False, "App process exited before becoming ready"
            time_module.sleep(0.25)
        return False, "App process did not become ready in time"

    return True, ""


def app_status(app_item: dict[str, Any]) -> tuple[str, bool]:
    app_type = str(app_item.get("type", "")).strip().lower()
    if app_type == "internal":
        return "Built-in", True
    if app_type == "static_html":
        file_path = app_item.get("file_path")
        available = bool(isinstance(file_path, Path) and file_path.exists())
        return ("Add-on" if available else "Add-on Missing"), available
    if app_type == "external_url":
        return "External", True
    if app_type == "process":
        app_id = str(app_item.get("id", "")).strip()
        process = APP_PROCESSES.get(app_id)
        running = bool(process is not None and process.poll() is None)
        return ("Running" if running else "Launch on demand"), True
    return "Unknown", False


def redirect_hub_with_message(message: str) -> RedirectResponse:
    query = urlencode({"msg": message}) if message else ""
    return RedirectResponse(url=(f"{ROOT_PATH}?{query}" if query else ROOT_PATH), status_code=303)


def render_home_page(message: str = "") -> str:
    cards_html = ""
    total_apps = len(APP_REGISTRY)
    available_apps = 0
    missing_apps = 0
    running_apps = 0

    for app_item in APP_REGISTRY:
        app_id = html.escape(str(app_item.get("id", "")))
        name = html.escape(str(app_item.get("name", "App")))
        description = html.escape(str(app_item.get("description", "")))
        status_text, enabled = app_status(app_item)
        if enabled:
            available_apps += 1
        if status_text == "Add-on Missing":
            missing_apps += 1
        if status_text == "Running":
            running_apps += 1
        status_html = html.escape(status_text)
        open_href = OPEN_APP_PATH.replace("{app_id}", app_id)
        if enabled:
            cards_html += f"""
            <a class="card card-link" href="{open_href}">
                <div class="badge">{status_html}</div>
                <h2>{name}</h2>
                <p>{description}</p>
            </a>
            """
        else:
            cards_html += f"""
            <article class="card card-disabled">
                <div class="badge">{status_html}</div>
                <h2>{name}</h2>
                <p>{description}</p>
            </article>
            """

    flash_html = f"<div class='flash'>{html.escape(message)}</div>" if message else ""
    status_badges_html = (
        f"<div class='hero-meta'>"
        f"<div class='hero-badge'>Apps: {total_apps}</div>"
        f"<div class='hero-badge'>Available: {available_apps}</div>"
        f"<div class='hero-badge'>Running: {running_apps}</div>"
        f"<div class='hero-badge'>Missing files: {missing_apps}</div>"
        f"</div>"
    )

    return f"""
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>{APP_HOME_TITLE}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        {shared_theme_css()}
        .page {{ max-width: {COMMON_PAGE_MAX_WIDTH}px; width: 100%; margin: 0 auto; padding: 24px; box-sizing: border-box; overflow: hidden; }}
        .hero {{ background: linear-gradient(135deg, #0f172a, #2952ff); color: white; padding: 28px; border-radius: 22px; box-shadow: var(--shadow); }}
        .hero h1 {{ margin: 0 0 10px 0; font-size: 36px; }}
        .hero p {{ margin: 0; max-width: 900px; color: rgba(255,255,255,0.88); line-height: 1.55; }}
        .hero-meta {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:16px; }}
        .hero-badge {{ background: rgba(255,255,255,0.14); border:1px solid rgba(255,255,255,0.20); border-radius:999px; padding:8px 12px; font-size:13px; }}
        .flash {{ background:#e8f4ff; border:1px solid #b8ddff; padding:12px 14px; border-radius:12px; margin:16px 0 0 0; color:#12456d; }}
        .grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap:18px; margin-top:20px; }}
        .card {{ display:block; background:var(--card); border-radius:18px; box-shadow:var(--shadow); border:1px solid rgba(217,225,238,0.75); padding:20px; min-width:0; overflow:hidden; text-decoration:none; color:inherit; }}
        .card-link:hover {{ transform: translateY(-2px); box-shadow: 0 18px 40px rgba(37, 60, 130, 0.16); }}
        .card-disabled {{ opacity:0.72; }}
        .badge {{ display:inline-block; margin-bottom:10px; font-size:12px; color:#4b5771; border:1px solid var(--line); background:#f6f8fd; border-radius:999px; padding:4px 10px; font-weight:700; }}
        .card h2 {{ margin:0 0 8px 0; font-size:22px; }}
        .card p {{ margin:0; color:var(--muted); line-height:1.45; }}
        .footer {{ margin-top:18px; color:var(--muted); font-size:13px; text-align:center; }}
        @media (max-width: 640px) {{ .page {{ padding:14px; }} .hero h1 {{ font-size:28px; }} }}
    </style>
</head>
<body>
    <div class="page">
        <section class="hero">
            <h1>{APP_HOME_TITLE}</h1>
            <p style="margin: 6px 0 0 0; font-size: 14px; opacity: 0.95;">{APP_AUTHOR}</p>
            <p>Unified launchpad for BAT, VPM, CVP, and related financial tools.</p>
            {status_badges_html}
        </section>
        {flash_html}

        <section class="grid">
            {cards_html}
        </section>
        <footer class="footer">{APP_COPYRIGHT}</footer>
    </div>
</body>
</html>
"""


def render_dashboard(
    message: str = "",
    selected_tenant_id: int | None = None,
    selected_portfolio_id: int | None = None,
    analyze_error: str = "",
    analyze_input: str = "",
    analyze_note: str = "",
    analyze_depth: str = "quick",
) -> str:
    with db_connection() as connection:
        tenants, current_tenant, portfolios, current_portfolio = resolve_selection(
            connection,
            selected_tenant_id,
            selected_portfolio_id,
        )
        current_tenant_id = int(current_tenant["id"])
        current_portfolio_id = int(current_portfolio["id"])
        positions = build_positions(connection, current_portfolio_id)
        trade_rows = load_trades(connection, current_portfolio_id)
        cash_rows = load_cash_ledger(connection, current_portfolio_id)
        cash_added = get_cash_added(connection, current_portfolio_id)
        cash_balance = get_cash_balance(connection, current_portfolio_id)

    market_value = to_decimal(sum(position["current_value"] for position in positions), MONEY_QUANT)
    portfolio_value = to_decimal(cash_balance + market_value, MONEY_QUANT)
    portfolio_gain = to_decimal(portfolio_value - cash_added, MONEY_QUANT)
    portfolio_gain_pct = Decimal("0")
    if cash_added > 0:
        portfolio_gain_pct = to_decimal((portfolio_gain / cash_added) * Decimal("100"), Decimal("0.01"))

    tenant_options_html = "".join(
        f"<option value=\"{int(tenant['id'])}\"{' selected' if int(tenant['id']) == current_tenant_id else ''}>{html.escape(str(tenant['name']))}</option>"
        for tenant in tenants
    )

    portfolio_links_html = "".join(
        f"<a class=\"portfolio-pill{' active' if int(portfolio['id']) == current_portfolio_id else ''}\" href=\"{VPM_PATH}?{build_query_string(current_tenant_id, int(portfolio['id']))}\">{html.escape(str(portfolio['name']))}</a>"
        for portfolio in portfolios
    )

    position_rows_html = ""
    if positions:
        for position in positions:
            gain_class = "gain" if position["unrealized_gain"] >= 0 else "loss"
            position_rows_html += f"""
            <tr>
              <td>{html.escape(position['symbol'])}</td>
              <td>{format_shares(position['quantity'])}</td>
              <td>{format_money(position['average_cost'])}</td>
              <td>{format_money(position['cost_basis'])}</td>
              <td>{format_money(position['current_price'])}</td>
              <td>{format_money(position['current_value'])}</td>
              <td class=\"{gain_class}\">{format_money(position['unrealized_gain'])}</td>
              <td class=\"{gain_class}\">{format_percent(position['unrealized_pct'])}</td>
              <td>{html.escape(str(position['first_buy_date']))}</td>
            </tr>
            """
    else:
        position_rows_html = "<tr><td colspan='9'>No open positions yet for this portfolio.</td></tr>"

    trade_rows_html = ""
    if trade_rows:
        for row in trade_rows:
            row_side = str(row["side"]).upper()
            trade_rows_html += f"""
            <tr>
              <td>{html.escape(row_side)}</td>
              <td>{html.escape(row['symbol'])}</td>
              <td>{format_shares(to_decimal(row['quantity'], SHARE_QUANT))}</td>
              <td>{format_money(to_decimal(row['price'], MONEY_QUANT))}</td>
              <td>{html.escape(row['trade_date'])}</td>
            </tr>
            """
    else:
        trade_rows_html = "<tr><td colspan='5'>No orders yet for this portfolio.</td></tr>"

    cash_rows_html = ""
    if cash_rows:
        for row in cash_rows:
            note = row["note"] or "-"
            cash_rows_html += f"""
            <tr>
              <td>{format_money(to_decimal(row['amount'], MONEY_QUANT))}</td>
              <td>{html.escape(row['entry_date'])}</td>
              <td>{html.escape(note)}</td>
            </tr>
            """
    else:
        cash_rows_html = "<tr><td colspan='3'>No funding entries yet for this portfolio.</td></tr>"

    flash_html = ""
    if message:
        flash_html = f"<div class='flash'>{html.escape(message)}</div>"

    analyze_error_html = f"<div class='muted-note' style='color:#b22121;'>{html.escape(analyze_error)}</div>" if analyze_error else ""
    analyze_note_html = f"<div class='muted-note' style='color:#0b7d23;'>{html.escape(analyze_note)}</div>" if analyze_note else ""
    backup_files = list_backups()
    backup_options_html = "".join(
        f"<option value=\"{html.escape(name)}\">{html.escape(name)}</option>" for name in backup_files
    )
    depth_value = "deep" if analyze_depth == "deep" else "quick"
    iframe_height = "2200" if depth_value == "deep" else "1180"
    iframe_src = "about:blank"
    if analyze_input.strip():
        analyze_query = build_query_string(
            current_tenant_id,
            current_portfolio_id,
            extras={
                "symbol": analyze_input.strip(),
                "depth": depth_value,
            },
        )
        iframe_src = f"/analyze?{analyze_query}"
    analyze_iframe_html = (
        "<div style='margin-top:12px; border:1px solid var(--line); border-radius:12px; overflow:hidden; background:#fff;'>"
        f"<iframe name='analysis-frame' src='{iframe_src}' style='width:100%; height:{iframe_height}px; border:0; display:block;'></iframe>"
        "</div>"
    )

    overall_gain_class = "gain" if portfolio_gain >= 0 else "loss"
    today_value = date.today().isoformat()

    return f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{APP_TITLE}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
        {shared_theme_css()}
    .page {{ max-width: {COMMON_PAGE_MAX_WIDTH}px; width: 100%; margin: 0 auto; padding: 24px; box-sizing: border-box; overflow: hidden; }}
    .hero {{ background: linear-gradient(135deg, #13203c, #2952ff); color: white; padding: 24px; border-radius: 20px; box-shadow: var(--shadow); }}
    .hero h1 {{ margin: 0 0 8px 0; font-size: 34px; }}
    .hero p {{ margin: 0; color: rgba(255,255,255,0.86); line-height: 1.5; }}
    .hero-meta {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 16px; }}
    .hero-badge {{ background: rgba(255,255,255,0.14); border: 1px solid rgba(255,255,255,0.2); border-radius: 999px; padding: 8px 12px; font-size: 13px; }}
    .flash {{ background: #e8f4ff; border: 1px solid #b8ddff; padding: 12px 14px; border-radius: 12px; margin: 16px 0 0 0; color: #12456d; }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 14px; margin: 18px 0; }}
    .metric {{ background: var(--card); padding: 16px; border-radius: 16px; box-shadow: var(--shadow); border: 1px solid rgba(217, 225, 238, 0.75); }}
    .label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }}
    .value {{ font-size: 26px; font-weight: 800; margin-top: 6px; }}
    .layout {{ display: grid; grid-template-columns: 360px minmax(0, 1fr); gap: 18px; align-items: start; min-width: 0; width: 100%; }}
    .layout > * {{ min-width: 0; overflow: hidden; }}
    .stack {{ display: grid; gap: 18px; min-width: 0; }}
    .main-stack {{ display: grid; gap: 18px; min-width: 0; }}
    .card {{ background: var(--card); border-radius: 18px; box-shadow: var(--shadow); border: 1px solid rgba(217, 225, 238, 0.75); padding: 18px; min-width: 0; overflow: hidden; }}
    .card h2 {{ margin: 0 0 8px 0; font-size: 21px; }}
    .card p {{ color: var(--muted); margin: 0 0 14px 0; line-height: 1.45; }}
    .section-title {{ display: flex; justify-content: space-between; gap: 12px; align-items: baseline; margin-bottom: 10px; }}
    form {{ display: grid; gap: 9px; }}
    label {{ font-weight: 700; font-size: 13px; color: var(--ink); }}
    input, select, button {{ width: 100%; border-radius: 10px; border: 1px solid var(--line); padding: 11px 12px; font-size: 14px; }}
    input, select {{ background: white; color: var(--ink); }}
    button {{ background: var(--brand); color: white; font-weight: 800; cursor: pointer; border: 0; }}
    button.secondary {{ background: white; color: var(--ink); border: 1px solid var(--line); }}
    .button-row {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .button-row button {{ flex: 1 1 160px; }}
    .portfolio-nav {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; }}
    .portfolio-pill {{ text-decoration: none; color: var(--ink); background: #f5f7fc; border: 1px solid var(--line); border-radius: 999px; padding: 8px 12px; font-weight: 700; font-size: 13px; }}
    .portfolio-pill.active {{ background: var(--brand); color: white; border-color: var(--brand); }}
    .muted-note {{ color: var(--muted); font-size: 13px; line-height: 1.45; }}
    .table-wrap {{ overflow-x: auto; width: 100%; max-width: 100%; }}
    iframe {{ max-width: 100%; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ border-bottom: 1px solid #eef2f8; padding: 10px 8px; text-align: left; font-size: 14px; white-space: nowrap; }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; background: #fbfcff; }}
    .gain {{ color: var(--gain); font-weight: 800; }}
    .loss {{ color: var(--loss); font-weight: 800; }}
    @media (max-width: 1080px) {{
      .layout {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 640px) {{
      .page {{ padding: 14px; }}
      .hero h1 {{ font-size: 28px; }}
      .value {{ font-size: 22px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>{APP_TITLE}</h1>
            <p style="margin: 6px 0 0 0; font-size: 14px; opacity: 0.95;">{APP_AUTHOR}</p>
      <p>
        A multi-tenant paper-trading system for households, families, or teams. Each tenant can own multiple
        portfolios, receive recurring funding such as salary top-ups, and place simulated buy or sell orders with
        live market pricing.
      </p>
      <div class="hero-meta">
        <div class="hero-badge">Owner: {html.escape(str(current_tenant['name']))}</div>
        <div class="hero-badge">Portfolio: {html.escape(str(current_portfolio['name']))}</div>
        <div class="hero-badge">Funding entries simulate salary, bonuses, and deposits</div>
                <div class="hero-badge"><a href="{ROOT_PATH}" style="color:#fff; text-decoration:none;">Back to Home</a></div>
      </div>
    </section>
    {flash_html}

    <section class="metrics">
      <div class="metric"><div class="label">Total Funding Added</div><div class="value">{format_money(cash_added)}</div></div>
      <div class="metric"><div class="label">Cash Balance</div><div class="value">{format_money(cash_balance)}</div></div>
      <div class="metric"><div class="label">Open Positions Value</div><div class="value">{format_money(market_value)}</div></div>
      <div class="metric"><div class="label">Portfolio Value</div><div class="value">{format_money(portfolio_value)}</div></div>
      <div class="metric"><div class="label">Gain / Loss</div><div class="value {overall_gain_class}">{format_money(portfolio_gain)} ({format_percent(portfolio_gain_pct)})</div></div>
    </section>

    <div class="layout">
      <div class="stack">
        <section class="card">
          <div class="section-title">
            <h2>Workspace</h2>
          </div>
                    <p>Choose who you are managing, then switch between that owner's portfolios.</p>
          <form method="get" action="{VPM_PATH}">
                        <label>Owner</label>
            <select name="tenant_id">{tenant_options_html}</select>
                        <button type="submit">Open Owner</button>
          </form>
          <hr style="border:0; border-top:1px solid var(--line); margin:16px 0;">
          <form method="post" action="/tenant/add">
                        <label>New Owner Name</label>
            <input name="tenant_name" type="text" maxlength="120" placeholder="e.g. Moshiko, Family Office" required>
                        <button type="submit" class="secondary">Add Owner</button>
          </form>
                    <form method="post" action="/owner/delete" style="margin-top:8px;">
                        <input type="hidden" name="tenant_id" value="{current_tenant_id}">
                        <input type="hidden" name="portfolio_id" value="{current_portfolio_id}">
                        <button type="submit" class="secondary" onclick="return confirm('Delete owner and all their portfolios, trades, and funding history?')">Delete Current Owner</button>
                    </form>
        </section>

        <section class="card">
          <div class="section-title">
            <h2>Portfolios</h2>
          </div>
          <p>{html.escape(str(current_tenant['name']))} can have multiple strategies or goals separated cleanly.</p>
          <div class="portfolio-nav">{portfolio_links_html}</div>
          <form method="post" action="{VPM_PATH}/add">
            <input type="hidden" name="tenant_id" value="{current_tenant_id}">
            <label>New Portfolio Name</label>
            <input name="portfolio_name" type="text" maxlength="120" placeholder="e.g. Long-Term, Dividends, Experimental" required>
            <button type="submit" class="secondary">Create Portfolio</button>
          </form>
        </section>

        <section class="card">
          <div class="section-title">
            <h2>Add Funding</h2>
          </div>
          <p>Use this for initial capital, salary contributions, monthly deposits, or one-time top-ups.</p>
          <form method="post" action="/cash/add">
            <input type="hidden" name="tenant_id" value="{current_tenant_id}">
            <input type="hidden" name="portfolio_id" value="{current_portfolio_id}">
            <label>Amount</label>
            <input name="amount" type="number" step="0.01" min="0.01" placeholder="10000" required>
            <label>Date</label>
            <input name="entry_date" type="date" value="{today_value}" required>
            <label>Note</label>
            <input name="note" type="text" maxlength="250" placeholder="Initial deposit, salary top-up, bonus">
            <button type="submit">Add Funding</button>
          </form>
        </section>

                <section class="card">
                    <div class="section-title">
                        <h2>Data Tools</h2>
                    </div>
                    <p>Save a snapshot, restore a previous one, reset to defaults, or zeroize the selected portfolio.</p>
                    <form method="post" action="/snapshot/save">
                        <input type="hidden" name="tenant_id" value="{current_tenant_id}">
                        <input type="hidden" name="portfolio_id" value="{current_portfolio_id}">
                        <button type="submit" class="secondary">Save Snapshot</button>
                    </form>
                    <form method="post" action="/snapshot/load" style="margin-top:8px;">
                        <input type="hidden" name="tenant_id" value="{current_tenant_id}">
                        <input type="hidden" name="portfolio_id" value="{current_portfolio_id}">
                        <label>Load Snapshot</label>
                        <select name="snapshot_name" {'required' if backup_files else 'disabled'}>
                            <option value="">{html.escape('Choose a snapshot' if backup_files else 'No snapshots saved yet')}</option>
                            {backup_options_html}
                        </select>
                        <button type="submit" class="secondary" {'disabled' if not backup_files else ''}>Load Snapshot</button>
                    </form>
                    <form method="post" action="/defaults/restore" style="margin-top:8px;">
                        <input type="hidden" name="tenant_id" value="{current_tenant_id}">
                        <input type="hidden" name="portfolio_id" value="{current_portfolio_id}">
                        <button type="submit" class="secondary" onclick="return confirm('Restore app defaults? This wipes current app data but creates a safety snapshot first.')">Restore Defaults</button>
                    </form>
                    <form method="post" action="{VPM_PATH}/zeroize" style="margin-top:8px;">
                        <input type="hidden" name="tenant_id" value="{current_tenant_id}">
                        <input type="hidden" name="portfolio_id" value="{current_portfolio_id}">
                        <button type="submit" class="secondary" onclick="return confirm('Zeroize the current portfolio? This removes its trades and funding, but creates a safety snapshot first.')">Zeroize Current Portfolio</button>
                    </form>
                    <div class="muted-note" style="margin-top:8px;">Safety snapshots are saved in the local <strong>VPM/portfolio_backups</strong> folder.</div>
                </section>
      </div>

      <div class="main-stack">
                <section class="card">
                    <div class="section-title">
                        <h2>Open Positions</h2>
                        <div class="muted-note">{html.escape(str(current_tenant['name']))} / {html.escape(str(current_portfolio['name']))}</div>
                    </div>
                    <div class="table-wrap">
                        <table>
                            <thead>
                                <tr>
                                    <th>Symbol</th>
                                    <th>Qty</th>
                                    <th>Avg Cost</th>
                                    <th>Cost Basis</th>
                                    <th>Current Price</th>
                                    <th>Current Value</th>
                                    <th>Gain / Loss</th>
                                    <th>%</th>
                                    <th>First Buy Date</th>
                                </tr>
                            </thead>
                            <tbody>{position_rows_html}</tbody>
                        </table>
                    </div>
                </section>

                <section class="card">
                    <div class="section-title">
                        <h2>Analyze Before Buying</h2>
                    </div>
                    <p>Check the stock first, then trade.</p>
                    <form method="get" action="/analyze" target="analysis-frame">
                        <input type="hidden" name="tenant_id" value="{current_tenant_id}">
                        <input type="hidden" name="portfolio_id" value="{current_portfolio_id}">
                        <label>Symbol</label>
                        <input name="symbol" type="text" placeholder="Ticker (e.g. AAPL)" value="{html.escape(analyze_input)}" required>
                        {analyze_error_html}
                        {analyze_note_html}
                        <div class="button-row">
                            <button type="submit" name="depth" value="quick">Quick View</button>
                            <button type="submit" name="depth" value="deep" class="secondary">One More Click: Deeper View</button>
                        </div>
                    </form>
                    {analyze_iframe_html}
                </section>

        <section class="card">
          <div class="section-title">
            <h2>Place Order</h2>
          </div>
          <p>Submit simulated buy or sell orders inside the currently selected portfolio.</p>
          <form method="post" action="/trade/add">
            <input type="hidden" name="tenant_id" value="{current_tenant_id}">
            <input type="hidden" name="portfolio_id" value="{current_portfolio_id}">
            <label>Side</label>
            <select id="trade-side" name="side" required>
              <option value="buy">BUY</option>
              <option value="sell">SELL</option>
            </select>
            <label>Symbol</label>
                        <input id="trade-symbol" name="symbol" type="text" placeholder="Ticker (e.g. AAPL)" required>
            <label>Quantity</label>
            <input name="quantity" type="number" min="0.0001" step="0.0001" placeholder="10" required>
            <label>Price (per share / unit)</label>
            <input id="trade-price" name="price" type="number" min="0.01" step="0.01" placeholder="150.25" required>
            <div class="button-row">
              <button id="refresh-current-price" type="button" class="secondary">Refresh Current Price</button>
              <button id="load-historical-price" type="button" class="secondary">Use Trade-Date Price</button>
            </div>
            <div id="quote-status" class="muted-note">Tip: current price is best for today's order. Historical close helps for backdated entries.</div>
            <label>Trade Date</label>
            <input id="trade-date" name="trade_date" type="date" value="{today_value}" required>
            <button id="submit-order" type="submit">Buy</button>
          </form>
        </section>

        <section class="card">
          <div class="section-title">
                        <h2>Order History</h2>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                                <tr><th>Side</th><th>Symbol</th><th>Qty</th><th>Price</th><th>Date</th></tr>
              </thead>
                            <tbody>{trade_rows_html}</tbody>
            </table>
          </div>
        </section>

        <section class="card">
          <div class="section-title">
                        <h2>Funding Ledger</h2>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                                <tr><th>Amount</th><th>Date</th><th>Note</th></tr>
              </thead>
                            <tbody>{cash_rows_html}</tbody>
            </table>
          </div>
        </section>
      </div>
    </div>

    <script>
      const sideField = document.getElementById('trade-side');
      const symbolField = document.getElementById('trade-symbol');
      const dateField = document.getElementById('trade-date');
      const priceField = document.getElementById('trade-price');
      const submitButton = document.getElementById('submit-order');
      const statusField = document.getElementById('quote-status');
      const currentButton = document.getElementById('refresh-current-price');
      const historicalButton = document.getElementById('load-historical-price');
            const analysisFrame = document.querySelector("iframe[name='analysis-frame']");

            window.addEventListener('message', (event) => {{
                const data = event.data || {{}};
                if (!analysisFrame || data.type !== 'analysis-height') return;
                const nextHeight = Number(data.height);
                if (!Number.isFinite(nextHeight) || nextHeight < 600) return;
                analysisFrame.style.height = `${{Math.min(Math.max(nextHeight + 24, 800), 3200)}}px`;
            }});

      function updateSubmitButton() {{
        const side = (sideField.value || 'buy').toLowerCase();
        submitButton.textContent = side === 'sell' ? 'Sell' : 'Buy';
      }}

      function setStatus(text, isError = false) {{
        statusField.textContent = text;
        statusField.style.color = isError ? '#b22121' : '#5f6b85';
      }}

      async function loadPrice(mode) {{
        const symbol = (symbolField.value || '').trim().toUpperCase();
        if (!symbol) {{
          setStatus('Enter a symbol first.', true);
          return;
        }}

        let url = `/api/quote/current?symbol=${{encodeURIComponent(symbol)}}`;
        if (mode === 'historical') {{
          const tradeDate = dateField.value;
          if (!tradeDate) {{
            setStatus('Choose a trade date first.', true);
            return;
          }}
          url = `/api/quote/historical?symbol=${{encodeURIComponent(symbol)}}&date=${{encodeURIComponent(tradeDate)}}`;
        }}

        setStatus(mode === 'historical' ? 'Fetching trade-date price...' : 'Fetching current price...');
        try {{
          const response = await fetch(url);
          const payload = await response.json();
          if (!response.ok || !payload.ok) {{
            setStatus(payload.error || 'Price lookup failed.', true);
            return;
          }}

          priceField.value = Number(payload.price).toFixed(2);
          if (mode === 'historical') {{
            setStatus(`Loaded historical close for ${{payload.symbol}} on ${{payload.date}}: $${{Number(payload.price).toFixed(2)}}`);
          }} else {{
            setStatus(`Loaded current price for ${{payload.symbol}}: $${{Number(payload.price).toFixed(2)}}`);
          }}
        }} catch (error) {{
          setStatus('Unable to fetch price right now.', true);
        }}
      }}

      sideField.addEventListener('change', updateSubmitButton);
      currentButton.addEventListener('click', () => loadPrice('current'));
      historicalButton.addEventListener('click', () => loadPrice('historical'));
      updateSubmitButton();
    </script>
        <footer class="footer">{APP_COPYRIGHT}</footer>
  </div>
</body>
</html>
"""


async def parse_form(request) -> dict[str, str]:
    body = await request.body()
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    return {key: values[0] if values else "" for key, values in parsed.items()}


def redirect_with_message(message: str, tenant_id: int | None = None, portfolio_id: int | None = None) -> RedirectResponse:
    query_string = build_query_string(tenant_id, portfolio_id, message)
    url = VPM_PATH
    if query_string:
        url = f"{VPM_PATH}?{query_string}"
    return RedirectResponse(url=url, status_code=303)


def redirect_dashboard(
    tenant_id: int | None = None,
    portfolio_id: int | None = None,
    message: str = "",
    extras: dict[str, str] | None = None,
) -> RedirectResponse:
    query_string = build_query_string(tenant_id, portfolio_id, message, extras=extras)
    url = VPM_PATH
    if query_string:
        url = f"{VPM_PATH}?{query_string}"
    return RedirectResponse(url=url, status_code=303)


def render_analysis_page(
    analysis: dict[str, Any],
    depth: str,
    tenant_id: int | None,
    portfolio_id: int | None,
) -> str:
    symbol = analysis.get("symbol", "")
    quick = analysis.get("quick", {})
    deep = analysis.get("deep", {})
    chart = analysis.get("chart", {})
    sources = analysis.get("sources", {})
    error = analysis.get("error", "")
    chart_labels = chart.get("labels", []) if isinstance(chart, dict) else []
    chart_prices = chart.get("prices", []) if isinstance(chart, dict) else []
    chart_json = json.dumps({"labels": chart_labels, "prices": chart_prices})
    source_chart = html.escape(str(sources.get("chart", "Unknown")))
    source_fund = html.escape(str(sources.get("fundamentals", "Unknown")))
    source_fb = html.escape(str(sources.get("fallback", "Unknown")))

    quick_available = [(key, value) for key, value in quick.items() if str(value) != "N/A"]
    quick_missing = [key for key, value in quick.items() if str(value) == "N/A"]

    quick_rows = "".join(
        f"<tr><th class='key-cell'>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in quick_available
    )
    if not quick_rows:
        quick_rows = "<tr><td colspan='2'>No quick metrics available right now.</td></tr>"

    missing_html = ""
    if quick_missing:
        missing_items = "".join(
            f"<code style='display:inline-block; margin:4px 6px 0 0; padding:4px 8px; border-radius:8px; border:1px solid #d9e1ee; background:#f7f9fd; color:#35415e; font-size:12px;'>{html.escape(item)}</code>"
            for item in quick_missing
        )
        missing_html = (
            "<div style='margin-top:10px;'>"
            "<div style='font-size:12px; color:#5f6b85; margin-bottom:6px;'>Unavailable right now:</div>"
            "<div style='font-size:12px; color:#5f6b85; margin-bottom:6px;'>This is usually a temporary upstream data-limit issue.</div>"
            f"<div>{missing_items}</div>"
            "</div>"
        )

    def render_rows_dict(section: dict[str, Any]) -> str:
        rows = ""
        for key, value in section.items():
            if isinstance(value, list):
                if not value:
                    rows += f"<tr><th class='key-cell'>{html.escape(str(key))}</th><td>N/A</td></tr>"
                    continue
                if value and isinstance(value[0], dict):
                    lines = []
                    for item in value:
                        line = " | ".join(f"{k}: {v}" for k, v in item.items())
                        lines.append(html.escape(line))
                    lines_html = "".join(f"<div>{line}</div>" for line in lines)
                    rows += f"<tr><th class='key-cell'>{html.escape(str(key))}</th><td><div style='display:grid; gap:4px;'>{lines_html}</div></td></tr>"
                else:
                    rows += f"<tr><th class='key-cell'>{html.escape(str(key))}</th><td>{html.escape(', '.join(str(v) for v in value))}</td></tr>"
            elif isinstance(value, dict):
                rows += f"<tr><th class='key-cell'>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
            else:
                rows += f"<tr><th class='key-cell'>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
        return rows

    trend_rows = render_rows_dict(deep.get("trend", {}))
    quality_rows = render_rows_dict(deep.get("quality", {}))
    risk_rows = render_rows_dict(deep.get("risk", {}))
    valuation_rows = render_rows_dict(deep.get("valuation", {}))

    context_block = deep.get("context", {})
    context_rows = ""
    for key, value in context_block.items():
        if key == "Recent Material News" and isinstance(value, list):
            if value:
                news_html = ""
                for item in value:
                    title = html.escape(str(item.get("title", "")))
                    publisher = html.escape(str(item.get("publisher", "")))
                    link = html.escape(str(item.get("link", "")))
                    if link:
                        news_html += f"<li><a href='{link}' target='_blank' rel='noreferrer'>{title}</a> <span style='color:#5f6b85;'>({publisher})</span></li>"
                    else:
                        news_html += f"<li>{title} <span style='color:#5f6b85;'>({publisher})</span></li>"
                context_rows += f"<tr><th class='key-cell'>{html.escape(key)}</th><td><ul style='margin:0; padding-left:18px;'>{news_html}</ul></td></tr>"
            else:
                context_rows += f"<tr><th class='key-cell'>{html.escape(key)}</th><td>N/A</td></tr>"
        else:
            context_rows += f"<tr><th class='key-cell'>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"

    query_base = build_query_string(tenant_id, portfolio_id)
    deeper_query = f"{query_base}&" if query_base else ""
    deeper_link = f"/analyze?{deeper_query}symbol={symbol}&depth=deep"
    back_link = f"{VPM_PATH}?{query_base}" if query_base else VPM_PATH
    show_deep = depth == "deep"
    error_html = f"<div class='flash'>{html.escape(error)}</div>" if error else ""

    deep_html = ""
    if show_deep:
        deep_html = f"""
        <section class='card'>
            <h2 style='margin:0 0 10px 0;'>📈 Performance & Trends</h2>
            <table><tbody>{trend_rows}</tbody></table>
        </section>

        <section class='card'>
            <h2 style='margin:0 0 10px 0;'>🏢 Business Quality</h2>
            <table><tbody>{quality_rows}</tbody></table>
        </section>

        <section class='card'>
            <h2 style='margin:0 0 10px 0;'>💰 Balance Sheet & Risk</h2>
            <table><tbody>{risk_rows}</tbody></table>
        </section>

        <section class='card'>
            <h2 style='margin:0 0 10px 0;'>📊 Valuation</h2>
            <table><tbody>{valuation_rows}</tbody></table>
        </section>

        <section class='card'>
            <h2 style='margin:0 0 10px 0;'>🧠 Context</h2>
            <table><tbody>{context_rows}</tbody></table>
        </section>
        """

    return f"""
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>{APP_TITLE} - Analysis</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        {shared_theme_css()}
        .wrap {{ max-width: {COMMON_PAGE_MAX_WIDTH}px; margin: 0 auto; padding: 24px; }}
        .top {{ background: linear-gradient(135deg, #13203c, #2952ff); color:white; border-radius:16px; padding:20px; }}
        .top h1 {{ margin:0 0 6px 0; }}
        .top p {{ margin:0; opacity:0.9; }}
        .actions {{ margin-top:14px; display:flex; gap:10px; flex-wrap:wrap; }}
        .btn {{ text-decoration:none; color:white; background:#2952ff; border-radius:10px; padding:10px 14px; font-weight:700; }}
        .btn.secondary {{ background:white; color:#13203c; border:1px solid #d9e1ee; }}
        .card {{ margin-top:14px; background:white; border-radius:14px; padding:16px; border:1px solid #d9e1ee; box-shadow:0 8px 20px rgba(20,42,90,0.08); }}
        .flash {{ background: #fff8e8; border:1px solid #ffe2a9; color:#684b00; padding:10px; border-radius:10px; margin-top:12px; }}
        table {{ width:100%; border-collapse: collapse; table-layout: fixed; }}
        td, th {{ border-bottom:1px solid #eef2f8; text-align:left; padding:9px 8px; vertical-align:top; }}
        .key-cell {{ width: 280px; color:#4b5771; font-size:13px; font-weight:700; text-transform:none; letter-spacing:0; }}
        .chart-wrap {{ margin-top:10px; background:#fbfcff; border:1px solid #d9e1ee; border-radius:12px; padding:10px; }}
        .chart-actions {{ display:flex; flex-wrap:wrap; gap:8px; margin:0 0 8px 0; }}
        .chart-actions button {{ border:1px solid #d9e1ee; background:white; color:#11203b; border-radius:8px; padding:6px 10px; cursor:pointer; font-size:12px; font-weight:700; }}
        .chart-actions button.active {{ background:#2952ff; color:white; border-color:#2952ff; }}
        #quick-chart {{ width:100%; height:240px; border-radius:8px; display:block; }}
        .chart-meta {{ margin-top:10px; display:flex; flex-wrap:wrap; gap:8px; }}
        .chart-pill {{ font-size:12px; border:1px solid #d9e1ee; background:white; color:#233256; border-radius:999px; padding:5px 10px; }}
        .chart-muted {{ font-size:12px; color:#5f6b85; margin-top:8px; }}
    </style>
</head>
<body>
    <div class="wrap">
        <section class="top">
            <h1>{APP_TITLE}: Stock Analysis ({html.escape(str(symbol))})</h1>
            <p style="margin: 6px 0 0 0; font-size: 14px; opacity: 0.95;">{APP_AUTHOR}</p>
            <div class="actions">
                <a class="btn secondary" href="{back_link}">Back to Portfolio</a>
                <a class="btn" href="{deeper_link}">One More Click: Deeper View</a>
            </div>
        </section>
        {error_html}

        <section class="card">
            <h2 style="margin:0 0 10px 0;">⚡ Quick View (decision in 10 sec)</h2>
            <table>
                <tbody>{quick_rows}</tbody>
            </table>
            {missing_html}
            <div class="main-stack">
                <section class="card">
                    <h2>Ledger: {html.escape(str(current_account['name']))}</h2>
                    <p>All income and expense entries for the selected account. Future salary projections appear here but are excluded from the current balance.</p>
                    <div class="table-wrap">
                        <table>
                            <thead>
                                <tr>
                                    <th>Amount</th>
                                    <th>Date</th>
                                    <th>Category</th>
                                    <th>Note</th>
                                </tr>
                            </thead>
                            <tbody>
                                {entry_rows_html}
                            </tbody>
                        </table>
                    </div>
                </section>

                <section class="card">
                    <h2>Deposit Cash</h2>
                    <p>Add funds any time, including paycheck, bonus, or one-off deposit.</p>
                    <form method="post" action="{TRACKER_PATH}/deposit">
                        <input type="hidden" name="tenant_id" value="{current_tenant_id}">
                        <input type="hidden" name="account_id" value="{current_account_id}">
                        <label>Amount</label>
                        <input name="amount" type="number" min="0.01" step="0.01" placeholder="1000.00" required>
                        <label>Date</label>
                        <input name="entry_date" type="date" value="{today_value}" required>
                        <label>Note</label>
                        <input name="note" type="text" maxlength="250" placeholder="Paycheck / bonus / cash deposit">
                        <button type="submit">Deposit</button>
                    </form>
                </section>
            </div>
    <script>
        const chartPayload = {chart_json};
        const chartCanvas = document.getElementById('quick-chart');
            if (label === '10Y') return 3650;
            return null;
        }}

        function pickPoints(rangeLabel) {{
            const labels = chartPayload.labels || [];
            const prices = chartPayload.prices || [];
            if (!labels.length || !prices.length || labels.length !== prices.length) return {{ labels: [], prices: [] }};

            const days = rangeDays(rangeLabel);
            if (!days) return {{ labels, prices }};

            const end = new Date(labels[labels.length - 1]);
            const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000);
            let idx = 0;
            while (idx < labels.length && new Date(labels[idx]) < start) idx += 1;
            return {{ labels: labels.slice(idx), prices: prices.slice(idx) }};
        }}

        function formatMoney(value) {{
            return `$${{value.toLocaleString(undefined, {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }})}}`;
        }}

        function updateSummary(labels, prices, rangeLabel) {{
            if (!chartSummary || !chartStatus) return;
            if (!prices.length) {{
                chartSummary.innerHTML = "";
                chartStatus.textContent = 'No chart data available for this symbol at the moment.';
                return;
            }}

            const start = prices[0];
            const end = prices[prices.length - 1];
            const min = Math.min(...prices);
            const max = Math.max(...prices);
            const diff = end - start;
            const pct = (diff / Math.max(Math.abs(start), 0.0001)) * 100;
            const color = pct >= 0 ? '#0b7d23' : '#b22121';

            chartSummary.innerHTML = `
                <span class="chart-pill">Range: ${{rangeLabel}}</span>
                <span class="chart-pill">Start: ${{formatMoney(start)}}</span>
                <span class="chart-pill">End: ${{formatMoney(end)}}</span>
                <span class="chart-pill">High: ${{formatMoney(max)}}</span>
                <span class="chart-pill">Low: ${{formatMoney(min)}}</span>
                <span class="chart-pill" style="border-color:${{color}}; color:${{color}};">Change: ${{pct.toFixed(2)}}% (${{diff >= 0 ? '+' : ''}}${{diff.toFixed(2)}})</span>
            `;
            chartStatus.textContent = `From ${{labels[0]}} to ${{labels[labels.length - 1]}} · ${{prices.length}} points`;
        }}

        function resizeCanvas() {{
            if (!chartCanvas) return;
            const dpr = window.devicePixelRatio || 1;
            const width = Math.max(320, Math.floor(chartCanvas.clientWidth));
            const height = 240;
            chartCanvas.width = Math.floor(width * dpr);
            chartCanvas.height = Math.floor(height * dpr);
            const ctx = chartCanvas.getContext('2d');
            ctx.setTransform(1, 0, 0, 1, 0, 0);
            ctx.scale(dpr, dpr);
        }}

        function drawLine(rangeLabel = '1Y') {{
            if (!chartCanvas) return;
            resizeCanvas();
            const ctx = chartCanvas.getContext('2d');
            const selected = pickPoints(rangeLabel);
            const labels = selected.labels;
            const data = selected.prices;
            activeLabels = labels;
            activePrices = data;

            const width = chartCanvas.clientWidth;
            const height = chartCanvas.clientHeight;
            ctx.clearRect(0, 0, width, height);
            if (!data.length) {{
                ctx.fillStyle = '#5f6b85';
                ctx.font = '13px Arial';
                ctx.fillText('No chart data available.', 14, 24);
                updateSummary(labels, data, rangeLabel);
                drawModel = null;
                return;
            }}

            const padLeft = 48;
            const padRight = 16;
            const padTop = 18;
            const padBottom = 24;
            const w = width - padLeft - padRight;
            const h = height - padTop - padBottom;
            const min = Math.min(...data);
            const max = Math.max(...data);
            const span = Math.max(max - min, 0.0001);

            ctx.strokeStyle = '#e8edf6';
            ctx.lineWidth = 1;
            for (let i = 0; i <= 4; i += 1) {{
                const y = padTop + (i / 4) * h;
                ctx.beginPath();
                ctx.moveTo(padLeft, y);
                ctx.lineTo(padLeft + w, y);
                ctx.stroke();
            }}

            const points = data.map((value, index) => {{
                const x = padLeft + (index / Math.max(data.length - 1, 1)) * w;
                const y = padTop + (1 - ((value - min) / span)) * h;
                return {{ x, y, value, label: labels[index] }};
            }});

            const gradient = ctx.createLinearGradient(0, padTop, 0, padTop + h);
            gradient.addColorStop(0, 'rgba(41,82,255,0.30)');
            gradient.addColorStop(1, 'rgba(41,82,255,0.03)');
            ctx.beginPath();
            points.forEach((point, index) => {{
                if (index === 0) ctx.moveTo(point.x, point.y);
                else ctx.lineTo(point.x, point.y);
            }});
            ctx.lineTo(points[points.length - 1].x, padTop + h);
            ctx.lineTo(points[0].x, padTop + h);
            ctx.closePath();
            ctx.fillStyle = gradient;
            ctx.fill();

            ctx.strokeStyle = '#2952ff';
            ctx.lineWidth = 2.2;
            ctx.beginPath();
            points.forEach((point, index) => {{
                if (index === 0) ctx.moveTo(point.x, point.y);
                else ctx.lineTo(point.x, point.y);
            }});
            ctx.stroke();

            const first = data[0];
            const last = data[data.length - 1];
            const pct = ((last - first) / Math.max(first, 0.0001)) * 100;
            ctx.fillStyle = pct >= 0 ? '#0b7d23' : '#b22121';
            ctx.font = '12px Arial';
            ctx.fillText(`${{rangeLabel}} change: ${{pct.toFixed(2)}}%`, padLeft, 14);

            ctx.fillStyle = '#5f6b85';
            ctx.fillText(formatMoney(max), 8, padTop + 4);
            ctx.fillText(formatMoney(min), 8, padTop + h);

            if (activeHoverIndex !== null && points[activeHoverIndex]) {{
                const hp = points[activeHoverIndex];
                ctx.strokeStyle = 'rgba(41,82,255,0.35)';
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.moveTo(hp.x, padTop);
                ctx.lineTo(hp.x, padTop + h);
                ctx.stroke();

                ctx.fillStyle = '#2952ff';
                ctx.beginPath();
                ctx.arc(hp.x, hp.y, 3.5, 0, Math.PI * 2);
                ctx.fill();

                const tipText = `${{hp.label}} · ${{formatMoney(hp.value)}}`;
                ctx.font = '12px Arial';
                const textWidth = ctx.measureText(tipText).width;
                const tipX = Math.min(Math.max(hp.x - textWidth / 2 - 8, padLeft), padLeft + w - textWidth - 16);
                const tipY = Math.max(padTop + 4, hp.y - 28);
                ctx.fillStyle = 'rgba(17,32,59,0.92)';
                ctx.fillRect(tipX, tipY, textWidth + 16, 18);
                ctx.fillStyle = '#ffffff';
                ctx.fillText(tipText, tipX + 8, tipY + 13);
            }}

            drawModel = {{ padLeft, padRight, padTop, padBottom, width, height, points }};
            updateSummary(labels, data, rangeLabel);
        }}

        chartButtons.forEach((btn) => {{
            btn.addEventListener('click', () => {{
                chartButtons.forEach((b) => b.classList.remove('active'));
                btn.classList.add('active');
                activeRange = btn.dataset.range || '1Y';
                activeHoverIndex = null;
                drawLine(activeRange);
            }});
        }});

        chartCanvas.addEventListener('mousemove', (event) => {{
            if (!drawModel || !drawModel.points.length) return;
            const rect = chartCanvas.getBoundingClientRect();
            const x = event.clientX - rect.left;
            if (x < drawModel.padLeft || x > drawModel.width - drawModel.padRight) {{
                if (activeHoverIndex !== null) {{
                    activeHoverIndex = null;
                    drawLine(activeRange);
                }}
                return;
            }}

            const ratio = (x - drawModel.padLeft) / Math.max(drawModel.width - drawModel.padLeft - drawModel.padRight, 1);
            const idx = Math.round(ratio * Math.max(drawModel.points.length - 1, 0));
            if (idx !== activeHoverIndex) {{
                activeHoverIndex = idx;
                drawLine(activeRange);
            }}
        }});

        chartCanvas.addEventListener('mouseleave', () => {{
            if (activeHoverIndex !== null) {{
                activeHoverIndex = null;
                drawLine(activeRange);
            }}
        }});

        window.addEventListener('resize', () => drawLine(activeRange));

        drawLine(activeRange);

        function postParentHeight() {{
            if (window.parent === window) return;
            const doc = document.documentElement;
            const body = document.body;
            const height = Math.max(
                body ? body.scrollHeight : 0,
                body ? body.offsetHeight : 0,
                doc ? doc.scrollHeight : 0,
                doc ? doc.offsetHeight : 0,
            );
            try {{
                window.parent.postMessage({{ type: 'analysis-height', height }}, '*');
            }} catch (_) {{}}
        }}

        postParentHeight();
        setTimeout(postParentHeight, 120);
        window.addEventListener('resize', postParentHeight);
    </script>
    <footer class="footer">{APP_COPYRIGHT}</footer>
</body>
</html>
"""


async def home_page(request):
    message = request.query_params.get("msg", "")
    return HTMLResponse(render_home_page(message=message))


async def open_app(request):
    app_id = str(request.path_params.get("app_id", "")).strip().lower()
    app_item = get_app_by_id(app_id)
    if app_item is None:
        return redirect_hub_with_message("Selected app was not found")

    app_type = str(app_item.get("type", "")).strip().lower()
    open_path = str(app_item.get("open_path", "")).strip()

    if app_type == "internal":
        if not open_path:
            return redirect_hub_with_message("App is missing an internal path")
        return RedirectResponse(url=open_path, status_code=303)

    if app_type == "static_html":
        file_path = app_item.get("file_path")
        if not isinstance(file_path, Path) or not file_path.exists():
            return redirect_hub_with_message("App file is missing")
        return RedirectResponse(url=open_path or ROOT_PATH, status_code=303)

    if app_type == "external_url":
        external_url = str(app_item.get("external_url", "")).strip()
        if not external_url:
            return redirect_hub_with_message("App URL is not configured")
        return RedirectResponse(url=external_url, status_code=303)

    if app_type == "process":
        ok, message = start_process_app(app_item)
        if not ok:
            return redirect_hub_with_message(message)
        target_url = str(app_item.get("target_url") or app_item.get("open_path") or "").strip()
        if not target_url:
            return redirect_hub_with_message("App service started but no target URL is configured")
        return RedirectResponse(url=target_url, status_code=303)

    return redirect_hub_with_message("Unsupported app type")


async def legacy_vpm_redirect(request):
    query_string = request.url.query
    target = f"{VPM_PATH}?{query_string}" if query_string else VPM_PATH
    return RedirectResponse(url=target, status_code=308)


async def legacy_otd_redirect(request):
    query_string = request.url.query
    target = f"{OTD_PATH}?{query_string}" if query_string else OTD_PATH
    return RedirectResponse(url=target, status_code=308)


async def legacy_tracker_redirect(request):
    query_string = request.url.query
    target = f"{TRACKER_PATH}?{query_string}" if query_string else TRACKER_PATH
    return RedirectResponse(url=target, status_code=308)


async def otd_tool(request):
    otd_app_config = get_app_by_id("otd") or {}
    otd_path = otd_app_config.get("file_path")
    if not isinstance(otd_path, Path):
        otd_path = OTD_HTML_PATH

    if not otd_path.exists():
        return HTMLResponse(
            f"<!doctype html><html lang='en'><head><meta charset='utf-8'><title>OTD Tool</title></head>"
            f"<body style='font-family:Inter,sans-serif;padding:24px;'>"
            f"<h1>OTD Tool not found</h1>"
            f"<p>Expected file: <strong>apps/OTD/otd_estimator.html</strong></p>"
            f"<p><a href='{ROOT_PATH}'>Back to Home</a></p>"
            f"</body></html>",
            status_code=404,
        )

    content = otd_path.read_text(encoding="utf-8")
    content = (
        content.replace("__COREPORTAL_ASSET_THEME_PATH__", ASSET_THEME_PATH)
        .replace("__COREPORTAL_ROOT_PATH__", ROOT_PATH)
        .replace("__COREPORTAL_OTD_POLICY_PATH__", f"{OTD_PATH}/policy_years.json")
    )
    return HTMLResponse(content)


async def cvp_tool(request):
    cvp_app_config = get_app_by_id("cvp") or {}
    cvp_path = cvp_app_config.get("file_path")
    if not isinstance(cvp_path, Path):
        cvp_path = CVP_HTML_PATH

    if not cvp_path.exists():
        return HTMLResponse(
            f"<!doctype html><html lang='en'><head><meta charset='utf-8'><title>CVP Tool</title></head>"
            f"<body style='font-family:Inter,sans-serif;padding:24px;'>"
            f"<h1>CVP Tool not found</h1>"
            f"<p>Expected file: <strong>apps/CVP/cvp_planner.html</strong></p>"
            f"<p><a href='{ROOT_PATH}'>Back to Home</a></p>"
            f"</body></html>",
            status_code=404,
        )

    content = cvp_path.read_text(encoding="utf-8")
    content = (
        content.replace("__COREPORTAL_ASSET_THEME_PATH__", ASSET_THEME_PATH)
        .replace("__COREPORTAL_ROOT_PATH__", ROOT_PATH)
        .replace("__COREPORTAL_OTD_POLICY_PATH__", f"{CVP_PATH}/policy_years.json")
    )
    return HTMLResponse(content)


async def coreportal_theme_css(request):
    if THEME_CSS_PATH.exists():
        return FileResponse(THEME_CSS_PATH, media_type="text/css")
    return HTMLResponse(
        """
:root {
  --font-ui: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  --font-mono: Consolas, "SFMono-Regular", Menlo, Monaco, "Liberation Mono", "Courier New", monospace;
}
""".strip(),
        media_type="text/css",
    )


async def otd_policy_years(request):
    if OTD_POLICY_PATH.exists():
        return FileResponse(OTD_POLICY_PATH, media_type="application/json")
    return JSONResponse(
        {
            "2025": {
                "taxRate": 6.625,
                "lfisRate": 0.4,
                "lfisThreshold": 45000,
                "dmvPlusTitle": 144,
                "plate": 5,
            }
        }
    )


async def cvp_policy_years(request):
    if CVP_POLICY_PATH.exists():
        return FileResponse(CVP_POLICY_PATH, media_type="application/json")
    return JSONResponse(
        {
            "2025": {
                "taxRate": 6.625,
                "lfisRate": 0.4,
                "lfisThreshold": 45000,
                "dmvPlusTitle": 144,
                "plate": 5,
            }
        }
    )


def build_tracker_query_string(
        tenant_id: int | None,
        account_id: int | None,
        message: str = "",
        extras: dict[str, str] | None = None,
) -> str:
        params: dict[str, str] = {}
        if tenant_id is not None:
                params["tenant_id"] = str(tenant_id)
        if account_id is not None:
                params["account_id"] = str(account_id)
        if message:
                params["msg"] = message
        if extras:
                for key, value in extras.items():
                        if value:
                                params[key] = value
        return urlencode(params)


def redirect_tracker(
        message: str,
        tenant_id: int | None = None,
        account_id: int | None = None,
) -> RedirectResponse:
        query = build_tracker_query_string(tenant_id, account_id, message)
        url = TRACKER_PATH if not query else f"{TRACKER_PATH}?{query}"
        return RedirectResponse(url=url, status_code=303)


def resolve_tracker_selection(
        connection: sqlite3.Connection,
        selected_tenant_id: int | None,
        selected_account_id: int | None,
) -> tuple[list[sqlite3.Row], sqlite3.Row, list[sqlite3.Row], sqlite3.Row, list[sqlite3.Row]]:
        tenants = load_tenants(connection)
        if not tenants:
                tenant_id, _ = ensure_default_portfolio(connection, DEFAULT_TENANTS[0])
                ensure_default_bank_account(connection, tenant_id)
                tenants = load_tenants(connection)

        current_tenant = next((tenant for tenant in tenants if int(tenant["id"]) == selected_tenant_id), tenants[0])
        tenant_id = int(current_tenant["id"])

        accounts = load_bank_accounts(connection, tenant_id)
        if not accounts:
                ensure_default_bank_account(connection, tenant_id)
                accounts = load_bank_accounts(connection, tenant_id)

        current_account = next(
                (account for account in accounts if int(account["id"]) == selected_account_id),
                accounts[0],
        )

        portfolios = load_portfolios(connection, tenant_id)
        if not portfolios:
                ensure_default_portfolio(connection, str(current_tenant["name"]))
                portfolios = load_portfolios(connection, tenant_id)

        return tenants, current_tenant, accounts, current_account, portfolios


def load_owner_finance_snapshot(
    connection: sqlite3.Connection,
    tenant_id: int,
) -> dict[str, Any]:
    accounts = load_bank_accounts(connection, tenant_id)
    if not accounts:
        ensure_default_bank_account(connection, tenant_id)
        accounts = load_bank_accounts(connection, tenant_id)

    portfolios = load_portfolios(connection, tenant_id)
    if not portfolios:
        tenant_row = connection.execute("SELECT name FROM tenants WHERE id=?", (tenant_id,)).fetchone()
        owner_name = str(tenant_row["name"]) if tenant_row else DEFAULT_TENANTS[0]
        ensure_default_portfolio(connection, owner_name)
        portfolios = load_portfolios(connection, tenant_id)

    account_rows: list[tuple[str, str, Decimal]] = []
    bank_total = Decimal("0")
    for account in accounts:
        balance = get_bank_balance(connection, int(account["id"]))
        bank_total = to_decimal(bank_total + balance, MONEY_QUANT)
        account_rows.append((str(account["name"]), str(account["account_type"]), balance))

    portfolio_rows: list[tuple[str, Decimal, Decimal, Decimal]] = []
    vpm_total = Decimal("0")
    for portfolio in portfolios:
        portfolio_id = int(portfolio["id"])
        cash_balance = get_cash_balance(connection, portfolio_id)
        positions = build_positions(connection, portfolio_id)
        market_value = to_decimal(sum(position["current_value"] for position in positions), MONEY_QUANT)
        total_value = to_decimal(cash_balance + market_value, MONEY_QUANT)
        vpm_total = to_decimal(vpm_total + total_value, MONEY_QUANT)
        portfolio_rows.append((str(portfolio["name"]), cash_balance, market_value, total_value))

    spending_series = build_monthly_spending_series(connection, tenant_id, months=12)
    networth_series = build_networth_estimate_series(connection, tenant_id, months=12)
    spent_this_month = monthly_spending_for_month(connection, tenant_id, date.today().year, date.today().month)
    net_worth = to_decimal(bank_total + vpm_total, MONEY_QUANT)

    return {
        "accounts": accounts,
        "portfolios": portfolios,
        "account_rows": account_rows,
        "portfolio_rows": portfolio_rows,
        "bank_total": bank_total,
        "vpm_total": vpm_total,
        "net_worth": net_worth,
        "spent_this_month": spent_this_month,
        "spending_series": spending_series,
        "networth_series": networth_series,
    }


def render_tracker_page(
        message: str = "",
        selected_tenant_id: int | None = None,
        selected_account_id: int | None = None,
) -> str:
        with db_connection() as connection:
                tenants, current_tenant, accounts, current_account, portfolios = resolve_tracker_selection(
                        connection,
                        selected_tenant_id,
                        selected_account_id,
                )
                current_tenant_id = int(current_tenant["id"])
                current_account_id = int(current_account["id"])

                entries = load_bank_entries(connection, current_account_id, limit=80)
                account_balance = get_bank_balance(connection, current_account_id)

                sum_row = connection.execute(
                        """
                        SELECT
                            COALESCE(SUM(CASE WHEN CAST(amount AS REAL) > 0 THEN CAST(amount AS REAL) ELSE 0 END), 0) AS income_total,
                            COALESCE(SUM(CASE WHEN CAST(amount AS REAL) < 0 THEN -CAST(amount AS REAL) ELSE 0 END), 0) AS expense_total
                        FROM bank_ledger
                        WHERE account_id=? AND entry_date <= date('now')
                        """,
                        (current_account_id,),
                ).fetchone()
                owner_snapshot = load_owner_finance_snapshot(connection, current_tenant_id)

        income_total = to_decimal(sum_row["income_total"], MONEY_QUANT)
        expense_total = to_decimal(sum_row["expense_total"], MONEY_QUANT)
        net_total = to_decimal(income_total - expense_total, MONEY_QUANT)

        tenant_options_html = "".join(
                f"<option value=\"{int(tenant['id'])}\"{' selected' if int(tenant['id']) == current_tenant_id else ''}>{html.escape(str(tenant['name']))}</option>"
                for tenant in tenants
        )

        account_links_html = "".join(
                f"<a class=\"portfolio-pill{' active' if int(account['id']) == current_account_id else ''}\" href=\"{TRACKER_PATH}?{build_tracker_query_string(current_tenant_id, int(account['id']))}\">{html.escape(str(account['name']))}</a>"
                for account in accounts
        )

        portfolio_options_html = "".join(
                f"<option value=\"{int(portfolio['id'])}\">{html.escape(str(portfolio['name']))}</option>"
                for portfolio in portfolios
        )

        entry_rows_html = ""
        if entries:
                for row in entries:
                        amount = to_decimal(row["amount"], MONEY_QUANT)
                        amount_class = "gain" if amount >= 0 else "loss"
                        entry_rows_html += f"""
                        <tr>
                            <td class=\"{amount_class}\">{format_money(amount)}</td>
                            <td>{html.escape(row['entry_date'])}</td>
                            <td>{html.escape(str(row['category'] or '-'))}</td>
                            <td>{html.escape(str(row['note'] or '-'))}</td>
                        </tr>
                        """
        else:
                entry_rows_html = "<tr><td colspan='4'>No tracker entries yet for this account.</td></tr>"

        account_rows_html = "".join(
            f"<tr><td>{html.escape(name)}</td><td>{html.escape(account_type)}</td><td>{format_money(balance)}</td></tr>"
            for name, account_type, balance in owner_snapshot["account_rows"]
        ) or "<tr><td colspan='3'>No tracker accounts found.</td></tr>"

        portfolio_rows_html = "".join(
            f"<tr><td>{html.escape(name)}</td><td>{format_money(cash)}</td><td>{format_money(market)}</td><td>{format_money(total)}</td></tr>"
            for name, cash, market, total in owner_snapshot["portfolio_rows"]
        ) or "<tr><td colspan='4'>No portfolios found.</td></tr>"

        chart_labels_json = json.dumps([label for label, _ in owner_snapshot["networth_series"]])
        networth_values_json = json.dumps([float(value) for _, value in owner_snapshot["networth_series"]])
        spending_values_json = json.dumps([float(value) for _, value in owner_snapshot["spending_series"]])

        flash_html = f"<div class='flash'>{html.escape(message)}</div>" if message else ""
        today_value = date.today().isoformat()

        return f"""
<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\">
    <title>BAT · Bank Account Tracker</title>
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
    <style>
        {shared_theme_css()}
        .page {{ max-width: {COMMON_PAGE_MAX_WIDTH}px; width: 100%; margin: 0 auto; padding: 24px; box-sizing: border-box; overflow: hidden; }}
        .hero {{ background: linear-gradient(135deg, #13203c, #2952ff); color: white; padding: 24px; border-radius: 20px; box-shadow: var(--shadow); }}
        .hero h1 {{ margin: 0 0 8px 0; font-size: 34px; }}
        .hero p {{ margin: 0; max-width: 920px; color: rgba(255,255,255,0.86); line-height: 1.5; }}
        .hero-meta {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 16px; }}
        .hero-badge {{ background: rgba(255,255,255,0.14); border: 1px solid rgba(255,255,255,0.2); border-radius: 999px; padding: 8px 12px; font-size: 13px; }}
        .hero-badge a {{ color:#fff; text-decoration:none; }}
        .flash {{ background: #e8f4ff; border: 1px solid #b8ddff; padding: 12px 14px; border-radius: 12px; margin: 16px 0 0 0; color: #12456d; }}
        .metrics {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 14px; margin: 18px 0; }}
        .metric {{ background: var(--card); padding: 16px; border-radius: 16px; box-shadow: var(--shadow); border: 1px solid rgba(217,225,238,0.75); }}
        .label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }}
        .value {{ font-size: 26px; font-weight: 800; margin-top: 6px; }}
        .glance {{ margin-bottom:18px; }}
        .glance-controls {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-bottom:12px; }}
        .glance-graph {{ width:100%; height:220px; border:1px solid var(--line); border-radius:12px; background:#fff; display:block; }}
        .glance-boxes {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:12px; margin-top:12px; }}
        .glance-box {{ border:1px solid var(--line); border-radius:12px; background:#fff; padding:14px; }}
        .glance-box .k {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:0.05em; }}
        .glance-box .v {{ font-size:24px; font-weight:900; margin-top:6px; }}
        .glance-box .s {{ color:var(--muted); font-size:13px; margin-top:2px; }}
        .overview-grid {{ display:grid; grid-template-columns: 1fr 1fr; gap:18px; margin-bottom:18px; min-width:0; width:100%; }}
        .overview-grid > * {{ min-width:0; overflow:hidden; }}
        .layout {{ display:grid; grid-template-columns: 360px minmax(0, 1fr); gap: 18px; align-items:start; min-width:0; width:100%; }}
        .layout > * {{ min-width:0; overflow:hidden; }}
        .stack {{ display:grid; gap:18px; min-width:0; }}
        .main-stack {{ display:grid; gap:18px; min-width:0; }}
        .actions-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap:18px; margin-top:18px; }}
        .card {{ background: var(--card); border-radius: 18px; box-shadow: var(--shadow); border: 1px solid rgba(217,225,238,0.75); padding: 18px; min-width:0; overflow:hidden; }}
        .card h2 {{ margin:0 0 8px 0; font-size: 21px; }}
        .card p {{ margin:0 0 14px 0; color: var(--muted); line-height: 1.45; }}
        .table-wrap {{ overflow-x:auto; width:100%; }}
        form {{ display:grid; gap:9px; }}
        label {{ font-weight:700; font-size:13px; color: var(--ink); }}
        input, select, button {{ width:100%; border-radius:10px; border:1px solid var(--line); padding:11px 12px; font-size:14px; }}
        input, select {{ background:#fff; color:var(--ink); }}
        button {{ background:var(--brand); color:#fff; font-weight:800; cursor:pointer; border:0; }}
        button.secondary {{ background:#fff; color:var(--ink); border:1px solid var(--line); }}
        .portfolio-nav {{ display:flex; gap:10px; flex-wrap:wrap; margin-bottom:12px; }}
        .portfolio-pill {{ text-decoration:none; color: var(--ink); background:#f5f7fc; border:1px solid var(--line); border-radius:999px; padding:8px 12px; font-weight:700; font-size:13px; }}
        .portfolio-pill.active {{ background:var(--brand); color:#fff; border-color:var(--brand); }}
        table {{ width:100%; border-collapse: collapse; }}
        th, td {{ border-bottom:1px solid #eef2f8; padding:10px 8px; text-align:left; font-size:14px; white-space:nowrap; }}
        th {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:0.05em; background:#fbfcff; }}
        .gain {{ color:var(--gain); font-weight:800; }}
        .loss {{ color:var(--loss); font-weight:800; }}
        @media (max-width: 1080px) {{ .overview-grid {{ grid-template-columns: 1fr; }} .layout {{ grid-template-columns: 1fr; }} }}
        @media (max-width: 640px) {{ .page {{ padding:14px; }} .hero h1 {{ font-size:28px; }} }}
    </style>
</head>
<body>
    <div class=\"page\">
        <section class=\"hero\">
            <h1>BAT · Bank Account Tracker</h1>
            <p style="margin: 6px 0 0 0; font-size: 14px; opacity: 0.95;">{APP_AUTHOR}</p>
            <p>Virtual bank-account ledger with built-in owner overview for cashflow, balances, and net worth. Move funds directly into VPM to keep both sides in sync.</p>
            <div class=\"hero-meta\">
                <div class=\"hero-badge\">Owner: {html.escape(str(current_tenant['name']))}</div>
                <div class=\"hero-badge\">Account: {html.escape(str(current_account['name']))}</div>
                <div class=\"hero-badge\"><a href=\"{VPM_PATH}\">Open VPM</a></div>
                <div class=\"hero-badge\"><a href=\"#overview\">Overview built in</a></div>
                <div class=\"hero-badge\"><a href=\"/\">Back to Home</a></div>
            </div>
        </section>
        {flash_html}

        <section class=\"metrics\">
            <div class=\"metric\"><div class=\"label\">Account Balance</div><div class=\"value\">{format_money(account_balance)}</div></div>
            <div class=\"metric\"><div class=\"label\">Total Income</div><div class=\"value gain\">{format_money(income_total)}</div></div>
            <div class=\"metric\"><div class=\"label\">Total Expense</div><div class=\"value loss\">{format_money(-expense_total)}</div></div>
            <div class=\"metric\"><div class=\"label\">Net Cashflow</div><div class=\"value {'gain' if net_total >= 0 else 'loss'}\">{format_money(net_total)}</div></div>
        </section>

        <section class="card glance" id="overview">
            <h2>Overview</h2>
            <p>Owner-level view of BAT balances, VPM value, and trend lines. This replaces the separate net worth page.</p>
            <div class="glance-controls">
                <label for="glance-mode">View</label>
                <select id="glance-mode">
                    <option value="networth">Net worth history</option>
                    <option value="spending">Monthly spending</option>
                </select>
            </div>
            <svg id="glance-graph" class="glance-graph" viewBox="0 0 960 220" preserveAspectRatio="none"></svg>
            <div class="glance-boxes">
                <div class="glance-box">
                    <div class="k">BAT total</div>
                    <div class="v">{format_money(owner_snapshot['bank_total'])}</div>
                    <div class="s">All tracker accounts for this owner</div>
                </div>
                <div class="glance-box">
                    <div class="k">VPM total</div>
                    <div class="v">{format_money(owner_snapshot['vpm_total'])}</div>
                    <div class="s">Cash plus market value</div>
                </div>
                <div class="glance-box">
                    <div class="k">Net worth</div>
                    <div class="v">{format_money(owner_snapshot['net_worth'])}</div>
                    <div class="s">Latest combined estimate</div>
                </div>
                <div class="glance-box">
                    <div class="k">This month spend</div>
                    <div class="v">{format_money(owner_snapshot['spent_this_month'])}</div>
                    <div class="s">Spending recorded so far this month</div>
                </div>
            </div>
        </section>

        <div class="overview-grid">
            <section class="card">
                <h2>Tracker Accounts</h2>
                <p>Cash and bank balances across BAT accounts for this owner.</p>
                <div class="table-wrap">
                    <table>
                        <thead><tr><th>Account</th><th>Type</th><th>Balance</th></tr></thead>
                        <tbody>{account_rows_html}</tbody>
                    </table>
                </div>
            </section>

            <section class="card">
                <h2>VPM Portfolios</h2>
                <p>Portfolio cash plus market value from VPM.</p>
                <div class="table-wrap">
                    <table>
                        <thead><tr><th>Portfolio</th><th>Cash</th><th>Market</th><th>Total</th></tr></thead>
                        <tbody>{portfolio_rows_html}</tbody>
                    </table>
                </div>
            </section>
        </div>

        <div class=\"layout\">
            <div class=\"stack\">
                <section class=\"card\">
                    <h2>Workspace</h2>
                    <p>Pick owner and account context for tracking and transfers.</p>
                    <form method=\"get\" action=\"{TRACKER_PATH}\">
                        <label>Owner</label>
                        <select name=\"tenant_id\">{tenant_options_html}</select>
                        <button type=\"submit\">Open Owner</button>
                    </form>
                    <div class=\"portfolio-nav\" style=\"margin-top:12px;\">{account_links_html}</div>
                    <form method=\"post\" action=\"{TRACKER_PATH}/account/add\">
                        <input type=\"hidden\" name=\"tenant_id\" value=\"{current_tenant_id}\">
                        <label>New Account Name</label>
                        <input name=\"account_name\" type=\"text\" maxlength=\"120\" placeholder=\"e.g. Checking, Savings\" required>
                        <label>Account Type</label>
                        <select name=\"account_type\">
                            <option value=\"checking\">checking</option>
                            <option value=\"savings\">savings</option>
                            <option value=\"cash\">cash</option>
                            <option value=\"credit\">credit</option>
                        </select>
                        <button type=\"submit\" class=\"secondary\">Add Account</button>
                    </form>
                </section>
            </div>

            <div class=\"main-stack\">
                <section class=\"card\">
                    <h2>Ledger: {html.escape(str(current_account['name']))}</h2>
                    <p>All income and expense entries for the selected account. Future salary projections appear here but are excluded from the current balance.</p>
                    <div class=\"table-wrap\">
                        <table>
                            <thead>
                                <tr>
                                    <th>Amount</th>
                                    <th>Date</th>
                                    <th>Category</th>
                                    <th>Note</th>
                                </tr>
                            </thead>
                            <tbody>
                                {entry_rows_html}
                            </tbody>
                        </table>
                    </div>
                </section>

                <section class=\"card\">
                    <h2>Deposit Cash</h2>
                    <p>Add funds any time, including paycheck, bonus, or one-off deposit.</p>
                    <form method=\"post\" action=\"{TRACKER_PATH}/deposit\">
                        <input type=\"hidden\" name=\"tenant_id\" value=\"{current_tenant_id}\">
                        <input type=\"hidden\" name=\"account_id\" value=\"{current_account_id}\">
                        <label>Amount</label>
                        <input name=\"amount\" type=\"number\" min=\"0.01\" step=\"0.01\" placeholder=\"1000.00\" required>
                        <label>Date</label>
                        <input name=\"entry_date\" type=\"date\" value=\"{today_value}\" required>
                        <label>Note</label>
                        <input name=\"note\" type=\"text\" maxlength=\"250\" placeholder=\"Paycheck / bonus / cash deposit\">
                        <button type=\"submit\">Deposit</button>
                    </form>
                </section>
            </div>
        </div>

        <div class=\"actions-grid\">
            <section class=\"card\">
                <h2>📅 Bi-Weekly Salary</h2>
                <p>Schedule recurring paycheck entries every 14 days.</p>
                <form method=\"post\" action=\"{TRACKER_PATH}/salary/add\">
                    <input type=\"hidden\" name=\"tenant_id\" value=\"{current_tenant_id}\">
                    <input type=\"hidden\" name=\"account_id\" value=\"{current_account_id}\">
                    <label>Amount per Paycheck</label>
                    <input name=\"amount\" type=\"number\" min=\"0.01\" step=\"0.01\" placeholder=\"4960.00\" required>
                    <label>First Payday</label>
                    <input name=\"first_pay_date\" type=\"date\" value=\"{today_value}\" required>
                    <label>Number of Pay Cycles</label>
                    <input name=\"cycles\" type=\"number\" min=\"1\" max=\"104\" step=\"1\" value=\"13\" required>
                    <label>Note</label>
                    <input name=\"note\" type=\"text\" maxlength=\"250\" placeholder=\"Bi-weekly payroll\">
                    <button type=\"submit\" class=\"secondary\">Schedule Salary</button>
                </form>
            </section>

            <section class=\"card\">
                <h2>🔁 Monthly Spending Plan</h2>
                <p>Add a recurring monthly expense with a preset or custom category.</p>
                <form method=\"post\" action=\"{TRACKER_PATH}/spending/add\">
                    <input type=\"hidden\" name=\"tenant_id\" value=\"{current_tenant_id}\">
                    <input type=\"hidden\" name=\"account_id\" value=\"{current_account_id}\">
                    <label>Monthly Amount</label>
                    <input name=\"monthly_amount\" type=\"number\" min=\"0.01\" step=\"0.01\" placeholder=\"150.00\" required>
                    <label>Category</label>
                    <select name=\"category\">
                        <option value=\"utilities - gas\">utilities - gas</option>
                        <option value=\"utilities - water\">utilities - water</option>
                        <option value=\"utilities - sewer\">utilities - sewer</option>
                        <option value=\"mobile\">mobile</option>
                        <option value=\"HOA\">HOA</option>
                        <option value=\"streaming\">streaming</option>
                        <option value=\"groceries & delivery\">groceries &amp; delivery</option>
                        <option value=\"restaurants\">restaurants</option>
                        <option value=\"gasoline\">gasoline</option>
                        <option value=\"shopping & misc\">shopping &amp; misc</option>
                        <option value=\"property tax\">property tax</option>
                        <option value=\"mortgage\">mortgage</option>
                        <option value=\"car lease\">car lease</option>
                        <option value=\"electricity\">electricity</option>
                        <option value=\"internet\">internet</option>
                        <option value=\"custom\">custom</option>
                    </select>
                    <label>Custom Category (if "custom" selected)</label>
                    <input name=\"custom_category\" type=\"text\" maxlength=\"80\" placeholder=\"gym, insurance, anything\">
                    <label>Start Date</label>
                    <input name=\"start_date\" type=\"date\" value=\"{today_value}\" required>
                    <label>Number of Months</label>
                    <input name=\"months\" type=\"number\" min=\"1\" max=\"120\" step=\"1\" value=\"12\" required>
                    <label>Note</label>
                    <input name=\"note\" type=\"text\" maxlength=\"250\" placeholder=\"Optional note\">
                    <button type=\"submit\" class=\"secondary\">Add Spending Plan</button>
                </form>
            </section>

            <section class=\"card\">
                <h2>✏️ Single Entry</h2>
                <p>Record a one-off income or expense not covered by a plan.</p>
                <form method=\"post\" action=\"{TRACKER_PATH}/entry/add\">
                    <input type=\"hidden\" name=\"tenant_id\" value=\"{current_tenant_id}\">
                    <input type=\"hidden\" name=\"account_id\" value=\"{current_account_id}\">
                    <label>Type</label>
                    <select name=\"entry_type\">
                        <option value=\"income\">income</option>
                        <option value=\"expense\">expense</option>
                    </select>
                    <label>Amount</label>
                    <input name=\"amount\" type=\"number\" min=\"0.01\" step=\"0.01\" placeholder=\"250.00\" required>
                    <label>Date</label>
                    <input name=\"entry_date\" type=\"date\" value=\"{today_value}\" required>
                    <label>Category</label>
                    <input name=\"category\" type=\"text\" maxlength=\"80\" placeholder=\"salary, rent, groceries\">
                    <label>Note</label>
                    <input name=\"note\" type=\"text\" maxlength=\"250\" placeholder=\"Optional note\">
                    <button type=\"submit\">Save Entry</button>
                </form>
            </section>

            <section class=\"card\">
                <h2>➡️ Transfer to VPM</h2>
                <p>Move funds from this account into a VPM portfolio for investing.</p>
                <form method=\"post\" action=\"{TRACKER_PATH}/transfer-to-vpm\">
                    <input type=\"hidden\" name=\"tenant_id\" value=\"{current_tenant_id}\">
                    <input type=\"hidden\" name=\"account_id\" value=\"{current_account_id}\">
                    <label>Portfolio</label>
                    <select name=\"portfolio_id\">{portfolio_options_html}</select>
                    <label>Amount</label>
                    <input name=\"amount\" type=\"number\" min=\"0.01\" step=\"0.01\" placeholder=\"500.00\" required>
                    <label>Date</label>
                    <input name=\"entry_date\" type=\"date\" value=\"{today_value}\" required>
                    <label>Note</label>
                    <input name=\"note\" type=\"text\" maxlength=\"250\" placeholder=\"Monthly invest transfer\">
                    <button type=\"submit\">Transfer</button>
                </form>
            </section>

            <section class=\"card\">
                <h2>🧹 Zeroize BAT Account</h2>
                <p>Delete all ledger entries for the selected BAT account. A safety snapshot is created first.</p>
                <form method=\"post\" action=\"{TRACKER_PATH}/zeroize\">
                    <input type=\"hidden\" name=\"tenant_id\" value=\"{current_tenant_id}\">
                    <input type=\"hidden\" name=\"account_id\" value=\"{current_account_id}\">
                    <button type=\"submit\" class=\"secondary\" onclick=\"return confirm('Zeroize this BAT account ledger? This will remove all entries for the selected account and create a safety snapshot first.')\">Zeroize Current BAT Account</button>
                </form>
            </section>
        </div>

        <footer class=\"footer\">{APP_COPYRIGHT}</footer>
    </div>
    <script>
        const chartLabels = {chart_labels_json};
        const netWorthSeries = {networth_values_json};
        const spendingSeries = {spending_values_json};
        const modeSelect = document.getElementById('glance-mode');
        const svg = document.getElementById('glance-graph');

        function drawSeries(values, color) {{
            const width = 960;
            const height = 220;
            const padX = 36;
            const padY = 24;
            const chartW = width - padX * 2;
            const chartH = height - padY * 2;
            const maxV = Math.max(...values, 1);
            const minV = Math.min(...values, 0);
            const span = Math.max(maxV - minV, 1);

            const points = values.map((value, index) => {{
                const x = padX + (chartW * index) / Math.max(values.length - 1, 1);
                const y = padY + chartH - ((value - minV) / span) * chartH;
                return `${{x.toFixed(1)}},${{y.toFixed(1)}}`;
            }}).join(' ');

            const labelText = chartLabels.map((label, index) => {{
                if (index % 3 !== 0 && index !== chartLabels.length - 1) return '';
                const x = padX + (chartW * index) / Math.max(chartLabels.length - 1, 1);
                return `<text x="${{x.toFixed(1)}}" y="208" fill="#5f6b85" font-size="11" text-anchor="middle">${{label}}</text>`;
            }}).join('');

            svg.innerHTML = `
                <rect x="0" y="0" width="960" height="220" fill="#ffffff"/>
                <line x1="36" y1="196" x2="924" y2="196" stroke="#d9e1ee" stroke-width="1"/>
                <polyline points="${{points}}" fill="none" stroke="${{color}}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
                ${{labelText}}
            `;
        }}

        function refreshGlance() {{
            const mode = modeSelect.value;
            if (mode === 'spending') {{
                drawSeries(spendingSeries, '#b22121');
            }} else {{
                drawSeries(netWorthSeries, '#2952ff');
            }}
        }}

        modeSelect.addEventListener('change', refreshGlance);
        refreshGlance();
    </script>
</body>
</html>
"""


async def tracker_dashboard(request):
        message = request.query_params.get("msg", "")
        tenant_id = parse_optional_int(request.query_params.get("tenant_id"))
        account_id = parse_optional_int(request.query_params.get("account_id"))
        return HTMLResponse(render_tracker_page(message=message, selected_tenant_id=tenant_id, selected_account_id=account_id))


async def tracker_account_add(request):
        form = await parse_form(request)
        tenant_id = parse_optional_int(form.get("tenant_id"))
        if tenant_id is None:
                return redirect_tracker("Account setup failed: owner is required")

        ok, message, account_id = create_bank_account(
                tenant_id,
                form.get("account_name", ""),
                form.get("account_type", "checking"),
        )
        status_message = message if ok else f"Account setup note: {message}"
        return redirect_tracker(status_message, tenant_id=tenant_id, account_id=account_id)


async def tracker_entry_add(request):
        form = await parse_form(request)
        tenant_id = parse_optional_int(form.get("tenant_id"))
        account_id = parse_optional_int(form.get("account_id"))
        entry_type = form.get("entry_type", "income").strip().lower()
        entry_date = form.get("entry_date") or date.today().isoformat()
        category = form.get("category", "")
        note = form.get("note", "")

        if account_id is None:
                return redirect_tracker("Entry failed: account is required", tenant_id=tenant_id)

        try:
                amount = parse_positive_decimal(form.get("amount", ""), MONEY_QUANT)
        except ValueError as error:
                return redirect_tracker(f"Entry failed: {error}", tenant_id=tenant_id, account_id=account_id)

        signed_amount = amount if entry_type == "income" else -amount
        add_bank_entry(account_id, signed_amount, entry_date, category, note)
        return redirect_tracker("Entry saved", tenant_id=tenant_id, account_id=account_id)


async def tracker_deposit(request):
        form = await parse_form(request)
        tenant_id = parse_optional_int(form.get("tenant_id"))
        account_id = parse_optional_int(form.get("account_id"))
        entry_date = form.get("entry_date") or date.today().isoformat()
        note = form.get("note", "")

        if account_id is None:
            return redirect_tracker("Deposit failed: account is required", tenant_id=tenant_id)

        try:
            amount = parse_positive_decimal(form.get("amount", ""), MONEY_QUANT)
        except ValueError as error:
            return redirect_tracker(f"Deposit failed: {error}", tenant_id=tenant_id, account_id=account_id)

        add_bank_entry(account_id, amount, entry_date, "deposit", note)
        return redirect_tracker("Deposit posted", tenant_id=tenant_id, account_id=account_id)


async def tracker_salary_add(request):
        form = await parse_form(request)
        tenant_id = parse_optional_int(form.get("tenant_id"))
        account_id = parse_optional_int(form.get("account_id"))
        note = form.get("note", "")

        if account_id is None:
            return redirect_tracker("Salary plan failed: account is required", tenant_id=tenant_id)

        try:
            amount = parse_positive_decimal(form.get("amount", ""), MONEY_QUANT)
            first_pay_date = date.fromisoformat(form.get("first_pay_date") or date.today().isoformat())
            cycles_raw = int(str(form.get("cycles") or "13"))
            cycles = max(1, min(cycles_raw, 104))
        except ValueError as error:
            return redirect_tracker(f"Salary plan failed: {error}", tenant_id=tenant_id, account_id=account_id)

        entries: list[tuple[Decimal, str, str, str]] = []
        for cycle in range(cycles):
            pay_day = first_pay_date + timedelta(days=14 * cycle)
            entries.append((amount, pay_day.isoformat(), "salary", note or "Bi-weekly salary"))

        created = add_bank_entries(account_id, entries)
        return redirect_tracker(f"Salary plan added ({created} entries)", tenant_id=tenant_id, account_id=account_id)


async def tracker_spending_add(request):
        form = await parse_form(request)
        tenant_id = parse_optional_int(form.get("tenant_id"))
        account_id = parse_optional_int(form.get("account_id"))
        note = form.get("note", "")

        if account_id is None:
            return redirect_tracker("Spending plan failed: account is required", tenant_id=tenant_id)

        try:
            monthly_amount = parse_positive_decimal(form.get("monthly_amount", ""), MONEY_QUANT)
            start_date = date.fromisoformat(form.get("start_date") or date.today().isoformat())
            months_raw = int(str(form.get("months") or "12"))
            months = max(1, min(months_raw, 120))
        except ValueError as error:
            return redirect_tracker(f"Spending plan failed: {error}", tenant_id=tenant_id, account_id=account_id)

        category = (form.get("category") or "expense").strip()
        custom_category = (form.get("custom_category") or "").strip()
        if category.lower() == "custom" and custom_category:
            category = custom_category
        if not category:
            category = "expense"

        signed_amount = to_decimal(-monthly_amount, MONEY_QUANT)
        entries: list[tuple[Decimal, str, str, str]] = []
        for index in range(months):
            spend_day = shift_months(start_date, index)
            entries.append((signed_amount, spend_day.isoformat(), category, note or f"Monthly {category}"))

        created = add_bank_entries(account_id, entries)
        return redirect_tracker(f"Spending plan added ({created} entries)", tenant_id=tenant_id, account_id=account_id)


async def tracker_transfer_to_vpm(request):
        form = await parse_form(request)
        tenant_id = parse_optional_int(form.get("tenant_id"))
        account_id = parse_optional_int(form.get("account_id"))
        portfolio_id = parse_optional_int(form.get("portfolio_id"))
        entry_date = form.get("entry_date") or date.today().isoformat()
        note = form.get("note", "")

        if account_id is None or portfolio_id is None:
                return redirect_tracker("Transfer failed: account and portfolio are required", tenant_id=tenant_id, account_id=account_id)

        try:
                amount = parse_positive_decimal(form.get("amount", ""), MONEY_QUANT)
        except ValueError as error:
                return redirect_tracker(f"Transfer failed: {error}", tenant_id=tenant_id, account_id=account_id)

        ok, message = transfer_bank_to_vpm(account_id, portfolio_id, amount, entry_date, note)
        if not ok:
                return redirect_tracker(f"Transfer failed: {message}", tenant_id=tenant_id, account_id=account_id)

        return redirect_tracker(message, tenant_id=tenant_id, account_id=account_id)


async def tracker_zeroize(request):
    form = await parse_form(request)
    tenant_id = parse_optional_int(form.get("tenant_id"))
    account_id = parse_optional_int(form.get("account_id"))

    if account_id is None:
        return redirect_tracker("Zeroize failed: account is required", tenant_id=tenant_id)

    try:
        ok, message = zeroize_bank_account(account_id)
    except Exception as error:
        return redirect_tracker(f"Zeroize failed: {error}", tenant_id=tenant_id, account_id=account_id)

    if not ok:
        return redirect_tracker(f"Zeroize failed: {message}", tenant_id=tenant_id, account_id=account_id)

    return redirect_tracker(message, tenant_id=tenant_id, account_id=account_id)


async def dashboard(request):
    message = request.query_params.get("msg", "")
    tenant_id = parse_optional_int(request.query_params.get("tenant_id"))
    portfolio_id = parse_optional_int(request.query_params.get("portfolio_id"))
    analyze_error = request.query_params.get("analyze_error", "")
    analyze_input = request.query_params.get("analyze_input", "")
    analyze_note = request.query_params.get("analyze_note", "")
    analyze_depth = request.query_params.get("depth", "quick").strip().lower()
    if analyze_depth not in {"quick", "deep"}:
        analyze_depth = "quick"
    return HTMLResponse(
        render_dashboard(
            message=message,
            selected_tenant_id=tenant_id,
            selected_portfolio_id=portfolio_id,
            analyze_error=analyze_error,
            analyze_input=analyze_input,
            analyze_note=analyze_note,
            analyze_depth=analyze_depth,
        )
    )


async def analyze_stock(request):
    raw_symbol = request.query_params.get("symbol", "").strip()
    depth = request.query_params.get("depth", "quick").strip().lower()
    if depth not in {"quick", "deep"}:
        depth = "quick"

    tenant_id = parse_optional_int(request.query_params.get("tenant_id"))
    portfolio_id = parse_optional_int(request.query_params.get("portfolio_id"))

    resolved_symbol, resolve_note = resolve_symbol_input(raw_symbol)
    if not resolved_symbol:
        return redirect_dashboard(
            tenant_id=tenant_id,
            portfolio_id=portfolio_id,
            extras={
                "analyze_error": "Symbol/company not found. Try a ticker like AAPL or JNPR.",
                "analyze_input": raw_symbol,
            },
        )

    analysis = build_stock_analysis(resolved_symbol)
    quick = analysis.get("quick", {})
    all_na = bool(quick) and all(str(value) == "N/A" for value in quick.values())
    if all_na:
        analysis["error"] = "Limited market data returned right now. You can still review and try again shortly."

    if resolve_note:
        analysis["error"] = ""
    page = render_analysis_page(analysis, depth=depth, tenant_id=tenant_id, portfolio_id=portfolio_id)
    if resolve_note:
        page = page.replace(
            "</section>",
            f"<div class='flash' style='background:#e8fff1;border-color:#baf0cc;color:#0f5e2a;'>{html.escape(resolve_note)}</div></section>",
            1,
        )
    return HTMLResponse(page)


async def tenant_add(request):
    form = await parse_form(request)
    ok, message, tenant_id, portfolio_id = create_tenant(form.get("tenant_name", ""))
    if not ok and tenant_id is None:
        return redirect_with_message(f"Owner setup failed: {message}")
    status_message = message if ok else f"Owner setup note: {message}"
    return redirect_with_message(status_message, tenant_id=tenant_id, portfolio_id=portfolio_id)


async def owner_delete(request):
    form = await parse_form(request)
    owner_id = parse_optional_int(form.get("tenant_id"))
    current_portfolio_id = parse_optional_int(form.get("portfolio_id"))

    if owner_id is None:
        return redirect_with_message("Owner delete failed: owner is required")

    ok, message, next_owner_id, next_portfolio_id = delete_owner(owner_id)
    if not ok:
        if next_owner_id is None:
            return redirect_with_message(f"Owner delete failed: {message}", tenant_id=owner_id, portfolio_id=current_portfolio_id)
        return redirect_with_message(f"Owner delete failed: {message}", tenant_id=next_owner_id, portfolio_id=next_portfolio_id)

    return redirect_with_message(message, tenant_id=next_owner_id, portfolio_id=next_portfolio_id)


async def portfolio_add(request):
    form = await parse_form(request)
    tenant_id = parse_optional_int(form.get("tenant_id"))
    if tenant_id is None:
        return redirect_with_message("Portfolio setup failed: tenant is required")

    ok, message, portfolio_id = create_portfolio(tenant_id, form.get("portfolio_name", ""))
    if not ok and portfolio_id is None:
        return redirect_with_message(f"Portfolio setup failed: {message}", tenant_id=tenant_id)
    status_message = message if ok else f"Portfolio setup note: {message}"
    return redirect_with_message(status_message, tenant_id=tenant_id, portfolio_id=portfolio_id)


async def cash_add(request):
    form = await parse_form(request)
    tenant_id = parse_optional_int(form.get("tenant_id"))
    portfolio_id = parse_optional_int(form.get("portfolio_id"))
    entry_date = form.get("entry_date") or date.today().isoformat()
    note = form.get("note", "")

    if portfolio_id is None:
        return redirect_with_message("Funding failed: portfolio is required", tenant_id=tenant_id)

    try:
        amount = parse_positive_decimal(form.get("amount", ""), MONEY_QUANT)
        add_cash_entry(portfolio_id, amount, entry_date, note)
    except ValueError as error:
        return redirect_with_message(f"Funding failed: {error}", tenant_id=tenant_id, portfolio_id=portfolio_id)

    return redirect_with_message("Funding added", tenant_id=tenant_id, portfolio_id=portfolio_id)


async def trade_add(request):
    form = await parse_form(request)
    tenant_id = parse_optional_int(form.get("tenant_id"))
    portfolio_id = parse_optional_int(form.get("portfolio_id"))
    trade_date = form.get("trade_date") or date.today().isoformat()
    symbol = form.get("symbol", "")
    side = form.get("side", "buy").lower()

    if portfolio_id is None:
        return redirect_with_message("Trade rejected: portfolio is required", tenant_id=tenant_id)

    try:
        quantity = parse_positive_decimal(form.get("quantity", ""), SHARE_QUANT)
        price = parse_positive_decimal(form.get("price", ""), MONEY_QUANT)
    except ValueError as error:
        return redirect_with_message(f"Trade rejected: {error}", tenant_id=tenant_id, portfolio_id=portfolio_id)

    ok, message = add_trade(
        portfolio_id=portfolio_id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        trade_date=trade_date,
    )
    if not ok:
        return redirect_with_message(f"Trade rejected: {message}", tenant_id=tenant_id, portfolio_id=portfolio_id)

    return redirect_with_message(f"{side.upper()} order recorded", tenant_id=tenant_id, portfolio_id=portfolio_id)


async def snapshot_save(request):
    form = await parse_form(request)
    tenant_id = parse_optional_int(form.get("tenant_id"))
    portfolio_id = parse_optional_int(form.get("portfolio_id"))

    try:
        snapshot_name = create_db_snapshot("manual")
    except Exception as error:
        return redirect_with_message(f"Snapshot save failed: {error}", tenant_id=tenant_id, portfolio_id=portfolio_id)

    return redirect_with_message(f"Snapshot saved: {snapshot_name}", tenant_id=tenant_id, portfolio_id=portfolio_id)


async def snapshot_load(request):
    form = await parse_form(request)
    tenant_id = parse_optional_int(form.get("tenant_id"))
    portfolio_id = parse_optional_int(form.get("portfolio_id"))
    snapshot_name = form.get("snapshot_name", "").strip()

    if not snapshot_name:
        return redirect_with_message("Snapshot load failed: choose a snapshot first", tenant_id=tenant_id, portfolio_id=portfolio_id)

    ok, message, next_tenant_id, next_portfolio_id = restore_db_snapshot(snapshot_name)
    if not ok:
        return redirect_with_message(message, tenant_id=tenant_id, portfolio_id=portfolio_id)

    return redirect_with_message(message, tenant_id=next_tenant_id, portfolio_id=next_portfolio_id)


async def defaults_restore(request):
    form = await parse_form(request)
    tenant_id = parse_optional_int(form.get("tenant_id"))
    portfolio_id = parse_optional_int(form.get("portfolio_id"))

    try:
        ok, message, next_tenant_id, next_portfolio_id = restore_default_state()
    except Exception as error:
        return redirect_with_message(f"Restore defaults failed: {error}", tenant_id=tenant_id, portfolio_id=portfolio_id)

    if not ok:
        return redirect_with_message(message, tenant_id=tenant_id, portfolio_id=portfolio_id)

    return redirect_with_message(message, tenant_id=next_tenant_id, portfolio_id=next_portfolio_id)


async def portfolio_zeroize(request):
    form = await parse_form(request)
    tenant_id = parse_optional_int(form.get("tenant_id"))
    portfolio_id = parse_optional_int(form.get("portfolio_id"))

    if portfolio_id is None:
        return redirect_with_message("Zeroize failed: portfolio is required", tenant_id=tenant_id)

    try:
        ok, message = zeroize_portfolio(portfolio_id)
    except Exception as error:
        return redirect_with_message(f"Zeroize failed: {error}", tenant_id=tenant_id, portfolio_id=portfolio_id)

    if not ok:
        return redirect_with_message(f"Zeroize failed: {message}", tenant_id=tenant_id, portfolio_id=portfolio_id)

    return redirect_with_message(message, tenant_id=tenant_id, portfolio_id=portfolio_id)


async def api_current_quote(request):
    symbol = request.query_params.get("symbol", "").upper().strip()
    if not symbol:
        return JSONResponse({"ok": False, "error": "symbol is required"}, status_code=400)

    quote_map = fetch_quotes([symbol])
    price = quote_map.get(symbol)
    if price is None:
        return JSONResponse({"ok": False, "error": "No current quote found"}, status_code=404)

    return JSONResponse({"ok": True, "symbol": symbol, "price": float(price)})


async def api_historical_quote(request):
    symbol = request.query_params.get("symbol", "").upper().strip()
    trade_day = request.query_params.get("date", "")
    if not symbol or not trade_day:
        return JSONResponse({"ok": False, "error": "symbol and date are required"}, status_code=400)

    price = fetch_historical_close(symbol, trade_day)
    if price is None:
        return JSONResponse(
            {
                "ok": False,
                "error": "No historical close found for that symbol/date (market may have been closed)",
            },
            status_code=404,
        )

    return JSONResponse({"ok": True, "symbol": symbol, "date": trade_day, "price": float(price)})


migrate_legacy_vpm_storage()
init_db()
remove_legacy_default_owner_maya()

app = Starlette(
    debug=False,
    routes=[
        Route(ROOT_PATH, home_page, methods=["GET"]),
        Route(ASSET_THEME_PATH, coreportal_theme_css, methods=["GET"]),
        Route(OPEN_APP_PATH, open_app, methods=["GET"]),
        Route(VPM_PATH, dashboard, methods=["GET"]),
        Route(TRACKER_PATH, tracker_dashboard, methods=["GET"]),
        Route(f"{TRACKER_PATH}/account/add", tracker_account_add, methods=["POST"]),
        Route(f"{TRACKER_PATH}/deposit", tracker_deposit, methods=["POST"]),
        Route(f"{TRACKER_PATH}/salary/add", tracker_salary_add, methods=["POST"]),
        Route(f"{TRACKER_PATH}/spending/add", tracker_spending_add, methods=["POST"]),
        Route(f"{TRACKER_PATH}/entry/add", tracker_entry_add, methods=["POST"]),
        Route(f"{TRACKER_PATH}/transfer-to-vpm", tracker_transfer_to_vpm, methods=["POST"]),
        Route(f"{TRACKER_PATH}/zeroize", tracker_zeroize, methods=["POST"]),
        Route(OTD_PATH, otd_tool, methods=["GET"]),
        Route(f"{OTD_PATH}/policy_years.json", otd_policy_years, methods=["GET"]),
        Route(with_base_path("/otd/policy_years.json"), otd_policy_years, methods=["GET"]),
        Route(CVP_PATH, cvp_tool, methods=["GET"]),
        Route(f"{CVP_PATH}/policy_years.json", cvp_policy_years, methods=["GET"]),
        Route(with_base_path("/portfolio"), legacy_vpm_redirect, methods=["GET"]),
        Route(with_base_path("/otd"), legacy_otd_redirect, methods=["GET"]),
        Route(with_base_path("/TRACKER"), legacy_tracker_redirect, methods=["GET"]),
        Route(with_base_path("/tracker"), legacy_tracker_redirect, methods=["GET"]),
        Route(with_base_path("/analyze"), analyze_stock, methods=["GET"]),
        Route(with_base_path("/snapshot/save"), snapshot_save, methods=["POST"]),
        Route(with_base_path("/snapshot/load"), snapshot_load, methods=["POST"]),
        Route(with_base_path("/defaults/restore"), defaults_restore, methods=["POST"]),
        Route(f"{VPM_PATH}/zeroize", portfolio_zeroize, methods=["POST"]),
        Route(with_base_path("/portfolio/zeroize"), portfolio_zeroize, methods=["POST"]),
        Route(with_base_path("/tenant/add"), tenant_add, methods=["POST"]),
        Route(with_base_path("/owner/delete"), owner_delete, methods=["POST"]),
        Route(f"{VPM_PATH}/add", portfolio_add, methods=["POST"]),
        Route(with_base_path("/portfolio/add"), portfolio_add, methods=["POST"]),
        Route(with_base_path("/cash/add"), cash_add, methods=["POST"]),
        Route(with_base_path("/trade/add"), trade_add, methods=["POST"]),
        Route(with_base_path("/api/quote/current"), api_current_quote, methods=["GET"]),
        Route(with_base_path("/api/quote/historical"), api_historical_quote, methods=["GET"]),
    ],
)


if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description=APP_TITLE)
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8081")), help="Bind port (default: PORT env or 8081)")
    args = parser.parse_args()

    uvicorn.run("coreportal:app", host=args.host, port=args.port, reload=False)
