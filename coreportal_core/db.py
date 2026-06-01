"""Database layer: SQLite connection, schema, snapshots, and CRUD.

Pure persistence - this layer never reaches out to market-data providers.
Numeric parsing helpers (``to_decimal`` etc.) live here because every query
result is normalized through them.
"""

from __future__ import annotations

from ._source import source

# Numeric / date helpers
to_decimal = source.to_decimal
parse_positive_decimal = source.parse_positive_decimal
parse_optional_int = source.parse_optional_int
shift_months = source.shift_months

# Connection + schema
db_connection = source.db_connection
column_exists = source.column_exists
init_db = source.init_db
ensure_vpm_storage_layout = source.ensure_vpm_storage_layout
migrate_legacy_vpm_storage = source.migrate_legacy_vpm_storage
ensure_default_portfolio = source.ensure_default_portfolio
ensure_default_bank_account = source.ensure_default_bank_account

# Snapshots / backups / reset
ensure_backup_dir = source.ensure_backup_dir
list_backups = source.list_backups
create_db_snapshot = source.create_db_snapshot
restore_db_snapshot = source.restore_db_snapshot
restore_default_state = source.restore_default_state
zeroize_portfolio = source.zeroize_portfolio
zeroize_bank_account = source.zeroize_bank_account

# Tenants / portfolios / owners
load_tenants = source.load_tenants
load_portfolios = source.load_portfolios
resolve_selection = source.resolve_selection
create_tenant = source.create_tenant
create_portfolio = source.create_portfolio
delete_owner = source.delete_owner
remove_legacy_default_owner_maya = source.remove_legacy_default_owner_maya

# Cash / trades / positions inputs
get_cash_added = source.get_cash_added
get_trade_totals = source.get_trade_totals
get_cash_balance = source.get_cash_balance
get_open_quantity = source.get_open_quantity
add_cash_entry = source.add_cash_entry
add_trade = source.add_trade
delete_trade = source.delete_trade
delete_cash_entry = source.delete_cash_entry
load_cash_ledger = source.load_cash_ledger
load_trades = source.load_trades

# Bank accounts / ledger
load_bank_accounts = source.load_bank_accounts
get_bank_balance = source.get_bank_balance
load_bank_entries = source.load_bank_entries
create_bank_account = source.create_bank_account
add_bank_entry = source.add_bank_entry
delete_bank_entry = source.delete_bank_entry
add_bank_entries = source.add_bank_entries
monthly_spending_for_month = source.monthly_spending_for_month
build_monthly_spending_series = source.build_monthly_spending_series
build_networth_estimate_series = source.build_networth_estimate_series

__all__ = [
    "to_decimal", "parse_positive_decimal", "parse_optional_int", "shift_months",
    "db_connection", "column_exists", "init_db", "ensure_vpm_storage_layout",
    "migrate_legacy_vpm_storage", "ensure_default_portfolio", "ensure_default_bank_account",
    "ensure_backup_dir", "list_backups", "create_db_snapshot", "restore_db_snapshot",
    "restore_default_state", "zeroize_portfolio", "zeroize_bank_account",
    "load_tenants", "load_portfolios", "resolve_selection", "create_tenant",
    "create_portfolio", "delete_owner", "remove_legacy_default_owner_maya",
    "get_cash_added", "get_trade_totals", "get_cash_balance", "get_open_quantity",
    "add_cash_entry", "add_trade", "delete_trade", "delete_cash_entry",
    "load_cash_ledger", "load_trades", "delete_bank_entry",
    "load_bank_accounts", "get_bank_balance", "load_bank_entries", "create_bank_account",
    "add_bank_entry", "add_bank_entries", "monthly_spending_for_month",
    "build_monthly_spending_series", "build_networth_estimate_series",
]
