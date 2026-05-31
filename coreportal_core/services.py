"""Service layer: orchestration that bridges the database and market data.

Functions here combine persisted state (positions, balances, ledgers) with
live quotes - e.g. valuing open positions, moving funds between the bank
tracker and a portfolio, and assembling the owner finance snapshot used by the
tracker dashboard. Also includes money-formatting helpers and the app registry
process management used by the home launcher.
"""

from __future__ import annotations

from ._source import source

# Portfolio valuation / transfers
build_positions = source.build_positions
create_bank_account = source.create_bank_account  # re-exported convenience
transfer_bank_to_vpm = source.transfer_bank_to_vpm

# Money formatting
format_money = source.format_money
format_percent = source.format_percent
format_shares = source.format_shares

# Query-string + selection helpers
build_query_string = source.build_query_string
build_tracker_query_string = source.build_tracker_query_string
resolve_tracker_selection = source.resolve_tracker_selection
load_owner_finance_snapshot = source.load_owner_finance_snapshot

# App launcher registry / process management
get_app_by_id = source.get_app_by_id
is_http_ready = source.is_http_ready
start_process_app = source.start_process_app
app_status = source.app_status

__all__ = [
    "build_positions", "create_bank_account", "transfer_bank_to_vpm",
    "format_money", "format_percent", "format_shares",
    "build_query_string", "build_tracker_query_string", "resolve_tracker_selection",
    "load_owner_finance_snapshot",
    "get_app_by_id", "is_http_ready", "start_process_app", "app_status",
]
