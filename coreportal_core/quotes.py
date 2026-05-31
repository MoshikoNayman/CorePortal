"""Market-data layer: quotes, charts, news, and stock analysis.

Talks to external providers (Yahoo Finance, stooq, optional Alpha Vantage)
through the shared timed HTTP session, with a short-TTL current-quote cache.
Also holds the numeric/series formatting helpers used to present that data.
This layer depends on config but not on the database.
"""

from __future__ import annotations

from ._source import source

# Current-quote cache primitives
fetch_quotes = source.fetch_quotes

# stooq / Yahoo / Alpha Vantage fetchers
stooq_symbol = source.stooq_symbol
fetch_current_quote_stooq = source.fetch_current_quote_stooq
fetch_historical_close_stooq = source.fetch_historical_close_stooq
fetch_chart_series_stooq = source.fetch_chart_series_stooq
fetch_historical_close = source.fetch_historical_close
fetch_quote_summary = source.fetch_quote_summary
fetch_quote_snapshot = source.fetch_quote_snapshot
fetch_alpha_overview = source.fetch_alpha_overview
fetch_alpha_quote = source.fetch_alpha_quote
fetch_chart = source.fetch_chart
fetch_recent_news = source.fetch_recent_news

# Parsing / math / formatting helpers
unwrap_value = source.unwrap_value
nested_get = source.nested_get
as_float = source.as_float
format_compact_number = source.format_compact_number
format_plain_number = source.format_plain_number
format_percent_float = source.format_percent_float
format_currency = source.format_currency
compute_return_pct = source.compute_return_pct
sma = source.sma
extract_series_from_chart = source.extract_series_from_chart
previous_price_by_days = source.previous_price_by_days
format_series_labels = source.format_series_labels

# High-level analysis
resolve_symbol_input = source.resolve_symbol_input
build_stock_analysis = source.build_stock_analysis

__all__ = [
    "fetch_quotes", "stooq_symbol", "fetch_current_quote_stooq",
    "fetch_historical_close_stooq", "fetch_chart_series_stooq", "fetch_historical_close",
    "fetch_quote_summary", "fetch_quote_snapshot", "fetch_alpha_overview",
    "fetch_alpha_quote", "fetch_chart", "fetch_recent_news",
    "unwrap_value", "nested_get", "as_float", "format_compact_number",
    "format_plain_number", "format_percent_float", "format_currency",
    "compute_return_pct", "sma", "extract_series_from_chart",
    "previous_price_by_days", "format_series_labels",
    "resolve_symbol_input", "build_stock_analysis",
]
