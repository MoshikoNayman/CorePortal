"""Configuration, paths, and shared runtime primitives.

This is the lowest layer: application constants, filesystem path resolution,
base-path helpers for reverse-proxy deployments, the shared HTTP session (with
default timeout), logging, and the in-memory current-quote cache.
"""

from __future__ import annotations

from ._source import source

# Identity / display
APP_TITLE = source.APP_TITLE
APP_AUTHOR = source.APP_AUTHOR
APP_HOME_TITLE = source.APP_HOME_TITLE
APP_COPYRIGHT = source.APP_COPYRIGHT
COMMON_PAGE_MAX_WIDTH = source.COMMON_PAGE_MAX_WIDTH

# Base-path / routing prefixes
normalize_base_path = source.normalize_base_path
with_base_path = source.with_base_path
BASE_PATH = source.BASE_PATH
ROOT_PATH = source.ROOT_PATH
ASSET_THEME_PATH = source.ASSET_THEME_PATH
OPEN_APP_PATH = source.OPEN_APP_PATH
VPM_PATH = source.VPM_PATH
CVP_PATH = source.CVP_PATH
OTD_PATH = source.OTD_PATH
TRACKER_PATH = source.TRACKER_PATH

# Domain constants
DEFAULT_PORTFOLIO_NAME = source.DEFAULT_PORTFOLIO_NAME
DEFAULT_TENANTS = source.DEFAULT_TENANTS
MONEY_QUANT = source.MONEY_QUANT
SHARE_QUANT = source.SHARE_QUANT
ANALYSIS_MODULES = source.ANALYSIS_MODULES
ALPHAVANTAGE_API_KEY = source.ALPHAVANTAGE_API_KEY

# Filesystem locations
VPM_DIR = source.VPM_DIR
DB_PATH = source.DB_PATH
BACKUP_DIR = source.BACKUP_DIR
OTD_HTML_PATH = source.OTD_HTML_PATH
OTD_POLICY_PATH = source.OTD_POLICY_PATH
CVP_HTML_PATH = source.CVP_HTML_PATH
CVP_POLICY_PATH = source.CVP_POLICY_PATH
THEME_CSS_PATH = source.THEME_CSS_PATH

# Outbound HTTP + logging
HTTP = source.HTTP
HTTP_TIMEOUT = source.HTTP_TIMEOUT
logger = source.logger

# Database + request limits
DB_TIMEOUT = source.DB_TIMEOUT
MAX_FORM_BYTES = source.MAX_FORM_BYTES

# Current-quote cache
QUOTE_CACHE_TTL = source.QUOTE_CACHE_TTL

# Authentication
AUTH_ENABLED = source.AUTH_ENABLED
SESSION_MAX_AGE = source.SESSION_MAX_AGE
SESSION_COOKIE = source.SESSION_COOKIE

__all__ = [
    "APP_TITLE", "APP_AUTHOR", "APP_HOME_TITLE", "APP_COPYRIGHT", "COMMON_PAGE_MAX_WIDTH",
    "normalize_base_path", "with_base_path", "BASE_PATH", "ROOT_PATH", "ASSET_THEME_PATH",
    "OPEN_APP_PATH", "VPM_PATH", "CVP_PATH", "OTD_PATH", "TRACKER_PATH",
    "DEFAULT_PORTFOLIO_NAME", "DEFAULT_TENANTS", "MONEY_QUANT", "SHARE_QUANT",
    "ANALYSIS_MODULES", "ALPHAVANTAGE_API_KEY",
    "VPM_DIR", "DB_PATH", "BACKUP_DIR", "OTD_HTML_PATH", "OTD_POLICY_PATH",
    "CVP_HTML_PATH", "CVP_POLICY_PATH", "THEME_CSS_PATH",
    "HTTP", "HTTP_TIMEOUT", "logger", "DB_TIMEOUT", "MAX_FORM_BYTES", "QUOTE_CACHE_TTL",
    "AUTH_ENABLED", "SESSION_MAX_AGE", "SESSION_COOKIE",
]
