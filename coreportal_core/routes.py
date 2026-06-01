"""HTTP layer: request handlers, middleware, and the Starlette app.

Exposes the ASGI ``app`` (the same instance the entrypoint serves), every
route handler, the security-headers middleware, and the error handlers.
"""

from __future__ import annotations

from ._source import source

# ASGI application
app = source.app

# Page handlers
home_page = source.home_page
dashboard = source.dashboard
tracker_dashboard = source.tracker_dashboard
analyze_stock = source.analyze_stock
open_app = source.open_app
otd_tool = source.otd_tool
cvp_tool = source.cvp_tool
coreportal_theme_css = source.coreportal_theme_css
otd_policy_years = source.otd_policy_years
cvp_policy_years = source.cvp_policy_years

# Legacy redirects
legacy_vpm_redirect = source.legacy_vpm_redirect
legacy_otd_redirect = source.legacy_otd_redirect
legacy_tracker_redirect = source.legacy_tracker_redirect

# Mutations (POST)
tracker_account_add = source.tracker_account_add
tracker_entry_add = source.tracker_entry_add
tracker_deposit = source.tracker_deposit
tracker_salary_add = source.tracker_salary_add
tracker_spending_add = source.tracker_spending_add
tracker_transfer_to_vpm = source.tracker_transfer_to_vpm
tracker_zeroize = source.tracker_zeroize
tracker_entry_delete = source.tracker_entry_delete
tenant_add = source.tenant_add
owner_delete = source.owner_delete
portfolio_add = source.portfolio_add
cash_add = source.cash_add
cash_delete = source.cash_delete
trade_add = source.trade_add
trade_delete = source.trade_delete
snapshot_save = source.snapshot_save
snapshot_load = source.snapshot_load
defaults_restore = source.defaults_restore
portfolio_zeroize = source.portfolio_zeroize

# JSON / ops endpoints
api_current_quote = source.api_current_quote
api_historical_quote = source.api_historical_quote
health_check = source.health_check

# Authentication
login = source.login
logout = source.logout
render_login_page = source.render_login_page

# Middleware + error handlers
SecurityHeadersMiddleware = source.SecurityHeadersMiddleware
AuthMiddleware = source.AuthMiddleware
on_internal_error = source.on_internal_error
on_not_found = source.on_not_found
on_form_too_large = source.on_form_too_large
FormTooLarge = source.FormTooLarge

__all__ = [
    "app",
    "home_page", "dashboard", "tracker_dashboard", "analyze_stock", "open_app",
    "otd_tool", "cvp_tool", "coreportal_theme_css", "otd_policy_years", "cvp_policy_years",
    "legacy_vpm_redirect", "legacy_otd_redirect", "legacy_tracker_redirect",
    "tracker_account_add", "tracker_entry_add", "tracker_deposit", "tracker_salary_add",
    "tracker_spending_add", "tracker_transfer_to_vpm", "tracker_zeroize", "tracker_entry_delete",
    "tenant_add", "owner_delete", "portfolio_add", "cash_add", "cash_delete",
    "trade_add", "trade_delete",
    "snapshot_save", "snapshot_load", "defaults_restore", "portfolio_zeroize",
    "api_current_quote", "api_historical_quote", "health_check",
    "login", "logout", "render_login_page",
    "SecurityHeadersMiddleware", "AuthMiddleware", "on_internal_error", "on_not_found",
    "on_form_too_large", "FormTooLarge",
]
