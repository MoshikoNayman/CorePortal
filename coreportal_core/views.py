"""Presentation layer: HTML rendering and the shared stylesheet.

Pure functions that take already-computed data and return HTML strings. The
shared compact stylesheet (``shared_theme_css``) is the single source of truth
for layout/component styling; per-page renderers add only page-specific rules.
"""

from __future__ import annotations

from ._source import source

shared_theme_css = source.shared_theme_css
render_home_page = source.render_home_page
render_dashboard = source.render_dashboard
render_analysis_page = source.render_analysis_page
render_tracker_page = source.render_tracker_page

__all__ = [
    "shared_theme_css",
    "render_home_page",
    "render_dashboard",
    "render_analysis_page",
    "render_tracker_page",
]
