"""CorePortal - a layered view of the application.

CorePortal is a personal finance & portfolio web app (Starlette + Uvicorn,
SQLite-backed) bundling several tools behind one launcher:

* **VPM** - Virtual Portfolio Manager (paper trading, live quotes, analysis)
* **BAT** - Bank Account Tracker (ledger, cashflow, net-worth, transfers)
* **OTD** - Out-the-Door vehicle pricing estimator
* **CVP** - Car Value / buy-sell TCO planner

The deployable entrypoint is the top-level ``coreportal.py``. This package
re-exposes that code as a clean, documented set of layers so the structure is
easy to navigate and import from:

    coreportal_core.config     constants, paths, HTTP session, logging, cache
    coreportal_core.db         SQLite connection, schema, snapshots, CRUD
    coreportal_core.quotes     market data (quotes/charts/news) + formatting
    coreportal_core.services   orchestration bridging db + quotes
    coreportal_core.views      HTML rendering + shared stylesheet
    coreportal_core.routes     request handlers, middleware, the ASGI ``app``

Dependency direction is strictly downward:
config → db → quotes → services → views → routes.

Typical use::

    from coreportal_core import app          # the ASGI application
    from coreportal_core import db, quotes    # layer modules
"""

from __future__ import annotations

from . import config, db, quotes, services, views, routes
from .routes import app

__version__ = "2.1.0"

__all__ = ["app", "config", "db", "quotes", "services", "views", "routes", "__version__"]
