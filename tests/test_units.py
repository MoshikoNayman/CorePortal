"""Unit tests for the pure-ish helper layers (no network)."""

from __future__ import annotations

import time
import unittest
from decimal import Decimal

import coreportal_core as cc


class FormattingTests(unittest.TestCase):
    def test_to_decimal_quantizes(self):
        self.assertEqual(cc.db.to_decimal("12.345", Decimal("0.01")), Decimal("12.34"))

    def test_parse_positive_decimal_rejects_nonpositive(self):
        with self.assertRaises(ValueError):
            cc.db.parse_positive_decimal("0")
        with self.assertRaises(ValueError):
            cc.db.parse_positive_decimal("-5")
        self.assertEqual(cc.db.parse_positive_decimal("3.5"), Decimal("3.5"))

    def test_parse_optional_int(self):
        self.assertIsNone(cc.db.parse_optional_int(""))
        self.assertIsNone(cc.db.parse_optional_int(None))
        self.assertIsNone(cc.db.parse_optional_int("abc"))
        self.assertEqual(cc.db.parse_optional_int("7"), 7)

    def test_format_money(self):
        self.assertEqual(cc.services.format_money(Decimal("1234.5")), "$1,234.50")

    def test_shift_months_wraps_year(self):
        from datetime import date
        self.assertEqual(cc.db.shift_months(date(2026, 11, 15), 3), date(2027, 2, 15))
        # clamps day to month length
        self.assertEqual(cc.db.shift_months(date(2026, 1, 31), 1), date(2026, 2, 28))


class BasePathTests(unittest.TestCase):
    def test_normalize_base_path(self):
        self.assertEqual(cc.config.normalize_base_path(""), "")
        self.assertEqual(cc.config.normalize_base_path("/"), "")
        self.assertEqual(cc.config.normalize_base_path("coreportal"), "/coreportal")
        self.assertEqual(cc.config.normalize_base_path("/coreportal/"), "/coreportal")


class DatabaseHardeningTests(unittest.TestCase):
    def test_connection_uses_wal_and_timeout(self):
        with cc.db.db_connection() as conn:
            self.assertEqual(
                conn.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal"
            )
            self.assertGreater(conn.execute("PRAGMA busy_timeout").fetchone()[0], 0)
            self.assertEqual(conn.execute("PRAGMA foreign_keys").fetchone()[0], 1)

    def test_concurrent_writes_do_not_lock(self):
        import sqlite3
        import threading

        errors = []

        def writer(tag):
            try:
                for i in range(15):
                    with cc.db.db_connection() as conn:
                        conn.execute(
                            "INSERT INTO cash_ledger (portfolio_id, amount, entry_date, note)"
                            " VALUES (?,?,?,?)",
                            (1, "1.00", "2026-01-01", f"unittest-conc-{tag}-{i}"),
                        )
            except sqlite3.Error as exc:  # pragma: no cover - failure path
                errors.append(str(exc))

        threads = [threading.Thread(target=writer, args=(n,)) for n in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # Always clean up the rows this test inserted.
        with cc.db.db_connection() as conn:
            conn.execute("DELETE FROM cash_ledger WHERE note LIKE 'unittest-conc-%'")
        self.assertEqual(errors, [])


class QuoteCacheTests(unittest.TestCase):
    def setUp(self):
        # Work on the live source module's cache dict and TTL.
        self.src = cc.config.HTTP  # touch to ensure import
        import coreportal as monolith
        self.m = monolith
        self.m._quote_cache.clear()

    def tearDown(self):
        self.m._quote_cache.clear()

    def test_cache_put_get_roundtrip(self):
        self.m._quote_cache_put("AAPL", Decimal("100.00"))
        self.assertEqual(self.m._quote_cache_get("AAPL"), Decimal("100.00"))

    def test_cache_expires(self):
        old_ttl = self.m.QUOTE_CACHE_TTL
        self.m.QUOTE_CACHE_TTL = 0.2
        try:
            self.m._quote_cache_put("MSFT", Decimal("400.00"))
            time.sleep(0.3)
            self.assertIsNone(self.m._quote_cache_get("MSFT"))
        finally:
            self.m.QUOTE_CACHE_TTL = old_ttl

    def test_fetch_quotes_serves_cache_without_network(self):
        # Pre-seed the cache and make HTTP.get explode: a cache hit must not
        # touch the network.
        self.m._quote_cache_put("GOOG", Decimal("150.00"))
        original = self.m.HTTP

        class Boom:
            def get(self, *a, **k):
                raise AssertionError("network must not be called for cached symbol")

        self.m.HTTP = Boom()
        try:
            result = self.m.fetch_quotes(["GOOG"])
            self.assertEqual(result, {"GOOG": Decimal("150.00")})
        finally:
            self.m.HTTP = original


class DeployCompatibilityTests(unittest.TestCase):
    """Guards against syntax that parses on a newer local Python but breaks on
    the deploy target (Raspberry Pi runs Python 3.11). Notably, backslashes
    inside f-string expression parts are only legal in 3.12+.
    """

    def _project_py_files(self):
        import os
        root = os.path.dirname(os.path.dirname(__file__))
        for sub in ("coreportal.py", "coreportal_core", "tests", "scripts"):
            path = os.path.join(root, sub)
            if os.path.isfile(path):
                yield path
            elif os.path.isdir(path):
                for dirpath, _dirs, files in os.walk(path):
                    for f in files:
                        if f.endswith(".py"):
                            yield os.path.join(dirpath, f)

    def test_no_backslash_in_fstring_expression(self):
        # Tokenize each file and flag f-strings whose {expression} contains '\'.
        import io
        import re
        import tokenize

        offenders = []
        for path in self._project_py_files():
            with open(path, encoding="utf-8") as fh:
                src = fh.read()
            try:
                tokens = tokenize.generate_tokens(io.StringIO(src).readline)
                for tok in tokens:
                    if tok.type == tokenize.STRING and re.match(r"^[a-zA-Z]*f", tok.string, re.I):
                        for part in re.findall(r"\{[^{}]*\}", tok.string):
                            if "\\" in part:
                                offenders.append(f"{path}:{tok.start[0]} {part[:50]}")
            except tokenize.TokenError:
                pass
        self.assertEqual(offenders, [], "backslash in f-string expression (breaks Python 3.11):\n" + "\n".join(offenders))


class PackageStructureTests(unittest.TestCase):
    def test_layers_share_single_app(self):
        import coreportal as monolith
        self.assertIs(cc.app, monolith.app)

    def test_all_reexports_resolve(self):
        import importlib
        for name in ["config", "db", "quotes", "services", "views", "routes"]:
            module = importlib.import_module(f"coreportal_core.{name}")
            for symbol in getattr(module, "__all__", []):
                with self.subTest(module=name, symbol=symbol):
                    self.assertTrue(hasattr(module, symbol))


if __name__ == "__main__":
    unittest.main()
