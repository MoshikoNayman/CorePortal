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
