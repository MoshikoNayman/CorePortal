"""Route-level smoke tests: every page renders and key invariants hold.

These drive the real ASGI app in-process (no sockets, no network) and use the
SQLite DB that ships with the repo. They assert status codes and a few
structural facts that have regressed before (notably the /analyze page).
"""

from __future__ import annotations

import json
import unittest

import coreportal_core as cc
from tests.asgi import request


class RouteSmokeTests(unittest.TestCase):
    def test_home_ok(self):
        status, _, body = request(cc.app, "/")
        self.assertEqual(status, 200)
        self.assertIn("CorePortal", body)

    def test_core_pages_ok(self):
        for path in ["/VPM", "/BAT", "/OTD", "/CVP", "/assets/coreportal_theme.css"]:
            with self.subTest(path=path):
                status, _, _ = request(cc.app, path)
                self.assertEqual(status, 200)

    def test_healthz_reports_ok(self):
        status, _, body = request(cc.app, "/healthz")
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["database"], "ok")
        self.assertIn("uptime_seconds", payload)

    def test_nwd_alias_redirects_to_bat(self):
        # /NWD is a documented compatibility alias: it 308-redirects to /BAT.
        status, headers, _ = request(cc.app, "/NWD")
        self.assertEqual(status, 308)
        self.assertTrue(headers.get("location", "").endswith("/BAT"))

    def test_analyze_quick_renders_chart(self):
        # Regression guard: this page used to 500 due to corrupted markup.
        status, _, body = request(cc.app, "/analyze?symbol=AAPL&depth=quick")
        self.assertEqual(status, 200)
        self.assertIn('id="quick-chart"', body)
        self.assertNotIn("current_account", body)  # stray tracker markup gone

    def test_analyze_deep_renders_sections(self):
        status, _, body = request(cc.app, "/analyze?symbol=MSFT&depth=deep")
        self.assertEqual(status, 200)
        self.assertIn("Performance & Trends", body)
        self.assertIn("Valuation", body)

    def test_security_headers_present(self):
        _, headers, _ = request(cc.app, "/")
        self.assertEqual(headers.get("x-content-type-options"), "nosniff")
        self.assertEqual(headers.get("x-frame-options"), "SAMEORIGIN")
        self.assertTrue(headers.get("referrer-policy"))
        self.assertTrue(headers.get("permissions-policy"))

    def test_unknown_route_404(self):
        status, _, _ = request(cc.app, "/definitely-not-a-real-page")
        self.assertEqual(status, 404)

    def test_compact_width_applied(self):
        _, _, body = request(cc.app, "/")
        self.assertIn("1080px", body)
        self.assertNotIn("1240", body)


if __name__ == "__main__":
    unittest.main()
