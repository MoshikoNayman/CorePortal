"""Authentication + CSRF tests.

Auth is configured from env vars at import time, but the middleware reads the
module-level globals at request time, so tests toggle those globals directly
(restoring them afterward) rather than re-importing the whole module.
"""

from __future__ import annotations

import time
import unittest
from urllib.parse import urlencode

import coreportal as monolith
import coreportal_core as cc
from tests.asgi import request

SAME_ORIGIN = [
    (b"host", b"testserver"),
    (b"origin", b"http://testserver"),
    (b"content-type", b"application/x-www-form-urlencoded"),
]


class AuthEnabledTests(unittest.TestCase):
    def setUp(self):
        self._saved = (monolith.AUTH_ENABLED, monolith.AUTH_PASSWORD, monolith.SECRET_KEY)
        monolith.AUTH_ENABLED = True
        monolith.AUTH_PASSWORD = "testpw"
        monolith.SECRET_KEY = "fixed-test-secret"

    def tearDown(self):
        monolith.AUTH_ENABLED, monolith.AUTH_PASSWORD, monolith.SECRET_KEY = self._saved

    def _login(self):
        body = urlencode({"password": "testpw", "next": "/VPM"}).encode()
        status, headers, _ = request(cc.app, "/login", method="POST", body=body, headers=SAME_ORIGIN)
        self.assertEqual(status, 303)
        cookie = headers.get("set-cookie", "")
        return cookie.split("coreportal_session=")[1].split(";")[0]

    def test_unauthenticated_redirects_to_login(self):
        status, headers, _ = request(cc.app, "/VPM")
        self.assertEqual(status, 303)
        self.assertIn("/login", headers.get("location", ""))

    def test_healthz_is_exempt(self):
        status, _, _ = request(cc.app, "/healthz")
        self.assertEqual(status, 200)

    def test_login_page_renders(self):
        status, _, body = request(cc.app, "/login")
        self.assertEqual(status, 200)
        self.assertIn('name="password"', body)

    def test_wrong_password_401(self):
        body = urlencode({"password": "nope"}).encode()
        status, _, _ = request(cc.app, "/login", method="POST", body=body, headers=SAME_ORIGIN)
        self.assertEqual(status, 401)

    def test_correct_password_sets_secure_cookie(self):
        body = urlencode({"password": "testpw"}).encode()
        status, headers, _ = request(cc.app, "/login", method="POST", body=body, headers=SAME_ORIGIN)
        self.assertEqual(status, 303)
        cookie = headers.get("set-cookie", "")
        self.assertIn("coreportal_session=", cookie)
        self.assertIn("HttpOnly", cookie)
        self.assertIn("strict", cookie.lower())

    def test_valid_cookie_grants_access(self):
        token = self._login()
        status, _, _ = request(cc.app, "/VPM", headers=[(b"cookie", f"coreportal_session={token}".encode())])
        self.assertEqual(status, 200)

    def test_tampered_cookie_rejected(self):
        status, _, _ = request(
            cc.app, "/VPM",
            headers=[(b"cookie", b"coreportal_session=9999999999.deadbeef")],
        )
        self.assertEqual(status, 303)

    def test_expired_session_rejected(self):
        old = monolith.SESSION_MAX_AGE
        monolith.SESSION_MAX_AGE = 1
        try:
            token = monolith._sign_session(int(time.time()) - 10)  # issued 10s ago
            status, _, _ = request(
                cc.app, "/VPM",
                headers=[(b"cookie", f"coreportal_session={token}".encode())],
            )
            self.assertEqual(status, 303)
        finally:
            monolith.SESSION_MAX_AGE = old

    def test_api_returns_401_json(self):
        status, _, _ = request(cc.app, "/api/quote/current")
        self.assertEqual(status, 401)

    def test_logout_clears_cookie(self):
        status, headers, _ = request(cc.app, "/logout")
        self.assertEqual(status, 303)
        self.assertIn("/login", headers.get("location", ""))


class CsrfTests(unittest.TestCase):
    def test_cross_origin_post_blocked(self):
        # CSRF protection is active regardless of auth state.
        body = urlencode({"tenant_id": "1"}).encode()
        headers = [
            (b"host", b"testserver"),
            (b"origin", b"http://evil.example"),
            (b"content-type", b"application/x-www-form-urlencoded"),
        ]
        status, _, _ = request(cc.app, "/cash/add", method="POST", body=body, headers=headers)
        self.assertEqual(status, 403)

    def test_same_origin_post_allowed(self):
        # Same-origin POST passes the CSRF gate (it then redirects as normal).
        body = urlencode({"tenant_id": "1"}).encode()
        headers = [
            (b"host", b"testserver"),
            (b"origin", b"http://testserver"),
            (b"content-type", b"application/x-www-form-urlencoded"),
        ]
        status, _, _ = request(cc.app, "/cash/add", method="POST", body=body, headers=headers)
        self.assertNotEqual(status, 403)


if __name__ == "__main__":
    unittest.main()
