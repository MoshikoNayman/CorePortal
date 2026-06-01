"""Tests for line-item edit/delete (trades, cash entries, bank ledger).

These exercise the db layer directly against the shipped database, creating
clearly-marked rows and removing them again, with cleanup in tearDown so the
real data is never left modified.
"""

from __future__ import annotations

import unittest
from decimal import Decimal

import coreportal as cp


class TradeDeleteTests(unittest.TestCase):
    MARK = "lineitem-test"

    def setUp(self):
        with cp.db_connection() as c:
            row = c.execute("SELECT id FROM portfolios LIMIT 1").fetchone()
            self.portfolio_id = int(row["id"]) if row else None

    def tearDown(self):
        with cp.db_connection() as c:
            c.execute("DELETE FROM trades WHERE trade_date = ?", (self.MARK,))
            c.execute("DELETE FROM cash_ledger WHERE note = ?", (self.MARK,))

    def _insert_trade(self):
        with cp.db_connection() as c:
            cur = c.execute(
                "INSERT INTO trades (portfolio_id, symbol, side, quantity, price, trade_date)"
                " VALUES (?,?,?,?,?,?)",
                (self.portfolio_id, "TEST", "buy", "1", "1", self.MARK),
            )
            return int(cur.lastrowid)

    def test_delete_trade_removes_row(self):
        self.assertIsNotNone(self.portfolio_id)
        tid = self._insert_trade()
        self.assertTrue(cp.delete_trade(self.portfolio_id, tid))
        with cp.db_connection() as c:
            self.assertIsNone(c.execute("SELECT id FROM trades WHERE id=?", (tid,)).fetchone())

    def test_delete_trade_wrong_portfolio_is_noop(self):
        tid = self._insert_trade()
        # A different portfolio id must not delete this trade.
        self.assertFalse(cp.delete_trade(self.portfolio_id + 99999, tid))
        with cp.db_connection() as c:
            self.assertIsNotNone(c.execute("SELECT id FROM trades WHERE id=?", (tid,)).fetchone())

    def test_delete_missing_trade_returns_false(self):
        self.assertFalse(cp.delete_trade(self.portfolio_id, 99999999))

    def test_delete_cash_entry(self):
        with cp.db_connection() as c:
            cur = c.execute(
                "INSERT INTO cash_ledger (portfolio_id, amount, entry_date, note) VALUES (?,?,?,?)",
                (self.portfolio_id, "1.00", "2026-01-01", self.MARK),
            )
            eid = int(cur.lastrowid)
        self.assertTrue(cp.delete_cash_entry(self.portfolio_id, eid))
        with cp.db_connection() as c:
            self.assertIsNone(c.execute("SELECT id FROM cash_ledger WHERE id=?", (eid,)).fetchone())


class BankEntryDeleteTests(unittest.TestCase):
    MARK = "lineitem-bank-test"

    def setUp(self):
        with cp.db_connection() as c:
            row = c.execute("SELECT id FROM bank_accounts LIMIT 1").fetchone()
            self.account_id = int(row["id"]) if row else None

    def tearDown(self):
        with cp.db_connection() as c:
            c.execute("DELETE FROM bank_ledger WHERE note = ?", (self.MARK,))

    def test_delete_bank_entry(self):
        self.assertIsNotNone(self.account_id)
        with cp.db_connection() as c:
            cur = c.execute(
                "INSERT INTO bank_ledger (account_id, amount, entry_date, category, note)"
                " VALUES (?,?,?,?,?)",
                (self.account_id, "-5.00", "2026-01-01", "test", self.MARK),
            )
            eid = int(cur.lastrowid)
        self.assertTrue(cp.delete_bank_entry(self.account_id, eid))
        with cp.db_connection() as c:
            self.assertIsNone(c.execute("SELECT id FROM bank_ledger WHERE id=?", (eid,)).fetchone())

    def test_delete_bank_entry_wrong_account_is_noop(self):
        with cp.db_connection() as c:
            cur = c.execute(
                "INSERT INTO bank_ledger (account_id, amount, entry_date, category, note)"
                " VALUES (?,?,?,?,?)",
                (self.account_id, "-5.00", "2026-01-01", "test", self.MARK),
            )
            eid = int(cur.lastrowid)
        self.assertFalse(cp.delete_bank_entry(self.account_id + 99999, eid))
        with cp.db_connection() as c:
            self.assertIsNotNone(c.execute("SELECT id FROM bank_ledger WHERE id=?", (eid,)).fetchone())


class CategoryBreakdownTests(unittest.TestCase):
    MARK = "catbreak-test"

    def setUp(self):
        with cp.db_connection() as c:
            self.tenant_id = int(c.execute("SELECT id FROM tenants LIMIT 1").fetchone()["id"])
            self.account_id = int(c.execute("SELECT id FROM bank_accounts WHERE tenant_id=? LIMIT 1", (self.tenant_id,)).fetchone()["id"])

    def tearDown(self):
        with cp.db_connection() as c:
            c.execute("DELETE FROM bank_ledger WHERE note = ?", (self.MARK,))

    def test_groups_and_sorts_expenses(self):
        import datetime
        today = datetime.date.today().isoformat()
        with cp.db_connection() as c:
            for cat, amt in [("Groceries", "-220"), ("Rent", "-1500"), ("Groceries", "-60"), ("Transport", "-90")]:
                c.execute(
                    "INSERT INTO bank_ledger (account_id, amount, entry_date, category, note) VALUES (?,?,?,?,?)",
                    (self.account_id, amt, today, cat, self.MARK),
                )
        d = datetime.date.today()
        with cp.db_connection() as c:
            breakdown = cp.spending_by_category(c, self.tenant_id, d.year, d.month)
        as_dict = {name: amount for name, amount in breakdown}
        self.assertEqual(as_dict["Rent"], Decimal("1500.00"))
        self.assertEqual(as_dict["Groceries"], Decimal("280.00"))  # merged 220 + 60
        # sorted largest first
        self.assertEqual(breakdown[0][0], "Rent")

    def test_income_is_excluded(self):
        import datetime
        today = datetime.date.today().isoformat()
        with cp.db_connection() as c:
            c.execute(
                "INSERT INTO bank_ledger (account_id, amount, entry_date, category, note) VALUES (?,?,?,?,?)",
                (self.account_id, "5000", today, "Salary", self.MARK),
            )
        d = datetime.date.today()
        with cp.db_connection() as c:
            breakdown = cp.spending_by_category(c, self.tenant_id, d.year, d.month)
        self.assertNotIn("Salary", {n for n, _ in breakdown})


if __name__ == "__main__":
    unittest.main()
