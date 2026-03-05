import unittest
from datetime import date

from app.services.intelligence.rules.purchases import run_supplier_dependency
from app.services.intelligence.rules.sales import run_sales_drop_period
from app.services.intelligence.rules.inventory import run_dead_stock_value
from app.services.intelligence.types import RuleContext


class _Result:
    def __init__(self, *, scalar=None, one_row=None, all_rows=None):
        self._scalar = scalar
        self._one = one_row
        self._all = all_rows or []

    def scalar_one(self):
        return self._scalar

    def scalar_one_or_none(self):
        return self._scalar

    def one(self):
        return self._one

    def all(self):
        return self._all


class _DB:
    def __init__(self, results):
        self._results = list(results)

    async def execute(self, _stmt):
        if not self._results:
            raise RuntimeError('No mocked result available')
        return self._results.pop(0)


def _ctx():
    return RuleContext(
        tenant_id=1,
        tenant_slug='tenant-a',
        tenant_plan='enterprise',
        tenant_source='pharmacyone',
        period_from=date(2026, 2, 1),
        period_to=date(2026, 2, 28),
        previous_from=date(2026, 1, 1),
        previous_to=date(2026, 1, 31),
        as_of=date(2026, 2, 28),
    )


class IntelligenceRuleTests(unittest.IsolatedAsyncioTestCase):
    async def test_sales_drop_rule_creates_insight(self):
        db = _DB(
            [
                _Result(scalar=80.0),  # current net
                _Result(scalar=100.0),  # previous net
            ]
        )
        rows = await run_sales_drop_period(db, {'drop_pct': 10}, _ctx())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].rule_code, 'SLS_DROP_PERIOD')
        self.assertLess(rows[0].delta_pct, 0)

    async def test_supplier_dependency_rule_creates_insight(self):
        db = _DB(
            [
                _Result(all_rows=[('SUP1', 60.0), ('SUP2', 40.0)]),
                _Result(all_rows=[('SUP1', 'Supplier 1'), ('SUP2', 'Supplier 2')]),
            ]
        )
        rows = await run_supplier_dependency(db, {'top_supplier_share_pct': 50}, _ctx())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].rule_code, 'SUP_DEPENDENCY')
        self.assertEqual(rows[0].entity_external_id, 'SUP1')

    async def test_dead_stock_rule_creates_insight(self):
        db = _DB(
            [
                _Result(scalar=date(2026, 2, 28)),  # latest snapshot
                _Result(all_rows=[('item-1', 500.0)]),  # inventory rows
                _Result(all_rows=[]),  # no sales movement
                _Result(all_rows=[('item-1', 'ITEM1', 'Item One')]),  # item metadata
            ]
        )
        rows = await run_dead_stock_value(db, {'no_sales_days': 60, 'min_value': 100}, _ctx())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].rule_code, 'INV_DEAD_STOCK')
        self.assertEqual(rows[0].entity_external_id, 'item-1')


if __name__ == '__main__':
    unittest.main()
