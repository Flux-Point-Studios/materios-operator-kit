"""Red-team check (PR #51 @ b9b8074): /v1 slot numbers above 2^53 through the real cursor.

Rows at slots 2^53+1 and 2^53+2 collapse to one slot under float parsing, which would make
the same-slot offset skip a row. The real ``cw.Insight.history`` over HTTP must list all."""
import itertools

import pytest

from tests.test_custody_watch import _Insight, insight  # noqa: F401  (fixture)

BIG = 2 ** 53


@pytest.mark.parametrize("page_cap", [1, 2, 3, 500])
def test_slots_past_2_53_are_walked_exactly(insight, page_cap):
    rows = [(BIG + 1, 2), (BIG + 2, 3), (2 ** 63 - 1, 1)]
    _Insight.history = [{"tx_hash": f"{s}-{i}", "slot_no": s, "block_no": 1} for s, n in rows for i in range(n)]
    _Insight.page_cap = page_cap
    walked = [r["tx_hash"] for r in itertools.islice(insight.history("addr1x", BIG + 1, 0), 20)]
    assert walked == [r["tx_hash"] for r in _Insight.history]
    assert float(BIG + 1) == float(BIG + 2) - 1 or float(BIG + 1) == float(BIG)  # float would collapse them
