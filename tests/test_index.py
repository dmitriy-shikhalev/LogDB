import random
import asyncio

from .. import column
from .. import index


loop = asyncio.get_event_loop()


COUNT_OF_ELEMENTS = 100


def test_index():
    col = column.Column('pytest/tmp', ('UBigInt', 'Double',))
    idx_col = index.Index(col)

    test_count = COUNT_OF_ELEMENTS

    for i in range(test_count):
        i = loop.run_until_complete(col.append((i, random.random())))
        idx_col.add(i)

    assert loop.run_until_complete(idx_col.read_at(0))[1] == COUNT_OF_ELEMENTS
    assert loop.run_until_complete(idx_col.read_at(0))[0] == 0
    assert len(idx_col) == COUNT_OF_ELEMENTS
    l = list(idx_col.generator())
    assert set(l) == set(list(range(COUNT_OF_ELEMENTS)))
    l_res = []
    for idx in l:
        l_res.append(
            loop.run_until_complete(col.read_at(idx))[1]
        )
    assert l_res == sorted(l_res)
