import random

from .. import column
from .. import index


def test_index():
    col = column.Column('pytest/tmp', 'Double')
    idx_col = index.Index(col)

    test_count = 1

    for i in range(test_count):
        i = col.append((i, random.random()))
        idx_col.add(i)
        # idx_col.check_no_loop()
    for tmp in idx_col:
        pass

    assert idx_col[0][1][1] == test_count
    assert idx_col[0][1][0] == 0
