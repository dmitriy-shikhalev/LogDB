import os
import asyncio

from envparse import env

from .. import column


env.read_envfile()

base_dir = env('LOGDB_BIGFILE_ROOT')

loop = asyncio.get_event_loop()


def test_column():
    col = column.Column('pytest/tmp', ('UBigInt', 'CharASCII(10)',))
    for i in range(30):
        st = chr(ord('A') + i) * 10
        loop.run_until_complete(col.append((i, st)))
    # print(os.listdir('data/pytest/tmp'))
    assert loop.run_until_complete(col.read_at(14)) == (14, 'OOOOOOOOOO')

    try:
        col.append(0, 'A' * 11)
    except:
        pass
    else:
        raise Exception('Must be error on size of CharASCII(10).')

    assert len(col) == 30
    assert loop.run_until_complete(col.read_at(16)) == (16, 'QQQQQQQQQQ')