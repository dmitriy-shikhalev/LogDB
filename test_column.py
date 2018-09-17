import os
import shutil

from envparse import env

from . import column


base_dir = env('LOGDB_BIGFILE_ROOT')


def teardown_module(module):
    shutil.rmtree(os.path.join(base_dir, 'test'))


def test_column():
    col = column.Column('test/tmp', 'CharASCII(10)')
    for i in range(30):
        st = chr(ord('A') + i) * 10
        col.append((i, st))
    assert col[14] == (14, 'OOOOOOOOOO')

    try:
        col.append(0, b'A' * 11)
    except:
        pass
    else:
        raise Exception('Must be error on size of CharASCII(10).')

    assert len(col) == 30