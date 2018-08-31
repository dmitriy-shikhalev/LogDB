import os
import shutil

from .saver import *


def teardown_module(module):
    shutil.rmtree('data/test')


def test_int():
    i = Int(256**8-1)

    try:
        i = Int(-1)
    except:
        pass
    else:
        raise Exception('Int(-1) no error bad!')

    try:
        i = Int(256**8)
    except:
        pass
    else:
        raise Exception('Int(256**8) no error bad!')


def test_index():
    i = Index('test')
    row_num = i.append()
    assert row_num == 0
    i[row_num] = 'f'
    assert i[row_num, 0] == 0
    assert i[row_num, 1] == 'f'

    row_num = i.append()
    assert row_num == 1
    i[row_num] = 't'
    assert i[row_num, 0] == 1
    assert i[row_num, 1] == 't'


def test_table():
    t = Table('test')
    t.append(dict(a=100, b=2))

    assert t['a'][0, 0] == 0
    assert t['a'][0, 1] == '100'

    assert t['b'][0, 0] == 0
    assert t['b'][0, 1] == '2'