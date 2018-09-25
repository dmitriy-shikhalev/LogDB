import shutil

from .saver import *


def teardown_module(module):
    shutil.rmtree('data/test_index')
    shutil.rmtree('data/test_table')


def test_index():
    i = Index('test_index')
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
    t = Table(
        {
            "tablename": "testtable",
            "columns": {
                "a": "Int",
                "b": "Double",
                "c": "Char14"
            }
        }
    )
    ret = t.append(dict(a=100, b=2))
    assert ret is True
    ret = t.append(dict(a=200, c=300))
    assert ret is True

    assert t['_index'][0] == 'Y'
    assert t['_index'][1] == 'Y'
    assert t['a'][0] == (0, '100')
    assert t['a'][1] == (1, '200')
    assert t['b'][0] == (0, '2')
    assert t['c'][0] == (1, '300')

    assert len(t) == 2

    assert t['a'][0] == t['a'][-2]

    t.close()

    t = Table('test_table')

    assert t['_index'][0] == 'Y'
    assert t['_index'][1] == 'Y'
    assert t['a'][0] == (0, '100')
    assert t['a'][1] == (1, '200')
    assert t['b'][0] == (0, '2')
    assert t['c'][0] == (1, '300')

    assert len(t) == 2

    assert t['a'][0] == t['a'][-2]
