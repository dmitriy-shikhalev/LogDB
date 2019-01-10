import asyncio
import base64
import json
import operator
import os
import shutil
import struct
import subprocess
import sys
from tempfile import TemporaryDirectory
import time

import pytest
import requests

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import filesystem
import column
import errors
import table
import type_


@pytest.fixture()
def tmp_dir():
    with TemporaryDirectory() as tmp_dir_name:
        yield tmp_dir_name


def test_typename_regexp():
    concreate_type = type_.get_type_by_name('LongLong()')
    assert concreate_type == type_.LongLong()
    concreate_type = type_.get_type_by_name('LongLong')
    assert concreate_type == type_.LongLong()


def test_file(tmp_dir):
    file = filesystem.File(
        tmp_dir,
        'test_without_table_and_column',
        'test_column',
        ['a', 'b']
    )
    file = filesystem.File(
        tmp_dir,
        'test_without_table_and_column',
        'test_column',
        ['a', 'b']
    )
    file = filesystem.File(
        tmp_dir,
        'test_without_table_and_column',
        'test_column',
        ['a', 'b']
    )

    async def test(idx, num):
        l = [
            bs
            async for bs
            in file.read(idx, num)
        ]
        return l

    loop.run_until_complete(file.append(b'aspdfijpoejpafadslkj'))
    assert b''.join(loop.run_until_complete(test(0, 100))) == b'aspdfijpoejpafadslkj'
    assert b''.join(loop.run_until_complete(test(1, 100))) == b'spdfijpoejpafadslkj'


def test_type():
    five_long = type_.LongLong()
    ten_ulong = type_.ULongLong()
    one_long = type_.LongLong()
    assert five_long.fmt == '>q'
    assert ten_ulong.fmt == '>Q'
    assert one_long.fmt == '>q'

def test_column(tmp_dir):
    c = column.Column(
        tmp_dir,
        'test_without_table',
        'test_column',
        type_.LongLong(),
        False,
    )
    async def test_read(c):
        l = []
        async for x in c.read():
            l.append(x)
        return l
    loop.run_until_complete(c.append(100500, ['a', 'b']))
    loop.run_until_complete(c.append(100501, ['a', 'b']))
    loop.run_until_complete(c.append(100502, ['a', 'b']))
    assert loop.run_until_complete(test_read(c)) == [100500, 100501, 100502]


def test_together_lock():
    async def g():
        lock = table.TogetherLock(3)
        async def f(lock):
            async with lock:
                assert lock.n == 0
        tasks = [f(lock) for _ in range(3)]
        await asyncio.gather(*tasks)
    loop.run_until_complete(g())


def test_table_write(tmp_dir):
    test_table = table.Table(
        tmp_dir,
        'test_table_write',
        {
            'a': (type_.LongLong(), True),
            'b': (type_.ULongLong(), False),
            'c': (type_.DoubleType(), False),
        }
    )

    try:
        loop.run_until_complete(
            test_table.append(
                {
                    'a': 100500,
                    'b': -10,
                    'c': 2.34,
                }
            )
        )
    except errors.EncodeError:
        pass
    else:
        raise ValueError

    loop.run_until_complete(
        test_table.append(
            {
                'a': 100500,
                'b': 10,
                'c': 2.34,
            }
        )
    )
    loop.run_until_complete(
        test_table.append(
            {
                'a': 100501,
                'b': 11,
                'c': 2.35,
            }
        )
    )
    loop.run_until_complete(
        test_table.append(
            {
                'a': 200500,
                'b': 20,
                'c': 3.34,
            }
        )
    )

    fn = '{base_path}/test_table_write/{a}/'.format(
        base_path=tmp_dir,
        a='100500',
    )
    assert open(fn + 'a', 'rb').read() == struct.pack('>Q', 100500)
    assert open(fn + 'b', 'rb').read() == struct.pack('>q', 10)
    assert open(fn + 'c', 'rb').read() == struct.pack('>d', 2.34)


async def agen_to_list(agen):
    l = []
    async for val in agen.__aiter__():
        l.append(val)
    return tuple(l)


def test_table_read(tmp_dir):
    test_table = table.Table(
        tmp_dir,
        'test_table_read',
        {
            'a': (type_.LongLong(), True),
            'b': (type_.ULongLong(), False),
            'c': (type_.DoubleType(), False),
        }
    )

    loop.run_until_complete(
        test_table.append(
            {
                'a': 100500,
                'b': 10,
                'c': 2.34,
            }
        )
    )
    loop.run_until_complete(
        test_table.append(
            {
                'a': 100501,
                'b': 11,
                'c': 2.35,
            }
        )
    )
    loop.run_until_complete(
        test_table.append(
            {
                'a': 200500,
                'b': 20,
                'c': 3.34,
            }
        )
    )

    assert loop.run_until_complete(
        agen_to_list(
            test_table.filter(
                {'a': 100500},
                ['a', 'b', 'c']
            )
        )
    ) == (
        (100500, 10, 2.34),
    )

    assert sorted(loop.run_until_complete(
        agen_to_list(
            test_table.filter(
                {'a__gte': 100501},
                ['a', 'c'],
            )
        )
    )) == sorted((
        (100501, 2.35),
        (200500, 3.34),
    ))

    assert set(loop.run_until_complete(
        agen_to_list(
            test_table.filter(
                columns=['b',]
            )
        )
    )) == {
        (10,),
        (11,),
        (20,),
    }

    assert loop.run_until_complete(
        agen_to_list(
            test_table.filter(
                {'a': 200500,},
                columns=['b', 'c']
            )
        )
    ) == (
        (20, 3.34,),
    )

    assert sorted(loop.run_until_complete(
        agen_to_list(
            test_table.filter(
                {'a__gte': 100501},
                ['a', 'b',],
            ).aggregate(['a'], ['b'], operator.add, (0,))
        )
    )) == sorted((
        (100501, 11),
        (200500, 20),
    ))

def test_table_aggregate(tmp_dir):
    test_table = table.Table(
        tmp_dir,
        'test_table',
        {
            'a': (type_.LongLong(), True),
            'b': (type_.ULongLong(), False),
            'c': (type_.DoubleType(), False),
        }
    )

    cycles = 100
    for _ in range(cycles):
        loop.run_until_complete(
            test_table.append(
                {
                    'a': -10,
                    'b': 1,
                    'c': 0,
                }
            )
        )
        loop.run_until_complete(
            test_table.append(
                {
                    'a': -20,
                    'b': 100,
                    'c': 0,
                }
            )
        )

    assert set(loop.run_until_complete(
        agen_to_list(
            test_table.filter(
                {'c': 0},
                ['a', 'b'],
            ).aggregate(['a'], ['b'], operator.add, (0, ))
        )
    )) == {
        (-10, 1 * cycles,),
        (-20, 100 * cycles,),
    }


def test_table_append_many(tmp_dir):
    test_table = table.Table(
        tmp_dir,
        'test_table',
        {
            'a': (type_.LongLong(), True),
            'b': (type_.ULongLong(), False),
            'c': (type_.DoubleType(), False),
            'd': (type_.ULongLong(), True)
        }
    )

    cycles = 10_000
    loop.run_until_complete(
        test_table.append_many(
            [{
                'a': -10,
                'b': 1,
                'c': 0,
                'd': 1,
            } for _ in range(cycles)]
        )
    )
    loop.run_until_complete(
        test_table.append_many(
            [{
                'a': -20,
                'b': 100,
                'c': 0,
                'd': 1,
            } for _ in range(cycles)]
        )
    )

    assert loop.run_until_complete(
        agen_to_list(
            test_table.filter(
                {'d': 1},
                ['a', 'b', 'd'],
            ).aggregate(['d'], ['a', 'b'], operator.add, (0, 0))
        )
    ) == ((
        (1, -30 * cycles, 101 * cycles),
    ))


# Integration tests

@pytest.fixture()
def create_server_process():
    os.mkdir('/tmp/test_log_db')
    try:
        os.mkdir('/tmp/test_log_db/test_table')
        process = subprocess.Popen([
            'python',
            'LogDB',
            'LogDB/config.conf.template'
        ], stdout=sys.stdout)
        try:
            time.sleep(2)

            yield
        finally:
            process.terminate()
    finally:
        shutil.rmtree('/tmp/test_log_db')


@pytest.fixture()
def test_write():
    r = requests.put('http://localhost:3121/add',
                 json.dumps({'table': 'test_table',
                  'data': [
                      {'a': 100500200, 'b': 101},
                      {'a': 2, 'b': 102}
                  ]})
    )
    assert r.text == 'OK'
    r = requests.put('http://localhost:3121/add',
                 json.dumps({
                    'table': 'test_table',
                    'data': [
                        {'a': 100500200, 'b': 103},
                        {'a': 3, 'b': 104},
                        {'a': 3, 'b': 105},
                        {'a': 3, 'b': 106},
                        {'a': 3, 'b': 107},
                        {'a': 3, 'b': 108},
                        {'a': 3, 'b': 109},
                        {'a': 3, 'b': 110},
                        {'a': 3, 'b': 111},
                        {'a': 3, 'b': 112},
                        {'a': 3, 'b': 113},
                    ]
                 })
    )
    assert r.text == 'OK'
    r = requests.put('http://localhost:3121/add',
                 json.dumps({'table': 'test_table',
                  'data': [
                      {'a': 100, 'b': 114},
                      {'a': 2, 'b': 115}
                  ]})
                 )

    assert r.text == 'OK'
    r = requests.put('http://localhost:3121/add',
                 json.dumps({'table': 'test_table',
                  'data': [
                      {'a': 100, 'b': 116},
                      {'a': 2, 'b': 117}
                  ]})
                 )
    assert r.text == 'OK'
    r = requests.put('http://localhost:3121/add',
                 json.dumps({'table': 'test_table',
                  'data': [
                      {'a': 100500200, 'b': 118},
                      {'a': 2, 'b': 119}
                  ]})
                 )
    assert r.text == 'OK'


def test_read(create_server_process, test_write):
    r = requests.put('http://localhost:3121/get_rows',
                 {'table': 'test_table',
                  'rows': ['b'],
                  'filters': ['a == 100']})
    result = r.json()

    assert {
        tuple(x)
        for x in result['rows']
    } == {
        (114,),
        (116,),
    }


def test_aggregate(create_server_process, test_write):
    r = requests.put('http://localhost:3121/get_agg',
                 {'table': 'test_table',
                  'indexes': ['a'],
                  'filters': ['a == 100'],
                  'values': ['b'],
                  'func': 'sum',
                  'initiate': [0],
                  'aggfunc': 'sum'})
    result = r.json()

    print('/tmp/test_log_db/test_table', os.listdir('/tmp/test_log_db/test_table'))

    assert set(map(tuple, result['rows'])) == {
        (100, 230)
    }

    r = requests.put('http://localhost:3121/get_agg',
                     {'table': 'test_table',
                      'indexes': ['a'],
                      'filters': ['a == 3'],
                      'values': ['b'],
                      'func': 'count',
                      'initiate': [0],
                      'aggfunc': 'sum'})
    # raise ZeroDivisionError(r.content)
    result = r.json()

    assert set(map(tuple, result['rows'])) == {
        (3, 1085),
    }