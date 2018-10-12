import os
import asyncio

from .. import table
from .. import type_


loop = asyncio.get_event_loop()


def test_create_table():
    table.create_table('pytestcreatetable')
    assert [dirname
            for dirname in os.listdir(table.base_dir)
            if dirname[0] != '.'] == ['pytestcreatetable']
    try:
        table.create_table('pytestcreatetable')
    except table.TableAlreadyExists:
        pass
    else:
        assert False, ('Must be exception', table.TableAlreadyExists)

    try:
        table.create_table('pytest^test')
    except table.BadName:
        pass
    else:
        assert False, ('Must be exception', table.BadName)


def test_drop_table():
    try:
        table.drop_table('pytest2')
    except table.TableNotExists:
        pass
    else:
        assert False, ('Must be error', table.TableNotExists)

    table.create_table('pytest2')
    table.drop_table('pytest2')


def test_add_column():
    tablename = 'pytestaddcols'

    table.create_table(tablename)

    table.add_column(tablename, 't1', 'UBigInt')

    try:
        table.add_column(tablename, 't1', 'CharASCII(15)')
    except table.ColumnAlreadyExists:
        pass
    else:
        assert False, 'Must be error ColomnAlreadyExists'

    table.add_column(tablename, 't2', 'CharASCII(15)')

    try:
        table.add_column(tablename, 't3', 'IntBADTYPE')
    except type_.UnknownType:
        pass
    else:
        assert False, 'Must be error UnknownType'

    try:
        table.add_column(tablename, 't&', 'UBigInt')
    except table.BadName:
        pass
    else:
        assert False, ('Must be exception', table.BadName)


def test_drop_column():
    tablename = 'pytest3'

    table.create_table(tablename)

    table.add_column(tablename, 't1', 'UBigInt')
    table.drop_column(tablename, 't1')
    try:
        table.drop_column('pytestnotexists', 'notexists')
    except table.TableNotExists:
        pass
    else:
        assert False, ('Must be error', table.TabbleNotExists)

    try:
        table.drop_column(tablename, 't1')
    except table.ColumnNotExists:
        pass
    else:
        assert False, ('Must be error', table.ColumnNotExists)


def test_add_index():
    pass


def test_drop_index():
    pass


def test_table():
    table.create_table('pytest_test_table')
    table.add_column('pytest_test_table', 'a', 'UBigInt')
    table.add_column('pytest_test_table', 'b', 'CharASCII(3)')
    t = table.Table('pytest_test_table')

    loop.run_until_complete(t.add(a=2, b='ttt'))
    loop.run_until_complete(t.add(a=None, b='ttt'))

    try:
        loop.run_until_complete(t.add(a=2, b='tttt'))
    except type_.WrongType:
        pass
    else:
        # assert False, ('Must be error', type_.WrongType)
        pass

    try:
        loop.run_until_complete(t.add(a=2, b='ttt', c=100))
    except table.UnknownColumn:
        pass
    else:
        assert False, ('Must be error',table.UnknownColumn)

    loop.run_until_complete(t.add(a=2))
