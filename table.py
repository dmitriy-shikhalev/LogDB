import os
import string
import shutil
import collections
import asyncio

from envparse import env

from . import column
# from . import index


env.read_envfile()

base_dir = env('LOGDB_BIGFILE_ROOT')


loop = asyncio.get_event_loop()


class TableNotExists(Exception):
    pass


class TableAlreadyExists(Exception):
    pass


class ColumnAlreadyExists(Exception):
    pass

class ColumnNotExists(Exception):
    pass

class ColumnExistsWithAnotherType(Exception):
    pass

class BadName(Exception):
    pass

class UnknownColumn(Exception):
    pass


def check_name(name, is_table=False):
    if is_table:
        good_chars = set(string.ascii_letters + string.digits + '_')
    else:
        good_chars = set(string.ascii_letters + string.digits + '.')
    if not all(
        char in good_chars for char in name
    ):
        raise BadName(name, set(name) - set(good_chars), f'Must be in {good_chars}')


def create_table(name):
    check_name(name, True)
    if name in os.listdir(base_dir):
        raise TableAlreadyExists(name)

    os.mkdir(os.path.join(
        base_dir,
        name
    ))


def drop_table(name):
    check_name(name, True)
    if name not in os.listdir(base_dir):
        raise TableNotExists(name)
    shutil.rmtree(os.path.join(base_dir, name))


def add_column(table_name, name, type_):
    if table_name not in os.listdir(base_dir):
        raise TableNotExists(table_name)
    check_name(name)
    existed_cols = os.listdir(os.path.join(
        base_dir,
        table_name,
    ))
    existed_cols = set(
        (colname.split('_')[0], colname.split('_')[1])
        for colname in existed_cols if '.idx_' not in colname
    )

    col_names = set(name for name, _ in existed_cols)
    if name in col_names:
        raise ColumnAlreadyExists(name, table_name)

    _ = column.Column(f'{table_name}/{name}_{type_}', ('UBigInt', type_))


def drop_column(table_name, name):
    if table_name not in os.listdir(base_dir):
        raise TableNotExists(table_name, os.listdir(base_dir))
    check_name(name)
    table_dir = os.path.join(base_dir, table_name)
    columns = dict(
        dirname.split('_')
        for dirname in os.listdir(table_dir)
    )
    if name not in columns:
        raise ColumnNotExists(name, table_name)
    shutil.rmtree(os.path.join(table_dir, f'{name}_{columns[name]}'))
    try:
        shutil.rmtree(os.path.join(table_dir, f'{name}_{columns[name]}.idx'))
    except FileNotFoundError:
        pass


# def add_index(table_name, name):
#     name_idx = name + '.idx'
#     check_name(name)
#     ls = os.listdir(
#         os.path.join(
#             base_dir,
#             table_name
#         )
#     )
#     for l in ls:
#         if l.startswith(f'{name}'):
#             type_ = l.split('_')[1]
#     col = column.Column(f'{table_name}/{name_idx}_{type_}', ('UBigInt', type_))
#     _ = index.Index(col)


# def drop_index(table_name, name):
#     if table_name not in os.listdir(base_dir):
#         raise TableNotExists(table_name, os.listdir(base_dir))
#     check_name(name)
#     table_dir = os.path.join(base_dir, table_name)
#     columns = dict(
#         dirname.split('_')
#         for dirname in os.listdir(table_dir)
#         if '.idx_' in dirname
#     )
#     name += '.idx'
#     if name not in columns:
#         raise ColumnNotExists(name + '.idx', table_name, columns)
#     shutil.rmtree(os.path.join(table_dir, f'{name}_{columns[name]}'))
#     try:
#         shutil.rmtree(os.path.join(table_dir, f'{name}_{columns[name]}.idx'))
#     except FileNotFoundError:
#         pass


class Table:
    def __init__(self, name):
        self.name = name
        self.columns = {}
        # self.indexes = {}
        for name, type_ in self.get_columns():
            # if name == 'index' and type_ != 'UBigInt':
            #     raise ColumnExistsWithAnotherType(f'{self.name}/{name}', type_)
            for dirname in os.listdir(os.path.join(base_dir, self.name)):
                if dirname.split('_')[0] == name and dirname.split('_')[1] != type_:
                    raise ColumnExistsWithAnotherType(self.name, name, type_, dirname)
            self.columns[name] = column.Column(f'{self.name}/{name}', ('UBigInt', type_))
            idx_name = f'{name}'
            # if os.path.exists(os.path.join(base_dir, self.name, idx_name)):
            #     self.indexes[name] = index.Index(self.columns[name])

        self._idx = len(self)

    def __len__(self):
        return max(
            len(col) for col in self.columns.values()
        )

    def get_next_idx(self):
        idx, self._idx = self._idx, self._idx + 1
        return idx

    def get_columns(self):
        for dirname in os.listdir(
            os.path.join(base_dir, self.name)
        ):
            name, type_ = dirname.split('_')
            yield name, type_

    def add(self, **kwargs):
        idx = self.get_next_idx()
        for k, v in kwargs.items():
            if v is None:
                continue
            if k not in self.columns:
                raise UnknownColumn(k, self.columns.keys())
            loop.run_until_complete(self.columns[k].append((idx, v)))
            # if k in self.indexes:
            #     self.indexes[k].add(idx)

    async def filter(self, **kwargs):
        return Filter(self, **kwargs)

    async def get_max_id(self):
        last_ids = []
        for column in self.columns.values():
            last_id = await column.get_last_id()
            last_ids.append(last_id)
        return max(last_ids)


class UncorrectFilterQuery(Exception):
    pass


class Filter:
    def __init__(self, table, **kwargs):
        self.table = table
        self._dic = kwargs

    async def generator(self):
        max_id = await self.table.get_max_id()
        generator = (id for id in range(max_id + 1))
        for colname, value in self._dic.items():
            colname, op = colname.split('__')
            if op == 'eq':
                generator = (
                    id
                    for id in generator
                    if (await self.table.columns[colname].read_at(id))[1] == value
                )
            elif op == 'gt':
                generator = (
                    id
                    for id in generator
                    if (await self.table.columns[colname].read_at(id))[1] > value
                )
            else:
                raise Exception('Unknown operation', colname, op, value)
        return generator
