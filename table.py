import os
import string
import shutil

from envparse import env

from . import column
from . import index


env.read_envfile()

base_dir = env('LOGDB_BIGFILE_ROOT')


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
        good_chars = set(string.ascii_letters + string.digits)
    if not all(
        char in good_chars for char in name
    ):
        raise BadName(name, f'Must be in {good_chars}')


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
    check_name((name))
    existed_cols = os.listdir(os.path.join(
        base_dir,
        table_name,
    ))
    existed_cols = set(
        (colname.split('_')[0], colname.split('_')[1])
        for colname in existed_cols if not colname.endswith('.idx')
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


class Table:
    def __init__(self, name):
        self.name = name
        self.columns = {}
        self.indexes = {}
        for name, type_ in self.get_columns():
            if name == 'index' and type_ != 'UBigInt':
                raise ColumnExistsWithAnotherType(f'{self.name}/{name}', type_)
            if any(
                    (
                            dirname.split('_')[0] == name
                            and dirname.split('_')[1] != type_
                    )
                for dirname in os.listdir(os.path.join(base_dir, self.name))
            ):
                raise ColumnExistsWithAnotherType(self.name, name, type_)
            self.columns[name] = column.Column(f'{self.name}/{name}', ('UBigInt', type_))
            idx_name = f'{name}.idx'
            if os.path.exists(os.path.join(base_dir, self.name, idx_name)):
                self.indexes[name] = index.Index(self.columns[name])

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

    async def add(self, **kwargs):
        idx = self.get_next_idx()
        for k, v in kwargs.items():
            if v is None:
                continue
            if k not in self.columns:
                raise UnknownColumn(k, self.columns.keys())
            await self.columns[k].append((idx, v))
            if k in self.indexes:
                self.indexes[k].add(idx)


    @classmethod
    def all(cls):
        return QuerySet(cls)


class QuerySet:
    def __init__(self, table):
        self.table = table

    def __iter__(self):
        raise NotImplementedError

    def filter(cls,):
        pass