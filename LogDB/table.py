import asyncio
import typing

from . import column
from . import errors
from . import type_


class TogetherLock():
    """
    This class provide simultanious launch of
    N functions by blocking others until
    all of its be ready.
    """

    def __init__(self, n):
        self.init = n
        self.n = n
        self.events = [asyncio.Event() for _ in range(n)]

    async def __aenter__(self):
        assert self.n > 0
        self.n -= 1
        self.events[self.n].set()
        for event in self.events:
            await event.wait()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass



class Filter:
    def __init__(self, field_name, val, type_):
        if '__' in field_name:
            self.field_name, self.op = field_name.split('__')
        else:
            self.field_name, self.op = field_name, 'eq'
        if not type_.isinstance(val):
            val = type_.from_string(val)
        self.val = val
        self.type_ = type_

    def cmp(self, val):
        if self.op == 'eq':
            return val == self.val
        elif self.op == 'neq':
            return val != self.val
        elif self.op == 'lt':
            return val < self.val
        elif self.op == 'lte':
            return val <= self.val
        elif self.op == 'gt':
            return val > self.val
        elif self.op == 'gte':
            return val >= self.val
        else:
            raise ValueError('Unknown operation %s' % self.op)


class Table(object):
    def __init__(self, base_path, table_name, types):
        self.base_path = base_path
        self.table_name = table_name
        self.columns = {
            name: column.Column(
                self.base_path,
                self.table_name,
                name,
                type_.get_type_by_name(pair[0])
                    if isinstance(pair[0], str) else pair[0],
                pair[1],  # distkey
            )
            for name, pair in types.items()
        }
        self.column_names = list(self.columns.keys())
        self.distkey_columns = {
            k: v
            for k, v in self.columns.items()
            if v.is_distkey
        }
        self.distkeys = list(self.distkey_columns.keys())  # This work good because dict
                                                           # in Python3.7 is ordered
        self.lock = asyncio.Lock()

    async def append(self, row):
        if len(row) != len(self.columns):
            raise errors.WrongAmountOfArguments(row.keys(), self.columns.keys())
        tasks = []
        together_lock = TogetherLock(len(self.columns))

        try:
            # import pdb; pdb.set_trace()
            distkeys = [
                col.type_.to_string(row[col_name])
                for col_name, col in self.distkey_columns.items()
            ]
        except KeyError as err:
            raise errors.ToStringError from err

        async with self.lock:
            for k, v in row.items():
                tasks.append(
                    self.columns[k].append(
                        v,
                        distkeys,
                        together_lock,
                    )
                )
            await asyncio.gather(*tasks)
        return True

    async def append_many(self, rows):
        for row in rows:
            if len(row) != len(self.columns):
                raise ValueError(row.keys(), self.columns.keys())

        try:
            distkeys_list = tuple(
                tuple(
                    col.type_.to_string(row[col_name])
                    for col_name, col in self.distkey_columns.items()
                ) for row in rows
            )
        except KeyError as err:
            raise ValueError from err

        dic = {}
        for row, distkeys in zip(rows, distkeys_list):
            dic.setdefault(distkeys, [])
            dic[distkeys].append(row)

        async with self.lock:
            for distkeys, rows in dic.items():
                together_lock = TogetherLock(len(self.columns))
                tasks = []
                for key in rows[0].keys():
                    tasks.append(
                        self.columns[key].append_many(
                            [row[key] for row in rows],
                            distkeys,
                            together_lock,
                        )
                    )
                await asyncio.gather(*tasks)
        return True

    def filter(self, filters: dict=None, columns: list=None):
        if filters is None:
            filters = dict()
        if columns is None:
            columns = list(self.columns.keys())
        return FilterMethod(self, filters,  columns)


class FilterMethod:
    def __init__(self, table, filters, columns):
        self.table = table
        self.filters = filters
        self.columns = columns
        assert all(
            (col_name in table.columns)
            for col_name in self.columns
        )

    async def __aiter__(self):
        _filters = [
                Filter(k, v, self.table.columns[k.split('__')[0]].type_)
                for k, v in self.filters.items()
        ]
        _filters = {
            filter.field_name: filter
            for filter in _filters
        }
        filters = {}
        for column_name, _ in self.table.distkey_columns.items():
            if column_name not in _filters:
                filters[column_name] = None
                # pass
            else:
                filters[column_name] = _filters[column_name]

        for filter_ in filters.values():
            if isinstance(filter_, Filter) and \
                    filter_.field_name not in self.table.distkey_columns.keys():
                raise errors.APIError('Can filter only by destkey. %r must be in %r' % (
                    filter_.field_name, self.table.distkey_columns.keys()
                ))

            if isinstance(filter_, Filter):
                # Set Type.to_string(val) in filter.val
                filter_.val = self.table.columns[
                    filter_.field_name
                ].type_.to_string(filter_.val)

        iters = [
            column.read(filters)
            for colname, column in self.table.columns.items() if colname in self.columns
        ]

        while True:
            try:
                vals = tuple([
                    await col_iter.__anext__()
                    for col_iter in iters
                ])
                _vals = []
                for val, column in zip(vals, (
                        val for name, val in self.table.columns.items()
                        if name in self.columns
                    )
                ):
                    _vals.append(val)
                vals = tuple(_vals)
            except StopAsyncIteration:
                break
            else:
                yield vals

    def aggregate(self, indexes, values, aggfunc, initiate):
        return Aggregate(self, indexes, values, aggfunc, initiate)


class Aggregate:
    def __init__(self, filter_obj: FilterMethod, indexes: typing.Iterable[str],
                 values: typing.Iterable, aggfunc: typing.Callable,
                 initiate: typing.Iterable):
        self.filter_obj = filter_obj
        self.indexes = indexes
        self.values = values
        self.aggfunc = aggfunc
        self.initiate = initiate

    async def __aiter__(self):
        dic = dict()
        async for vals in self.filter_obj:
            key = tuple(
                vals[
                    self.filter_obj.columns.index(k)
                ] for k in self.indexes
            )
            value = tuple(
                vals[
                    self.filter_obj.columns.index(k)
                ] for k in self.values
            )
            dic.setdefault(key, list(self.initiate))
            for i in range(len(self.values)):
                dic[key][i] = self.aggfunc(
                    dic[key][i], value[i]
                )

        for key, value in dic.items():
            yield tuple((*key, *value))

