import asyncio
from aiohttp import web

import os
import struct
import numbers
from datetime import datetime


class Type:
    def __init__(self, value):
        self.value = value
        self._check_type()

    def _check_type(self):
        raise NotImplementedError

    @property
    def size(self):
        raise NotImplementedError

    def encode(self):
        raise NotImplementedError

    @classmethod
    def decode(self, bs):
        raise NotImplementedError


class Int(Type):
    size = 8

    def _check_type(self):
        assert isinstance(self.value, numbers.Integral)
        assert self.value >= 0 and self.value < 256**8

    def encode(self):
        return struct.pack('Q', self.value)

    @classmethod
    def decode(cls, bs):
        return cls(
            struct.unpack('Q', bs)[0]
        )


def VarChar(num):
    class _var_char_n(Type):
        size = None

        def _check_type(self):
            assert isinstance(self.value, str)
            assert len(self.value) <= self.size

        def encode(self):
            bs = self.value.encode('ascii')
            return bs.rjust(self.size, b'\0')

        @classmethod
        def decode(cls, bs):
            return cls(
                bs.decode('ascii')
            )

        def __repr__(self):
            return f'{self.__class__.__name__}({self.value})'

    _var_char_n.size = num
    _var_char_n.__name__ = 'VarChar%d' % num
    return _var_char_n


class Row:
    def __init__(self, *args):
        assert all(issubclass(arg, Type) for arg in args)

        self.types = args

    @property
    def size(self):
        return sum(
            type.size for type in self.types
        )

    def __eq__(self, other):
        if self.size == other.size and all(
            s.size == o.size and s.__name__ == o.__name__
            for s, o in zip(self.types, other.types)
        ):
            return True
        return False


class Column:
    def __init__(self, fn, row=Row(Int, VarChar(256))):
        self.fn = f'data/{fn}'
        if not os.path.exists(
            os.path.dirname(self.fn)
        ):
            os.mkdir(
                os.path.dirname(self.fn)
            )
        self.row = row

        if os.path.exists(self.fn):
            self.fd = open(self.fn, 'r+b')
            self.fd.seek(0, 2)  # move to end of file
            self._new_file = False
        else:
            self.fd = open(self.fn, 'w+b')
            self.idx = 0
            self._new_file = True

    def __setitem__(self, indexes, value):
        assert len(indexes) == 2

        col_num, type_num = indexes
        self.fd.seek(col_num * self.row.size, 0)

        for _, _type in zip(range(type_num), self.row.types):
            self.fd.seek(_type.size, 1)

        value_inst = self.row.types[type_num](value)
        self.fd.write(value_inst.encode())

        # Is there a problem with flush of data to HD???

    def __getitem__(self, indexes):
        col_num, type_num = indexes

        _byte_num = col_num * self.row.size

        self.fd.seek(_byte_num, 0 if _byte_num >= 0 else 2)

        for _, _type in zip(range(type_num), self.row.types):
            self.fd.seek(_type.size, 1)

        _type = self.row.types[type_num]
        bs = self.fd.read(_type.size)
        return _type.decode(bs).value

    def append(self):
        self.fd.seek(0, 2)
        try:
            row_num = self.fd.tell() // self.row.size
        except AttributeError:
            raise Exception(self.row, self.__class__.__name__)
        self.fd.write(b'\0' * self.row.size)
        return row_num

    def close(self):
        self.fd.close()


class Index(Column):
    def __init__(self, date_dir_name):
        fn = os.path.join(
            date_dir_name,
            '_index'
        )
        super().__init__(fn, Row(Int, VarChar(1)))
        if self._new_file:
            self.idx = 0
        else:
            self.idx = self[-1, 0] + 1

    def append(self):
        idx = super().append()
        super().__setitem__((idx, 0), self.idx)
        self.idx += 1
        return idx

    def __setitem__(self, idx, val):
        assert isinstance(idx, int)
        assert isinstance(val, str) and len(val) == 1

        super().__setitem__((idx, 1), val)


class Columns:
    def __init__(self, table_name):
        self.pool = dict()
        self.table_name = table_name
        self['_index'] = Index(f'{self.table_name}')

    def __getitem__(self, idx):
        if idx == '_index':
            return self.pool['_index']
        column_name = idx
        row = Row(Int, VarChar(256))
        if column_name not in self.pool:
            self.pool[column_name] = Column(f'{self.table_name}/{column_name}', row)
        column = self.pool[column_name]
        assert column.row == row
        return column

    def __setitem__(self, k, v):
        self.pool[k] = v


class Table:
    def __init__(self, name):
        self.name = name

        # Create table dir
        if not os.path.exists(f'data/{self.name}'):
            os.mkdir(f'data/{self.name}')

        self.columns = Columns(self.name)

    def __getitem__(self, col):
        return self.columns[col]

    def get_date_dir_name(self, date):
        return 'data/{name}/{date}'.format(
            name=self.name,
            date=date.strftime('%Y-%m-%d')
        )

    def get_or_create_day(self, dt):
        date_dir_name = self.get_date_dir_name(dt)

        if not os.path.exists(date_dir_name):
            os.path.mkdir(date_dir_name)

    def append(self, data):
        data['dt'] = datetime.utcnow()  # this field will strictly increase
        idx = self.columns['_index'].append()
        for k, v in data.items():
            _idx = self.columns[k].append()
            self.columns[k][_idx, 0] = idx
            self.columns[k][_idx, 1] = str(v)
        self.columns['_index'][idx] = 'Y'


class Tables:
    def __init__(self):
        self.pool = dict()

    def __getitem__(self, table_name):
        if table_name not in self.pool:
            self.pool[table_name] = Table(table_name)
        return self.pool[table_name]


class Server:
    """Main class"""
    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.tables = Tables()

    async def saver(self, request):
        # name = request.match_info.get('name', "Anonymous")
        try:
            data = await request.json()

            table = self.tables[data['table']]
            table.add(data['data'])

            return web.Response(text="OK")
        except Exception as err:
            return web.Response(text=f'Exception: {err}')

    def run_forever(self):
        loop = asyncio.get_event_loop()

        app = web.Application()
        app.add_routes([
            web.get('/', self.saver),
        ])
        # web.get('/{name}', handler)])

        web.run_app(app)




if __name__ == '__main__':
    server = Server('localhost', 8080)
    server.run_forever()