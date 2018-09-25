import asyncio
from aiohttp import web

import json
import os
from collections.abc import Iterable

import table_types







class Index(Column):
    def __init__(self, date_dir_name):
        fn = os.path.join(
            date_dir_name,
            '_idx'
        )
        super().__init__(fn, row=Row(table_types.UBigInt, Char(1)))
        if self._new_file:
            self.idx = 0
        else:
            self.idx = self[-1, 0] + 1

    def append(self):
        idx = super().append()
        super().__setitem__((idx, 0), idx)
        self.idx += 1
        return idx

    def __setitem__(self, idx, val):
        assert isinstance(val, str) and len(val) == 1

        super().__setitem__((idx, 1), val)

    def __getitem__(self, idx):
        if isinstance(idx, int):
            return super().__getitem__((idx, 1))
        return super().__getitem__(idx)


class Columns:
    def __init__(self, table_name, columns):
        self.pool = dict()
        self.table_name = table_name

        self.pool['_index'] = Index(f'{self.table_name}')
        for k, v in columns.items():
            self.pool[k] = Column(self.table_name, Row(UBigInt, v))

    def __getitem__(self, idx):
        if idx == '_index':
            return self.pool['_index']
        column_name = idx
        column = self.pool[column_name]
        return column

    def __setitem__(self, k, v):
        self.pool[k] = v

    def close(self):
        for column in self.pool.values():
            column.close()


class Table:
    def __init__(self, tableconf):
        self.name = tableconf['tablename']

        # Create table dir
        if not os.path.exists(f'data/{self.name}'):
            os.mkdir(f'data/{self.name}')

        self.columns = Columns(self.name, tableconf['columns'])

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
        idx = self.columns['_index'].append()
        for k, v in data.items():
            _idx = self.columns[k].append()
            self.columns[k][_idx, 0] = idx
            self.columns[k][_idx, 1] = str(v)
        self.columns['_index'][idx] = 'Y'
        return True

    def __len__(self):
        return len(self.columns['_index'])


class Tables:
    def __init__(self, config):
        self.pool = dict()
        for tableconf in config['tables']:
            self.pool[tableconf['name']] = Table(tableconf)

    def __getitem__(self, table_name):
        if table_name not in self.pool:
            self.pool[table_name] = Table(table_name)
        return self.pool[table_name]


class Server:
    """Main class"""
    def __init__(self, host, port):
        self.host = host
        self.port = port

        tables_json = json.load(open('config.json'))
        raise Exception(tables_json)

        self.tables = Tables()

    async def saver(self, request):
        # name = request.match_info.get('name', "Anonymous")
        # uuid = request.rel_url.query['uuid']
        try:
            data = await request.json()

            table = self.tables[data['table']]
            result = table.append(data['data'])

            if result:
                return web.Response(text="OK")
            else:
                return web.Response(test='ERR')
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