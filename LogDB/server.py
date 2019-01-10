from aiohttp import web
from urllib.parse import parse_qs

import json

from . import aggfunc
from . import table


class Server:
    """Main class"""
    def __init__(self, config):
        self.host = config['host']
        self.port = int(config['port'])

        self.tables = {
            name: table.Table(config['base_dir'], name, {
                colname: (properties['type'], properties['is_distkey'])
                for colname, properties in columns.items()
            })
            for name, columns in config['tables'].items()
        }

    @staticmethod
    def map_st_to_triplet(st):
        if '==' in st:
            return st.split('==')[0].strip() + '__eq', st.split('==')[1].strip()
        else:
            raise Exception(st)

    async def add(self, request):
        try:
            data = await request.json()

            table = self.tables[data['table']]
            result = await table.append_many(data['data'])

            if result:
                return web.Response(text="OK")
            else:
                return web.Response(text='ERR')
        except Exception as err:
            raise
            return web.Response(text=f'Exception: {err}')

    async def get_rows(self, request):
        try:
            bs = await request.content.read()
            params = parse_qs(bs.decode('utf-8'))
            table = self.tables[params['table'][0]]
            result = table.filter(dict(map(
                self.map_st_to_triplet,
                params['filters'],
            )), params['rows'])
            l = []
            async for row in result:
                l.append(row)
            return web.Response(body=json.dumps({'rows': l}))

        except Exception as err:
            return web.Response(
                body=json.dumps(
                    {
                        'error': [
                            f'Exception: {err}'
                        ]
                    }
                )
            )

    async def get_agg(self, request):
        try:
            bs = await request.content.read()
            params = parse_qs(bs.decode('utf-8'))
            # data = await request.json()

            table = self.tables[params['table'][0]]
            result = table.filter(dict(map(
                self.map_st_to_triplet,
                params['filters'],
            )), params['indexes'] + params['values']).aggregate(
                params['indexes'],
                params['values'],
                getattr(aggfunc, params['aggfunc'][0]),
                params['initiate']
            )
            l = []
            async for row in result:
                l.append(row)
            return web.Response(body=json.dumps({'rows': l}))

        except Exception as err:
            return web.Response(
                body=json.dumps(
                    {
                        'error': [
                            f'Exception: {err}'
                        ]
                    }
                )
            )

    def run_forever(self):
        app = web.Application()
        app.add_routes([
            web.put('/add', self.add),
            web.put('/get_rows', self.get_rows),
            web.put('/get_agg', self.get_agg),
        ])

        web.run_app(
            app,
            host=self.host,
            port=self.port,
        )
