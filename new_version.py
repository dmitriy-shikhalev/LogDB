import asyncio
from contextlib import suppress

import aiofiles

from . import type_
from . import config


loop = asyncio.get_event_loop()


class Row:
    index_type = type_.get_type_by_str('UBigInt')

    def __init__(self, type_):
        self.type = type_

    @property
    def size(self):
        return self.index_type.size + self.type.size

    def encode(self, values):
        return b''.join(
            map(
                lambda TypeVal: TypeVal[0].from_value(TypeVal[1]).bs,
                zip(
                    (self.index_type, self.type),
                    values
                )
            )
        )

    def decode(self, bs):
        yield self.index_type.from_bs(
            bs[:self.index_type.size]
        ).value
        yield self.type.from_bs(
            bs[self.index_type.size:]
        ).value


class Column:
    def __init__(self, filename, type_name):
        self.filename = filename
        self.type = type_.get_type_by_str(type_name)
        self.row = Row(self.type)
        self._fh_r = None
        self._last_time_fh_r = None
        self._fh_a = None
        self._last_time_fh_a = None
        self._lock_fh_r = asyncio.Lock()
        self._lock_fh_a = asyncio.Lock()

        self._close_fh_task = asyncio.ensure_future(self._close_fh())

    async def close(self):
        self._close_fh_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._close_fh_task

    async def _close_fh(self):
        while True:
            async with self._lock_fh_a, self._lock_fh_r:
                for attr_name in (
                    '_fh_r', '_fh_a'
                ):
                    if (
                        getattr(self, attr_name) is not None
                        and (
                            loop.time()
                            - getattr(self, '_last_time' + attr_name)
                        ) > config.FILE_LIFETIME
                    ):
                        getattr(self, attr_name).close()
                        setattr(self, attr_name, None)

                print('fuck')
                asyncio.sleep(config.FILE_CLOSE_FUNCTION_SLEEP_TIME)
        # asyncio.ensure_future(self._close_fh())
        # asyncio.ensure_future(config.FILE_CLOSE_FUNCTION_SLEEP_TIME, self._close_fh())

    def _get_read_fh(self):
        if self._fh_r is None:
            self._fh_r = open(self.filename, 'rb')
        return self._fh_r

    def _get_append_fh(self):
        if self._fh_a is None:
            self._fh_a = open(self.filename, 'ab')
        return self._fh_a

    async def append(self, values):
       async with self._lock_fh_a:
            fh = self._get_append_fh()
            self._last_time_fh_a = loop.time()

            fh.write(
                self.row.encode(values)
            )
            fh.flush()

    async def read_row(self, idx):
        async with self._lock_fh_r:
            fh = self._get_read_fh()
            self._last_time_fh_r = loop.time()

            file_idx = idx * self.row.size
            if file_idx != fh.tell():
                fh.seek(file_idx)
            return self.row.decode(
                fh.read(self.row.size)
            )


class Table:
    def __init__(self):
        raise NotImplementedError