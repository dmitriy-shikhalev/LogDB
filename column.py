import os
import asyncio
from asyncio import Lock
import struct

import aiofiles

from . import filesystem
from . import type_


loop = asyncio.get_event_loop()

class Column:
    def __init__(self, name, types):
        self.name = name
        self.fs = filesystem.BigFile(f'{name}')
        self.types = [type_.get_type_by_str(type_name)
                      for type_name in types]
        self._len_lock = Lock()

    @property
    def size(self):
        return sum(
            type_.size for type_ in self.types
        )

    async def read_at(self, idx):
        bs = await self.fs.read_at(idx * self.size, self.size)
        vals = []
        bs_i = 0
        for type_ in self.types:
            _bs = bs[bs_i:bs_i + type_.size]
            bs_i += type_.size
            val = type_.from_bs(_bs)
            vals.append(val.value)

        return tuple(vals)

    async def write_at(self, idx, vals):
        if len(self.types) != len(vals):
            raise ValueError(self.types, vals)
        vals = [type_.from_value(val) for type_, val in zip(self.types, vals)]
        bs = b''.join(val.bs for val in vals)
        fs_idx = idx * self.size
        await self.fs.write_at(fs_idx, bs)

    def __len__(self):
        return len(self.fs) // self.size

    async def async_iter(self):
        for idx in range(len(self)):
            yield self.read_at(idx)

    async def append(self, vals):
        async with self._len_lock:
            new_index = len(self)
            await self.write_at(new_index, vals)
            return new_index

    async def get_last_id(self):
        if len(self) == 0:
            return -1
        vals = await self.read_at(len(self) - 1)
        return vals[0]

    def generator(self):
        for idx in range(len(self)):
            yield idx


# class SeriesColumn(Column):
#     def __init__(self, name):
#         self.series_lock = threading.Lock()
#         super().__init__(name, 'UBigInt')
#
#     def get_next_idx(self):
#         with self.series_lock:
#             idx = self.append((len(self), None))
#             return idx