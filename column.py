import os
import asyncio
from asyncio import Lock
import threading
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
        try:
            with open(self._idx_fn, 'b') as fd:
                self._idx = struct.unpack('>Q', fd.read())[0]
        except (IOError, ValueError):
            self._idx = 0
        # self.commit_lock = Lock()
        # self.lock = threading.Lock()
        # self.commit_lock = threading.Lock()

    @property
    def _idx_fn(self):
        return os.path.join(
            self.fs.get_path(),
            '_idx'
        )

    async def get_next_idx(self):
        self._idx += 1
        async with aiofiles.open(self._idx_fn, 'w') as fd:
            fd.write(struct.pack('>Q', self._idx))
        return self._idx

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


# class SeriesColumn(Column):
#     def __init__(self, name):
#         self.series_lock = threading.Lock()
#         super().__init__(name, 'UBigInt')
#
#     def get_next_idx(self):
#         with self.series_lock:
#             idx = self.append((len(self), None))
#             return idx