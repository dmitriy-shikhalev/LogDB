import os
# import asyncio

from . import filesystem
from . import type_ as type_module


class DirsIterator:
    def __init__(self, path):
        self.path = path

    def __iter__(self):
        for subdirname in os.listdir(self.path):
            yield (
                subdirname,
                os.path.join(self.path, subdirname)
            )


class Column:
    def __init__(self, base_path, table_name, column_name, type_, is_distkey):
        self.base_path = base_path
        self.table_name = table_name
        self.column_name = column_name
        assert isinstance(type_, type_module.Type)
        self.type_ = type_
        self.is_distkey = is_distkey

    @property
    def table_path(self):
        return os.path.join(
            self.base_path,
            self.table_name,
        )

    @property
    def size(self):
        return self.type_.size

    def get_file(self, distkeys):
        return filesystem.File(
            self.base_path,
            self.table_name,
            self.column_name,
            distkeys
        )

    def get_files(self, distkeys):
        base_dir = os.path.join(
            self.base_path,
            self.table_name,
            *distkeys
        )
        has_subdirs = False
        for dirname in os.listdir(base_dir):
            if os.path.isdir(os.path.join(
                base_dir,
                dirname
            )):
                has_subdirs = True
                yield from self.get_files(distkeys + (dirname,))
        if has_subdirs is False:
            yield filesystem.File(
                self.base_path,
                self.table_name,
                self.column_name,
                distkeys
            )

    def _get_file_distkeys(self, path, filters, distkeys=()):
        filters = filters.copy()
        if not filters:
            yield distkeys
        else:
            filter = filters.pop(list(filters.keys())[0])
            dirs_iter = DirsIterator(os.path.join(self.table_path, path))
            for dirname, full_dirname in dirs_iter:
                if not filter or filter.cmp(dirname):
                    yield from self._get_file_distkeys(
                        os.path.join(path, dirname),
                        filters,
                        distkeys + (dirname,)
                    )

    async def append(self, val, distkeys, together_lock=None):
        bs = self.type_.encode(val)
        if together_lock is not None:
            async with together_lock:
        # if True
                await self.get_file(distkeys).append(bs)
        else:
            await self.get_file(distkeys).append(bs)

    async def append_many(self, vals, distkeys, together_lock=None):
        bs = b''.join(self.type_.encode(val) for val in vals)
        if together_lock is not None:
            async with together_lock:
                await self.get_file(distkeys).append(bs)
        else:
            await self.get_file(distkeys).append(bs)

    async def read(self, filters=None):
        if filters is None:
            filters = {}
        for distkeys in self._get_file_distkeys('', filters):
            for file in self.get_files(distkeys):
                async for bs in file.read():
                    for i in range(0, len(bs), self.type_.size):
                        val = self.type_.decode(
                            bs[i:i + self.type_.size]
                        )
                        yield val
