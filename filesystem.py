import os
import asyncio

import aiofiles
from envparse import env


env.read_envfile()


# class Cache:
#     BIGFILE_CACHE_SIZE = int(env('BIGFILE_CACHE_SIZE'))
#
#     def __init__(self):
#         self.size = 0
#         self.cache = collections.OrderedDict()
#
#     def __getitem__(self, key):
#         if key in self.cache:
#             return self.cache[key]
#
#     def __setitem__(self, key, value):
#         if len(value) >= self.BIGFILE_CACHE_SIZE:
#             return
#         while self.size + len(value) >= self.BIGFILE_CACHE_SIZE:
#             self.cache.popitem(last=False)
#         self.cache[key] = value
#
#         self.size += len(value)
#
#     def __contains__(self, item):
#         return item in self.cache


class _BigFileOne:
    def __init__(self, fn):
        self.fn = fn
        self.lock = asyncio.Lock()
        if not os.path.exists(fn):
            open(self.fn, 'w').close()

    def __len__(self):
        return os.path.getsize(self.fn)

    async def read_at(self, at, count):
        async with self.lock:
            async with aiofiles.open(self.fn, 'rb') as fd:
                await fd.seek(at)
                return await fd.read(count)

    async def write_at(self, at, bs):
        if at < 0 or at > len(self):
            raise IndexError(at)
        async with self.lock:
            async with aiofiles.open(self.fn, 'r+b') as fd:
                await fd.seek(at)
                await fd.write(bs)
                await fd.flush()

    async def append(self, bs):
        async with self.lock:
            async with open(self.fn, 'r+b') as fd:
                await fd.seek(0, -2)
                await fd.write(bs)


class BigFile:
    base_dir = env('LOGDB_BIGFILE_ROOT')
    filesize = int(env('LOGDB_BIGFILE_FILESIZE'))

    def __init__(self, name):
        self.name = name

        if not os.path.exists(self.get_path()):
            os.makedirs(self.get_path())

        self.file_one_dict = dict()

        self._len_lock = asyncio.Lock()

        # self._cache = Cache()

    def get_path(self):
        return os.path.join(
            self.base_dir,
            self.name,
        )

    def __len__(self):
        size = 0
        for fn in os.listdir(
            self.get_path()
        ):
            size += os.path.getsize(
                os.path.join(self.get_path(), fn)
            )
        return size

    async def get_file_one(self, file_num):
        fn = os.path.join(self.get_path(), f'{file_num}.bf')
        async with self._len_lock:
            if fn not in self.file_one_dict:
                file_one = _BigFileOne(fn)
                self.file_one_dict[fn] = file_one
        return self.file_one_dict[fn]

    async def read_at(self, at, count):
        file_num_start, idx_start = divmod(at, self.filesize)
        file_num_end, idx_end = divmod(at + count, self.filesize)

        l = []

        for file_num in range(file_num_start, file_num_end + 1):
            if file_num == file_num_start:
                at = idx_start
                _count = min(count, self.filesize - idx_start)
            else:
                at = 0
                _count = min(count, self.filesize)

            file_obj = await self.get_file_one(file_num)
            bs = await file_obj.read_at(at, count)
            l.append(bs)

            count -= _count
        return b''.join(l)

    async def write_at(self, at, bs):
        file_num, idx = divmod(at, self.filesize)
        at = idx
        from_ = 0
        till = len(bs)
        while from_ < till:
            count = min(till - from_, self.filesize - at)
            file_obj = await self.get_file_one(file_num)
            await file_obj.write_at(at, bs[from_:from_ + count])
            at = 0
            file_num += 1
            from_ += count
        return len(bs)

    async def append(self, bs):
        async with self._len_lock:
            return await self.write_at(len(self), bs)
