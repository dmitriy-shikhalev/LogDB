import os
import collections
import asyncio

import aiofiles
from envparse import env


loop = asyncio.get_event_loop()

env.read_envfile()


lock = asyncio.Lock()


class Cache:
    BIGFILE_CACHE_SIZE = int(env('BIGFILE_CACHE_SIZE'))

    def __init__(self):
        self.size = 0
        self.cache = collections.OrderedDict()

    def __getitem__(self, key):
        if key in self.cache:
            return self.cache[key]

    def __setitem__(self, key, value):
        if len(value) >= self.BIGFILE_CACHE_SIZE:
            return
        while self.size + len(value) >= self.BIGFILE_CACHE_SIZE:
            self.cache.popitem(last=False)
        self.cache[key] = value

        self.size += len(value)

    def __contains__(self, item):
        return item in self.cache


class BigFile:
    base_dir = env('LOGDB_BIGFILE_ROOT')
    filesize = int(env('LOGDB_BIGFILE_FILESIZE'))

    async def create_if_not_exists(self):
        with await lock:
            dirname = self.get_path()
            if not os.path.exists(dirname):
                os.makedirs(dirname)
                fn = os.path.join(dirname, '0.bf')
                async with aiofiles.open(fn, 'w'):
                    pass

    def __init__(self, name):
        self.name = name
        loop.run_until_complete(self.create_if_not_exists())

        self._cache = Cache()

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

    async def read_from_file(self, fn, from_=None, count=None):
        async with aiofiles.open(fn, 'rb') as fd:
            if from_:
                await fd.seek(from_)
            if count is not None:
                bs = await fd.read(count)
            else:
                bs = await fd.read()
            return bs

    def __getitem__(self, slice_):
        if isinstance(slice_, int):
            slice_ = slice(slice_, slice_+1)
        if (slice_.start, slice_.stop) in self._cache:
            return self._cache[(slice_.start, slice_.stop)]

        file_num_start, idx_start = divmod(slice_.start or 0, self.filesize)
        file_num_end, idx_end = divmod(slice_.stop or self.__len__(), self.filesize)
        result_ls = []
        for num in range(file_num_start, file_num_end + 1):
            from_ = None
            count = None
            fn = os.path.join(
                self.get_path(), '{:n}.bf'.format(num)
            )
            if num == file_num_start:
                from_ = idx_start
            if num == file_num_end:
                count = (idx_end - idx_start)
            bs = loop.run_until_complete(self.read_from_file(fn, from_, count))
            result_ls.append(bs)
        self._cache[(slice_.start, slice_.stop)] = b''.join(result_ls)
        return b''.join(result_ls)

    async def write_to_file(self, fn, bs, idx):
        if not os.path.exists(fn):
            async with aiofiles.open(fn, 'w'):
                pass
        async with aiofiles.open(fn, 'rb+') as fd:
            await fd.seek(idx)
            await fd.write(bs)

    def __setitem__(self, idx, bs):
        assert isinstance(idx, int)

        self._cache = Cache()

        num, idx_ = divmod(idx, self.filesize)
        r = 0
        while r < len(bs):
            left_runner = r
            right_runner = r + min(len(bs) - r, self.filesize - idx_)
            fn = os.path.join(
                self.get_path(), '{:n}.bf'.format(num)
            )
            loop.run_until_complete(self.write_to_file(fn, bs[left_runner:right_runner], idx_))

            idx_ = 0
            r = right_runner
            num += 1
