import os
import aiofiles


MAX_CHUNK_SIZE = 100 * 1024 ** 2


class File:
    """
    todo
    """
    def __init__(self, base_path: str, table_name: str, column_name: str,
                 distkeys: list):
        path = os.path.join(
            base_path,
            table_name,
        )

        if not os.path.exists(path):
            os.mkdir(path)

        for distkey in distkeys:
            path = os.path.join(
                path,
                distkey
            )

            if not os.path.exists(path):
                os.mkdir(path)

        self.filename = os.path.join(
            path,
            column_name
        )

    async def append(self, bs):
        async with aiofiles.open(self.filename, 'ba') as f:
            await f.write(bs)


    async def read(self, idx=0, num=-1):
        if num == -1:
            num = self.length
        try:
            async with aiofiles.open(self.filename, 'br') as f:
                await f.seek(idx)
                while num > 0:
                    bytes_count = min(MAX_CHUNK_SIZE, num)
                    yield await f.read(bytes_count)
                    num -= bytes_count
        except Exception as err:  # TODO need cetain error type
            raise err

    @property
    def length(self):
        return os.path.getsize(self.filename)
