import asyncio

from . import column
from . import avl_tree


class SerialColumn:
    none_idx = 256 ** 8 - 1

    def __init__(self, column):
        self.column = column
        self.loop = asyncio.get_event_loop()

    def __getitem__(self, idx):
        val = self.loop.run_until_complete(self.column.read_at(idx))
        val = list(None if v == self.none_idx else v for v in val)
        return val

    def __setitem__(self, idx, val):
        val = [self.none_idx if v is None else v for v in val]
        return self.loop.run_until_complete(self.column.write_at(idx, val))

    def __len__(self):
        return len(self.column)

    def append(self, val):
        idx = len(self)
        val = [self.none_idx if v is None else v for v in val]
        self.loop.run_until_complete(self.column.append(val))
        return idx


class Index(column.Column):
    def __init__(self, column):
        super().__init__(f'{column.name}.idx', ('UBigInt', 'UBigInt', 'UBigInt', 'UBigInt'))
        self.base_column = column
        self.avl_tree = avl_tree.AVLTree(SerialColumn(self.base_column), SerialColumn(self))

    async def read_at(self, idx):
        if idx is None or idx > len(self):
            return (None, 0, None, None)
        return await super().read_at(idx)

    def add(self, val):
        self.avl_tree.add(val)

    def generator(self, left=None, right=None):
        return self.avl_tree.generator(left, right)
