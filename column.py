import os

from . import filesystem
from . import type_


class Column:
    idx_type = type_.UBigInt

    def __init__(self, name, type_name):
        self.fs = filesystem.BigFile(f'{name}')
        self.type_ = type_.get_type_by_str(type_name)

    @property
    def size(self):
        return self.type_.size + self.idx_type.size

    def __getitem__(self, index):
        bs = b''.join(self.fs[index * self.size:(index + 1) * self.size])
        idx = self.idx_type.decode(bs[:self.idx_type.size])
        val = self.type_.decode(bs[self.idx_type.size:])
        return idx.value, val.value

    def __setitem__(self, index, values):
        idx, val = values
        fs_index = index * self.size
        self.fs[fs_index] = (
            self.idx_type(idx).encode()
            + self.type_(val).encode()
        )

    def __len__(self):
        return len(self.fs) // self.size

    def append(self, values):
        idx, val = values
        new_index = len(self)
        self[new_index] = (idx, val)
