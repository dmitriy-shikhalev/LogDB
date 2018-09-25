import asyncio
from asyncio import Lock
import threading

from . import filesystem
from . import type_


loop = asyncio.get_event_loop()

class Column:
    idx_type = type_.UBigInt

    def __init__(self, name, type_name):
        self.name = name
        self.fs = filesystem.BigFile(f'{name}')
        self.type_ = type_.get_type_by_str(type_name)
        # self.lock = Lock()
        # self.commit_lock = Lock()
        self.lock = threading.Lock()
        self.commit_lock = threading.Lock()

    @property
    def size(self):
        return self.type_.size + self.idx_type.size

    def __getitem__(self, index):
        with self.lock:
            bs = self.fs[index * self.size:
                         (index + 1) * self.size]
            if self.idx_type:
                idx = self.idx_type.decode(bs[:self.idx_type.size])
            else:
                idx = None
            val = self.type_.decode(bs[self.idx_type.size:])
            return idx.value, val.value

    def __setitem__(self, index, values):
        with self.lock:
            idx, val = values
            fs_index = index * self.size
            self.fs[fs_index] = (
                self.idx_type(idx).encode()
                + self.type_(val).encode()
            )

    def __len__(self):
        with self.lock:
            return len(self.fs) // self.size

    def __iter__(self):
        for idx in range(len(self)):
            yield self[idx]

    def append(self, values):
        with self.commit_lock:
            idx, val = values
            new_index = idx
            Column.__setitem__(self, new_index, (idx, val))
            return new_index


class SeriesColumn(Column):
    def __init__(self, name):
        self.series_lock = threading.Lock()
        super().__init__(name, 'UBigInt')

    def get_next_idx(self):
        with self.series_lock:
            idx = self.append((len(self), None))
            return idx