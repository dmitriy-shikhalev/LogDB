import os

from envparse import env


env.read_envfile()


class BigFile:
    base_dir = env('LOGDB_BIGFILE_ROOT')
    filesize = int(env('LOGDB_BIGFILE_FILESIZE'))

    def create_if_not_exists(self):
        dirname = self.get_path()
        if not os.path.exists(dirname):
            os.makedirs(dirname)
            fn = os.path.join(dirname, '0.bf')
            open(fn, 'w').close()

    def __init__(self, name):
        self.name = name
        self.create_if_not_exists()

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

    def __getitem__(self, slice_):
        if isinstance(slice_, int):
            slice_ = slice(slice_, slice_+1)
        file_num_start, idx_start = divmod(slice_.start or 0, self.filesize)
        file_num_end, idx_end = divmod(slice_.stop or len(self), self.filesize)
        for num in range(file_num_start, file_num_end + 1):
            with open(os.path.join(
                self.get_path(), '{:n}.bf'.format(num)
            ), 'rb') as fd:
                if num == file_num_start:
                    fd.seek(idx_start)
                if num == file_num_end:
                    bs = fd.read(idx_end - idx_start)
                else:
                    bs = fd.read()
                yield bs

    def __setitem__(self, idx, bs):
        assert isinstance(idx, int)

        num, idx_ = divmod(idx, self.filesize)
        r = 0
        while r < len(bs):
            left_runner = r
            right_runner = r + min(len(bs) - r, self.filesize - idx_)
            fn = os.path.join(
                self.get_path(), '{:n}.bf'.format(num)
            )
            if not os.path.exists(fn):
                open(fn, 'w').close()
            with open(fn, 'rb+') as fd:
                fd.seek(idx_)
                fd.write(bs[left_runner:right_runner])

            idx_ = 0
            r = right_runner
            num += 1
