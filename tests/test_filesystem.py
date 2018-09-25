import os
import asyncio

from envparse import env

from .. import filesystem

base_dir = env('LOGDB_BIGFILE_ROOT')

loop = asyncio.get_event_loop()


def test_bigfile():
    _filesize = filesystem.BigFile.filesize
    filesystem.BigFile.filesize = 100
    bf = filesystem.BigFile('pytest/tmp')

    bf[0] = b'\x1f' * 100
    bf[100] = b'\xff'*3

    assert open(os.path.join(
        base_dir, 'pytest/tmp', '0.bf'
    ), 'rb').read() == b'\x1f' * 100
    assert open(os.path.join(
        base_dir, 'pytest/tmp', '1.bf'
    ), 'rb').read() == b'\xff' * 3

    bf = filesystem.BigFile('pytest/tmp')
    assert len(
        bf[slice(None, None)]
    ) == 103
    assert bf[:103] == b'\x1f' * 100 + b'\xff' * 3

    bf[103] = b'A' * 200

    assert len(os.listdir(os.path.join(
        base_dir, 'pytest/tmp'
    ))) == 4

    assert len(bf) == 103 + 200
    
    assert bf[:] == b'\x1f' * 100 + b'\xff\xff\xff' + b'A' * 200

    bf[100] = b'x'
    assert bf[100] == b'x'


    filesystem.BigFile.filesize = _filesize