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

    loop.run_until_complete(bf.write_at(0, b'\x1f' * 100))
    loop.run_until_complete(bf.write_at(100, b'\xff'*3))

    assert open(os.path.join(
        base_dir, 'pytest/tmp', '0.bf'
    ), 'rb').read() == b'\x1f' * 100
    assert open(os.path.join(
        base_dir, 'pytest/tmp', '1.bf'
    ), 'rb').read() == b'\xff' * 3

    bf = filesystem.BigFile('pytest/tmp')
    assert len(
        bf
    ) == 103
    assert loop.run_until_complete(bf.read_at(0, 103)) == b'\x1f' * 100 + b'\xff' * 3

    loop.run_until_complete(bf.write_at(103, b'A' * 200))

    assert len(os.listdir(os.path.join(
        base_dir, 'pytest/tmp'
    ))) == 4

    assert len(bf) == 103 + 200
    
    assert loop.run_until_complete(bf.read_at(0, len(bf))) == b'\x1f' * 100 + b'\xff\xff\xff' + b'A' * 200

    loop.run_until_complete(bf.write_at(100, b'x'))
    assert loop.run_until_complete(bf.read_at(100, 1)) == b'x'

    loop.run_until_complete(bf.append(b'u' * 300))
    assert len(bf) == 603

    filesystem.BigFile.filesize = _filesize