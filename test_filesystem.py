import os
import shutil

from envparse import env

from . import filesystem

base_dir = env('LOGDB_BIGFILE_ROOT')


def teardown_module(module):
    shutil.rmtree(os.path.join(base_dir, 'test'))
    # pass


def test_bigfile():
    _filesize = filesystem.BigFile.filesize
    filesystem.BigFile.filesize = 100
    bf = filesystem.BigFile('test/tmp')


    bf[0] = b'\x1f' * 100
    bf[100] = b'\xff'*3

    assert open(os.path.join(
        base_dir, 'test/tmp', '0.bf'
    ), 'rb').read() == b'\x1f' * 100
    assert open(os.path.join(
        base_dir, 'test/tmp', '1.bf'
    ), 'rb').read() == b'\xff' * 3

    bf = filesystem.BigFile('test/tmp')
    assert len(b''.join(bf[:])) == 103
    assert b''.join(bf[:103]) == b'\x1f' * 100 + b'\xff' * 3

    bf[103] = b'A' * 200

    assert len(os.listdir(os.path.join(
        base_dir, 'test/tmp'
    ))) == 4

    assert len(bf) == 200 + 103
    
    assert b''.join(bf[:]) == b'\x1f' * 100 + b'\xff\xff\xff' + b'A' * 200

    bf[100] = b'x'
    assert b''.join(bf[100]) == b'x'


    filesystem.BigFile.filesize = _filesize