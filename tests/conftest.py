import os
import shutil

from envparse import env
import pytest


env.read_envfile()

base_dir = os.path.abspath(env('LOGDB_BIGFILE_ROOT'))


@pytest.fixture(scope='module', autouse=True)
def teardown_module():
    for name in os.listdir(base_dir):
        if name.startswith('test'):
            full_path = os.path.join(
                base_dir,
                name
            )
            if os.path.isdir(full_path):
                shutil.rmtree(full_path)
            else:
                os.remove(full_path)
                ls = os.listdir(base_dir)
                if ls:
                    raise Exception(ls)
