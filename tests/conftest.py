import os
import shutil

from envparse import env
import pytest


env.read_envfile()

base_dir = os.path.abspath(env('LOGDB_BIGFILE_ROOT'))


@pytest.fixture(scope='module', autouse=True)
def teardown_module():
    try:
        for name in os.listdir(base_dir):
            if name.startswith('pytest'):
                shutil.rmtree(os.path.join(base_dir, name))
    except Exception as err:
        pass