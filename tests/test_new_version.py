import time
import asyncio

from .. import new_version


loop = asyncio.get_event_loop()


def test_column():
    test_string = 'ABCDEFG'

    column = new_version.Column('data/test', 'CharASCII(8)')
    for i in range(10):
        loop.run_until_complete(column.append((i, test_string + str(i))))
        assert tuple(loop.run_until_complete(column.read_row(i))) == (i, test_string + str(i))

    assert tuple(loop.run_until_complete(column.read_row(4))) == (4, test_string + '4')
    assert tuple(loop.run_until_complete(column.read_row(1))) == (1, test_string + '1')
    loop.run_until_complete(column.append((10, 'T')))
    loop.run_until_complete(column.append((11, 'Test')))
    loop.run_until_complete(column.append((2, 'Test2')))
    assert tuple(loop.run_until_complete(column.read_row(5))) == (5, test_string + '5')
    assert tuple(loop.run_until_complete(column.read_row(11))) == (11, 'Test')
    assert tuple(loop.run_until_complete(column.read_row(12))) == (2, 'Test2')

    loop.run_until_complete(asyncio.sleep(10))

    loop.run_until_complete(column.close())

    raise FileNotFoundError
