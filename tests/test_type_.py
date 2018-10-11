import struct

from ..type_ import *


def test_ubigint():
    u1 = UBigInt.from_value(100500)
    u2 = UBigInt.from_value(100500)
    u3 = UBigInt.from_value(1005001)

    assert u1 == u2
    assert u1 != u3


def test_CharASCII():
    CharASCII20 = CharASCII(20)

    c = CharASCII20.from_value('abc')
    assert c.bs == b'abc'.ljust(20, b'\0')

    c = CharASCII20.from_value('a' * 20)

    assert c.bs == b'a' * 20

    c = CharASCII20.from_value('a' * 21)
    assert c.value == 'a' * 20

    c = CharASCII20.from_bs(b'')
    assert c.value == ''

    c = CharASCII20.from_bs(b'abc'.ljust(20, b'\0'))
    assert c.value == 'abc'
    assert c.bs == b'abc'.ljust(20, b'\0')


def test_bad_type():
    try:
        u = UBigInt.from_value('abc')
        _ = u.bs
    except ValueError:
        pass
    else:
        assert False, 'Must be error. Bad argument.'