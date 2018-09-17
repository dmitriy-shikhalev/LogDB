

from .type_ import *


def test_ubigint():
    i = UBigInt(256**8-1)

    try:
        i = UBigInt(-1)
    except:
        pass
    else:
        raise Exception('UBigInt(-1) no error on bad value')

    try:
        i = UBigInt(256**8)
    except:
        pass
    else:
        raise Exception('UBigInt(256**8) no error on bad value')

    i = UBigInt(None)
    assert i.encode() == b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'
    i = UBigInt.decode(b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF')
    assert i.value == None


def test_CharASCII():
    CharASCII20 = CharASCII(20)

    c = CharASCII20('abc')
    assert c.value == 'abc'

    c = CharASCII20('a' * 20)

    assert c.encode() == b'a' * 20

    try:
        c = CharASCII20('a'*21)
    except:
        pass
    else:
        raise Exception('CharASCII(20) not error on bad value')

    c = CharASCII20(None)
    assert c.encode() == b''

    c = CharASCII20.decode(b'')
    assert c.value == None

    c = CharASCII20.decode(b'abc')
    assert c.value == 'abc'