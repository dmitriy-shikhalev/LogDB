from ..type_ import *


def test_ubigint():
    assert UBigInt.NANValue == 256**8 - 1

    try:
        i = UBigInt(256**8-1)
    except:
        pass
    else:
        raise Exception('UBigInt(256**8-1) is reserved value for NaN, must occure error.')

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
    assert i.encode() == UBigInt.NANCode
    i = UBigInt.decode(UBigInt.NANCode)
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


def test_BlankType():
    b1 = BlankType()
    b2 = BlankType()

    assert b1 is b2
    assert b1 == b2


def test_IndexRow():
    i = IndexRow((0, 0, None, None))
    assert i.value == (0, 0, None, None)
    assert len(i.encode()) == IndexRow.size
    assert i.encode() == (
        b'\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\xFF\xff\xff\xff\xff\xff\xff\xFF'
        b'\xff\xff\xff\xff\xff\xff\xff\xff'
    )
    assert IndexRow.decode(
        b'\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\xFF\xff\xff\xff\xff\xff\xff\xFF'
        b'\xff\xff\xff\xff\xff\xff\xff\xff'
    ).value == [0, 0, None, None]