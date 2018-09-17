import numbers
import struct


class Type:
    def __init__(self, value):
        self.value = value
        self._check_type()

    @property
    def NANValue(self):
        raise NotImplementedError

    @property
    def size(self):
        raise NotImplementedError

    @property
    def struct_character(self):
        raise NotImplementedError

    def _check_type(self):
        raise NotImplementedError

    def encode(self):
        raise NotImplementedError

    @classmethod
    def decode(self, bs):
        raise NotImplementedError

    def __eq__(self, other):
        if self.__class__.__name__ != other.__class__.__name__:
            return False
        if self.value != other.value:
            return False
        return True


class UBigInt(Type):
    size = 8
    NANValue = b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'
    struct_character = '>Q'

    def _check_type(self):
        if self.value is not None:
            assert isinstance(self.value, numbers.Integral)
            assert self.value >= 0 and self.value < 256**8

    def encode(self):
        if self.value is None:
            return self.NANValue
        return struct.pack(self.struct_character, self.value)

    @classmethod
    def decode(cls, bs):
        if bs == cls.NANValue:
            return cls(None)
        return cls(
            struct.unpack(cls.struct_character, bs)[0]
        )


class Int(Type):
    size = 4
    NANValue = b''

    def _check_type(self):
        assert isinstance(self.value, numbers.Integral)
        assert self.value >= -(256**3 * 128) and self.value < (256**3 * 128)

    def encode(self):
        return struct.pack('i', self.value)

    @classmethod
    def decode(cls, bs):
        return cls(
            struct.unpack('i', bs)[0]
        )


class UInt(Type):
    size = 4

    def _check_type(self):
        assert isinstance(self.value, numbers.Integral)
        assert self.value >= 0 and self.value < 256**4

    def encode(self):
        return struct.pack('I', self.value)

    @classmethod
    def decode(cls, bs):
        return cls(
            struct.unpack('I', bs)[0]
        )


class Double(Type):
    size = 8

    def _check_type(self):
        assert isinstance(self.value, numbers.Real)

    def encode(self):
        return struct.pack('d', self.value)

    @classmethod
    def decode(cls, bs):
        return cls(
            struct.unpack('d', bs)[0]
        )


def CharASCII(num):
    class _var_char_n(Type):
        size = None

        def _check_type(self):
            if self.value is None:
                return True
            assert isinstance(self.value, str)
            assert len(self.value) <= self.size


        def encode(self):
            if self.value is None:
                return b''
            bs = self.value.encode('ascii')
            return bs.ljust(self.size, b'\0')

        @classmethod
        def decode(cls, bs):
            if bs == b'':
                return cls(None)
            return cls(
                bs.rstrip(b'\0').decode('ascii')
            )

        def __repr__(self):
            return f'{self.__class__.__name__}({self.value})'

    _var_char_n.size = num
    _var_char_n.__name__ = f'Char{num:n}'
    return _var_char_n