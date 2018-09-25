import numbers
import struct
import re
import collections


class WrongType(Exception):
    pass


class MetaType:
    pass


class Type(MetaType):
    def __init__(self, value):
        self.value = value
        self._check_type()

    @property
    def NANCode(self):
        raise NotImplementedError

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


class BlankType(Type):
    "Singleton"

    size = 0
    struct_character = ''
    value = None


    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, value=None):
        pass

    def encode(self):
        return b''

    @classmethod
    def decode(cls, bs):
        assert bs == b''
        return cls()

    def __eq__(self, other):
        return self is other


class UBigInt(Type):
    size = 8
    NANCode = b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'
    struct_character = '>Q'
    NANValue = 0xFFFFFFFFFFFFFFFF

    def _check_type(self):
        if self.value is not None:
            if not isinstance(self.value, numbers.Integral):
                raise WrongType(self.value, self.__class__)
            if not (self.value >= 0 and self.value < 256**8 - 1):
                raise WrongType(self.value, self.__class__)
        else:
            return True

    def encode(self):
        if self.value is None:
            return self.NANCode
        return struct.pack(self.struct_character, self.value)

    @classmethod
    def decode(cls, bs):
        if bs == cls.NANCode:
            return cls(None)
        return cls(
            struct.unpack(cls.struct_character, bs)[0]
        )


class Int(Type):
    size = 4
    NANValue = None
    NANCode = b'\x77\x77\x77\x77'

    def _check_type(self):
        if self.value is None:
            return True
        if not (
                isinstance(self.value, numbers.Integral)
                and self.value >= -(256**3 * 128) and self.value < (256**3 * 128)
        ):
            raise WrongType(self.value, self.__class__)

    def encode(self):
        if self.value is None:
            return self.NANCode
        return struct.pack('i', self.value)

    @classmethod
    def decode(cls, bs):
        if bs == self.NANCode:
            return cls(None)
        return cls(
            struct.unpack('i', bs)[0]
        )


class UInt(Type):
    size = 4

    def _check_type(self):
        if self.value is None:
            return True
        if not (
                isinstance(self.value, numbers.Integral)
                and self.value >= 0 and self.value < 256**4
        ):
            raise WrongType(self.value, self.__class__)

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
        if self.value is None:
            return True
        if not isinstance(self.value, numbers.Real):
            raise WrongType(self.value, self.__class__)

    def encode(self):
        return struct.pack('d', self.value)

    @classmethod
    def decode(cls, bs):
        return cls(
            struct.unpack('d', bs)[0]
        )


class CharASCII(MetaType):
    def __new__(cls, num):
        class _var_char_n(Type):
            size = None

            def _check_type(self):
                if self.value is None:
                    return True
                if not (
                        isinstance(self.value, str)
                        and len(self.value) <= self.size
                ):
                    raise WrongType(self.value, self.__class__)


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
        _var_char_n.__name__ = f'{cls.__name__}{num:n}'
        return _var_char_n


class IndexRow(Type):
    size = 8 * 4  # 4 UBigInt - index, weight, left, right
    struct_character = '>QQQQ'
    NANValue = 0xFFFFFFFFFFFFFFFF
    NANCode = b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'

    def _check_type(self):
        if self.value is None:
            return True
        if not (
                isinstance(self.value, collections.Iterable)
                and all(isinstance(val, numbers.Integral) or val is None for val in self.value)
                and all(val is None or (val >= 0 and val < 256 ** 16) for val in self.value)
        ):
            raise WrongType(self.value, self.__class__)


    def encode(self):
        vals = [val if val is not None else self.NANValue for val in self.value]
        return struct.pack(self.struct_character, *vals)

    @classmethod
    def decode(cls, bs):
        vals = struct.unpack(cls.struct_character, bs)
        vals = [
            val if val != cls.NANValue else None
            for val in vals
        ]

        return cls(vals)


class UnknownType(Exception):
    pass


def get_type_by_str(name):
    if name in globals() and issubclass(globals()[name], Type):
        return globals()[name]
    for cls_name, cls in globals().items():
        try:
            if issubclass(cls, MetaType):
                r = re.search(r'\((\w+)\)', name)
                if name.startswith(cls_name) and r:
                    return cls(int(r.groups()[0]))
        except TypeError:
            pass
    raise UnknownType(name)
