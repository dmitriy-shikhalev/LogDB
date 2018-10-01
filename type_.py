import numbers
import struct
import re
import collections


class WrongType(Exception):
    pass


class UnknownType(Exception):
    pass



class Type:
    def __init__(self, bs=None, value=None):
        assert bs is not None or value is not None
        self._bs = bs
        self._value = value

    @property
    def size(self):
        raise NotImplementedError

    @property
    def struct_character(self):
        raise NotImplementedError

    @property
    def value(self):
        if self._value is None:
            self._value = struct.unpack(self.struct_character, self.bs)[0]
        return self._value

    @property
    def bs(self):
        if self._bs is None:
            self._bs = struct.pack(self.struct_character, self.value)
        return self._bs

    @classmethod
    def from_bs(cls, bs):
        return cls(bs=bs, value=None)

    @classmethod
    def from_value(cls, value):
        return cls(bs=None, value=value)

    def __eq__(self, other):
        if self.__class__.__name__ != other.__class__.__name__:
            return False
        if self.bs != other.bs:
            return False
        return True


class MetaType(Type):
    pass


class UBigInt(Type):
    size = 8  # Unsigned long
    struct_character = '>Q'


class CharASCII(MetaType):
    _dict = dict()
    def __new__(cls, num):
        if num not in cls._dict:
            class _var_char_n(Type):
                size = num
                struct_character = 'c' * num

                def __init__(self, bs=None, value=None):
                    super().__init__(bs, value)
                    if self._bs and len(self._bs) > num:
                        self._bs = self._bs[:num]
                    if self._value and len(self._value) > num:
                        self._value = self._value[:num]

                @property
                def value(self):
                    if self._value is None:
                        self._value = self.bs.rstrip(b'\0').decode('ascii')
                    return self._value

                @property
                def bs(self):
                    if self._bs is None:
                        self._bs = self.value.encode('ascii').ljust(num, b'\0')
                    return self._bs

                def __repr__(self):
                    return f'{self.__class__.__name__}({self.value})'

            _var_char_n.__name__ = f'{cls.__name__}{num:n}'
            cls._dict[num] = _var_char_n
        return cls._dict[num]


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
