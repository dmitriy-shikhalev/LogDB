import abc
import base64
import json
import re
import struct

from . import errors


ALL_TYPES = {}


typename_pattern = re.compile(r'(?P<name>\w+)')


class TypeMeta(abc.ABCMeta):
    def __new__(typ, *args, **kwargs):
        cls = super().__new__(typ, *args, **kwargs)
        ALL_TYPES[cls.__name__] = cls
        return cls


class Type(metaclass=TypeMeta):
    def __init__(self):
        pass

    @property
    @abc.abstractmethod
    def fmt_char(self):
        pass

    @property
    def fmt(self):
        return f'>{self.fmt_char}'

    @abc.abstractmethod
    def to_string(self, val):
        pass

    @abc.abstractmethod
    def from_string(self, string):
        pass

    @property
    def size(self):
        return struct.calcsize(self.fmt)

    def encode(self, val):
        try:
            return struct.pack(self.fmt, val)
        except struct.error as err:
            raise errors.EncodeError('{!r} {!r}'.format(self.fmt, val)) from err

    def decode(self, bs):
        try:
            vals = struct.unpack(self.fmt, bs)
            return vals[0]
        except struct.error as err:
            raise errors.DecodeError('{!r} {!r}'.format(self.fmt, bs)) from err

    def __eq__(self, other):
        if self.__class__.__name__ == other.__class__.__name__:
            return True
        return False

    @classmethod
    @abc.abstractmethod
    def isinstance(cls, instance):
        pass


class NumberType(Type):
    def to_string(self, val):
        return json.dumps(val)


class IntegralType(NumberType):
    def from_string(self, string):
        return json.loads(string)

    @classmethod
    def isinstance(cls, instance):
        return isinstance(instance, int)


class DoubleType(NumberType):
    fmt_char = 'd'

    def from_string(self, string):
        return json.loads(string)

    @classmethod
    def isinstance(cls, instance):
        return isinstance(instance, (int, float))


class ULongLong(IntegralType):
    fmt_char = 'Q'


class LongLong(IntegralType):
    fmt_char = 'q'


def get_type_by_name(type_name):
    r = typename_pattern.match(type_name)
    d = r.groupdict()
    try:
        return ALL_TYPES[d['name']]()
    except KeyError:
        raise errors.UnknownType(type_name)
