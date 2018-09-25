from table_types import Type, UInt


class BaseRow:
    pass


def Row(row_type):
    assert isinstance(row_type, Type), 'Row([row_type]) - row_type must be a subclass of table_types.Type.'

    class ConcreteRow(BaseRow):
        row_type = row_type
        size = UInt.size + row_type.size

        def __init__(self, idx, val):
            self.idx = UInt(idx)
            self.val = self.row_type(val)

        def __eq__(self, other):
            if (
                    self.size == other.size
                and self.idx == other.idx
                and self.val == other.val
            ):
                return True
            return False

        def encode(self):
            return self.idx.encode() + self.val.encode()

        @classmethod
        def decode(cls, bs):
            return cls(
                UInt.decode(bs[:UInt.size]),
                cls.row_type.decode(bs[UInt.size:])
            )

    ConcreteRow.__name__ = f'Row{row_type.__name__}'
    return ConcreteRow
