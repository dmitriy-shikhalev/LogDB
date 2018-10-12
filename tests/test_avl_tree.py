import random

from .. import avl_tree


def test_avl_tree():
    COUNT_OF_ELEMENTS = 1000
    l = [(None, i) for i in range(COUNT_OF_ELEMENTS)]

    class _TestList(list):
        def append(self, element):
            idx = len(self)
            super().append(element)
            return idx

        def __getitem__(self, idx):
            if idx is None:
                return (None, 0, None, None)
            return super().__getitem__(idx)

    l_tree = _TestList()

    tree = avl_tree.AVLTree(l, l_tree)

    for i, v in enumerate(l):
        tree.add(i)
    tree.check_no_loop()

    assert tree.get_left(None) == 0
    assert tree.get_left(-100) == 0
    assert l[tree.get_left(0)][1] == 0
    assert tree.get_left(COUNT_OF_ELEMENTS) is None
    assert l[tree.get_left(COUNT_OF_ELEMENTS - 1)][1] == COUNT_OF_ELEMENTS - 1

    assert tree.get_right(None) == COUNT_OF_ELEMENTS - 1
    assert tree.get_right(-100) is None
    assert l[tree.get_right(0)][1] == 0
    assert tree.get_right(COUNT_OF_ELEMENTS) == COUNT_OF_ELEMENTS - 1
    assert l[tree.get_right(COUNT_OF_ELEMENTS - 1)][1] == COUNT_OF_ELEMENTS - 1

    assert list(tree.generator(2,7)) == [2, 3, 4, 5, 6, 7]
    assert list(tree.generator()) == list(range(COUNT_OF_ELEMENTS))
