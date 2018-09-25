import math
import threading

from . import type_
from . import column
from . import filesystem


def tree(index):
    class Tree:
        def __init__(self, idx):
            self.idx = idx

        @property
        def val(self):
            if self.idx is None:
                return None
            return index[self.idx][1][0]

        @property
        def weight(self):
            if self.idx is None:
                return 0
            return index[self.idx][1][1]

        @property
        def height(self):
            if self.idx is None:
                return 0
            if self.weight == 0:
                return 0
            return math.ceil(
                math.log(
                    self.weight,
                    2
                )
            )

        @property
        def left(self):
            if self.idx is None:
                return None
            return Tree(index[self.idx][1][2])

        @property
        def right(self):
            if self.idx is None:
                return None
            return Tree(index[self.idx][1][3])

    return Tree


class Index(column.Column):
    idx_type = type_.BlankType
    type_ = type_.IndexRow

    def __init__(self, column):
        # self.fs = filesystem.BigFile(f'{column.name}.idx')
        super().__init__(f'{column.name}.idx', self.type_.__name__)
        self.tree = tree(self)
        self.base_column = column
        self.index_lock = threading.Lock()

    def check_no_loop(self):
        s = set(range(1, len(self)))
        s_res = set()
        for i in range(len(self)):
            _, (
                _,
                _,
                left,
                right
            ) = self[i]
            if left:
                if left in s_res:
                    raise Exception('Check_no_loop fail: 2 times "{}"'.format(left))
                if left is not None:
                    s_res.add(left)
            if right:
                if right in s_res:
                    raise Exception('Check_no_loop fail: 2 times "{}"'.format(right))
                if right is not None:
                    s_res.add(right)

        assert s == s_res, (s - s_res, s_res - s)

    def __getitem__(self, idx):
        with self.index_lock:
            if idx is None:
                return None
            while idx >= len(self):
                _idx = self._next()
                self.append(
                    (
                        _idx,
                        (
                            None,
                            0,
                            None,
                            None,
                        )
                    )
                )
            return super().__getitem__(idx)

    def __setitem__(self, idx, val):
        with self.index_lock:
            while idx >= len(self):
                _idx = self._next()
                super().append(
                    (
                        _idx,
                        (
                            None,
                            0,
                            None,
                            None,
                        )
                    )
                )
            return super().__setitem__(idx, val)

    def _next(self):
        return len(self)

    def add(self, idx):
        if not len(self.fs):
            _idx = self._next()
            self.append(
                (
                    _idx,
                    (
                        idx,
                        1,
                        None,
                        None,
                    )
                )
            )
        else:
            runner = 0

            te = self.tree(runner)

            inner_idx_ls = []

            # while weight != 0:
            while te.weight:
                # if self.base_column[idx][1] > self.base_column[base_idx][1]:
                if self.base_column[idx][1] > self.base_column[te.val][1]:
                    runner_next = te.right.idx if te.right else None

                    if runner_next is None:
                        runner_next = self._next()
                    self[runner] = None, (te.val, te.weight + 1, te.left.idx, runner_next)
                else:
                    runner_next = te.left.idx if te.left else None

                    if runner_next is None:
                        runner_next = self._next()
                    self[runner] = None, (te.val, te.weight + 1, runner_next, te.right.idx)
                te = self.tree(runner_next)
                inner_idx_ls.append(runner)
                runner = runner_next
            self[runner] = (None, (
                idx,
                1,
                None,
                None,
            ))

            self.ballance(inner_idx_ls)

    def ballance(self, idx_ls):
        for i, idx in list(zip(
                range(len(idx_ls)),
                idx_ls))[::-1]:
            if i > 0 and len(idx_ls) > 2:
                prev_idx = i - 1
            else:
                prev_idx = None
            if prev_idx is not None:
                self.ballance_one(idx, idx_ls[prev_idx])


    def ballance_one(self, idx, prev_idx):
        if idx == 0:  # Do not move first element
            raise Exception('Never occure', idx, prev_idx)

        if (
                self.tree(idx).right is not None
                and self.tree(idx).left is not None
                and self.tree(idx).right.height - self.tree(idx).left.height == 2
        ):
            if (
                    self.tree(idx).right.left is not None
                    and self.tree(idx).right.right is not None
                    and self.tree(idx).right.left.height > self.tree(idx).right.right.height
            ):
                self.big_left_rotation(idx, prev_idx)
            elif (
                    self.tree(idx).right.left is not None
                    and self.tree(idx).right.right is not None
                    and self.tree(idx).right.left.height < self.tree(idx).right.right.height
            ):
                self.small_left_rotation(idx, prev_idx)
        elif (
                self.tree(idx).right is not None
                and self.tree(idx).left is not None
                and self.tree(idx).left.height - self.tree(idx).right.height == 2
        ):
            if (
                self.tree(idx).left.right is not None
                and self.tree(idx).left.left is not None
                and self.tree(idx).left.right.height > self.tree(idx).left.left.height
            ):
                self.big_right_rotation(idx, prev_idx)
            elif (
                self.tree(idx).left.right is not None
                and self.tree(idx).left.left is not None
                and self.tree(idx).left.right.height < self.tree(idx).left.left.height
            ):
                self.small_right_rotation(idx, prev_idx)


    def big_left_rotation(self, idx, prev_idx):
        # prev
        if self.tree(prev_idx).left.idx == idx:
            self[prev_idx] = None, (
                self.tree(prev_idx).val,
                self.tree(prev_idx).weight,
                self.tree(idx).right.left.idx,
                self.tree(prev_idx).right.idx
            )
        else:
            self[prev_idx] = None, (
                self.tree(prev_idx).val,
                self.tree(prev_idx).weight,
                self.tree(prev_idx).left.idx,
                self.tree(idx).right.left.idx
            )

        a_0 = self.tree(idx).val
        a_1 = self.tree(idx).weight - self.tree(idx).right.weight + (
            self.tree(idx).right.left.left.weight
            if self.tree(idx).right.left.left
            else 0
        )
        a_2 = self.tree(idx).left.idx
        a_3 = (
            self.tree(idx).right.left.left.idx
            if self.tree(idx).right.left.left
            else None
        )
        a_idx = idx

        b_0 = self.tree(idx).right.val
        b_1 = self.tree(idx).right.weight - (
            self.tree(idx).right.left.left.weight
            if self.tree(idx).right.left.left
            else 0
        ) - 1
        b_2 = (
            self.tree(idx).right.left.right.idx
            if self.tree(idx).right.left.right
            else None
        )
        b_3 = self.tree(idx).right.right.idx
        b_idx = self.tree(idx).right.idx

        c_0 = self.tree(idx).right.left.idx
        c_1 = self.tree(idx).right.left.weight + self.tree(idx).right.right.weight + self.tree(idx).left.weight + 2
        c_2 = idx
        c_3 = self.tree(idx).right.idx
        c_idx = self.tree(idx).right.left.idx

        # a
        self[a_idx] = None, (
            a_0,
            a_1,
            a_2,
            a_3
        )
        # b
        self[b_idx] = None, (
            b_0,
            b_1,
            b_2,
            b_3
        )
        # c
        self[c_idx] = None, (
            c_0,
            c_1,
            c_2,
            c_3
        )

    def small_left_rotation(self, idx, prev_idx):
        # prev
        if self.tree(prev_idx).left.idx == idx:
            self[prev_idx] = None, (
                self.tree(prev_idx).val,
                self.tree(prev_idx).weight,
                self.tree(idx).right.idx,
                self.tree(prev_idx).right.idx
            )
        else:
            self[prev_idx] = None, (
                self.tree(prev_idx).val,
                self.tree(prev_idx).weight,
                self.tree(prev_idx).left.idx,
                self.tree(idx).right.idx
            )

        a_0 = self.tree(idx).val
        a_1 = self.tree(idx).weight - self.tree(idx).right.right.weight - 1
        a_2 = self.tree(idx).left.idx
        a_3 = self.tree(idx).right.left.idx
        a_idx = idx

        b_0 = self.tree(idx).right.val
        b_1 = self.tree(idx).right.weight + self.tree(idx).left.weight + 1
        b_2 = idx
        b_3 = self.tree(idx).right.right.idx
        b_idx = self.tree(idx).right.idx

        # a
        self[a_idx] = None, (
            a_0,
            a_1,
            a_2,
            a_3
        )
        # b
        self[b_idx] = None, (
            b_0,
            b_1,
            b_2,
            b_3
        )

    def small_right_rotation(self, idx, prev_idx):
        # prev
        if self.tree(prev_idx).left.idx == idx:
            self[prev_idx] = None, (
                self.tree(prev_idx).val,
                self.tree(prev_idx).weight,
                self.tree(idx).left.idx,
                self.tree(prev_idx).right.idx
            )
        else:
            assert self.tree(prev_idx).right.idx == idx, (
                prev_idx,
                idx,
                self.tree(prev_idx).left.idx,
                self.tree(prev_idx).right.idx,
            )
            self[prev_idx] = None, (
                self.tree(prev_idx).val,
                self.tree(prev_idx).weight,
                self.tree(prev_idx).left.idx,
                self.tree(idx).left.idx
            )

        a_0 = self.tree(idx).val
        a_1 = self.tree(idx).weight - self.tree(idx).left.left.weight - 1
        a_2 = self.tree(idx).left.right.idx
        a_3 = self.tree(idx).right.idx
        a_idx = idx

        b_0 = self.tree(idx).left.val
        b_1 = self.tree(idx).left.weight + self.tree(idx).right.weight + 1
        b_2 = self.tree(idx).left.left.idx
        b_3 = idx
        b_idx = self.tree(idx).left.idx

        # a
        self[a_idx] = None, (
            a_0,
            a_1,
            a_2,
            a_3
        )
        # b
        self[b_idx] = None, (
            b_0,
            b_1,
            b_2,
            b_3
        )

    def big_right_rotation(self, idx, prev_idx):
        # prev
        if self.tree(prev_idx).left.idx == idx:
            self[prev_idx] = None, (
                self.tree(prev_idx).val,
                self.tree(prev_idx).weight,
                self.tree(idx).left.right.idx,
                self.tree(prev_idx).right.idx
            )
        else:
            self[prev_idx] = None, (
                self.tree(prev_idx).val,
                self.tree(prev_idx).weight,
                self.tree(prev_idx).left.idx,
                self.tree(idx).left.right.idx
            )

        a_0 = self.tree(idx).val
        a_1 = self.tree(idx).weight - self.tree(idx).left.weight + self.tree(idx).left.right.right.weight
        a_2 = self.tree(idx).left.right.right.idx
        a_3 = self.tree(idx).right.idx
        a_idx = idx

        b_0 = self.tree(idx).left.val
        b_1 = self.tree(idx).left.weight - self.tree(idx).left.right.right.weight - 1
        b_2 = self.tree(idx).left.left.idx
        b_3 = self.tree(idx).left.right.left.idx
        b_idx = self.tree(idx).left.idx

        c_0 = self.tree(idx).left.right.idx
        c_1 = self.tree(idx).left.right.weight + self.tree(idx).left.left.weight + self.tree(idx).right.weight + 2
        c_2 = self.tree(idx).left.idx
        c_3 = idx
        c_idx = self.tree(idx).left.right.idx

        # a
        self[a_idx] = None, (
            a_0,
            a_1,
            a_2,
            a_3
        )
        # b
        self[b_idx] = None, (
            b_0,
            b_1,
            b_2,
            b_3
        )
        # c
        self[c_idx] = None, (
            c_0,
            c_1,
            c_2,
            c_3
        )
