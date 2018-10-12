import math


"""
AVL-tree class.
"""

class AVLTree:
    @staticmethod
    def height(weight):
        if weight == 0:
            return 0
        return math.ceil(
            math.log(
                weight,
                2
            )
        )

    def __init__(self, base, recipient):
        """
        "base" is an instance of class with method __getitem__, that return value.
        "recipient" is an instance of class with methods "__getitem__",
        "__setitem__" and "append".
            type of element - (out index (int), weight (int), left inner index),
            right inner index;
            recipient get/set a class of "type of element" by index of type int;
            "append" get a tuple "type of element" and return index of that new row.
        "recipient" is a linked list with two children for all element or None.
        "__getitem__(None)" must return (None, 0, None, None)
        """
        self.base = base
        self.recipient = recipient

    def add(self, base_idx):
        runner = 0 if len(self.recipient) else None

        idx_ls = []

        if runner is None:
            self.recipient.append((
                base_idx,
                1,
                None,
                None,
            ))
            return

        runner_vals = self.recipient[runner]

        while self.recipient[runner][1]:
            idx_ls.append(runner)
            if self.base[base_idx][1] > self.base[runner_vals[0]][1]:
                right = runner_vals[3]

                if right is None:
                    rec_idx = self.recipient.append((
                        base_idx,
                        1,
                        None,
                        None,
                    ))
                    self.recipient[runner] = (
                        runner_vals[0],  # base_idx
                        runner_vals[1] + 1,  # weight
                        runner_vals[2],  # left inner idx
                        rec_idx,  # right inner idx
                    )
                    break
                else:
                    self.recipient[runner] = (
                        runner_vals[0],
                        runner_vals[1] + 1,
                        runner_vals[2],
                        runner_vals[3],
                    )

                    runner = right
                    runner_vals = self.recipient[runner]
            else:
                left = runner_vals[2]

                if left is None:
                    rec_idx = self.recipient.append((
                        base_idx,
                        1,
                        None,
                        None,
                    ))
                    self.recipient[runner] = (
                        runner_vals[0],  # base_idx
                        runner_vals[1] + 1,  # weight
                        rec_idx,  # left inner idx
                        runner_vals[3],  # right inner idx
                    )
                    break
                else:
                    self.recipient[runner] = (
                        runner_vals[0],
                        runner_vals[1] + 1,
                        runner_vals[2],
                        runner_vals[3],
                    )

                    runner = left
                    runner_vals = self.recipient[runner]

        self.ballance(idx_ls)

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

        idx_vals = self.recipient[idx]

        left_vals = self.recipient[idx_vals[2]]
        right_vals = self.recipient[idx_vals[3]]
        if self.height(right_vals[1]) - self.height(left_vals[1]) == 2:
            right_left_idx = right_vals[2]
            right_right_idx = right_vals[3]
            right_left_vals = self.recipient[right_left_idx]
            right_right_vals = self.recipient[right_right_idx]
            if self.height(right_left_vals[1]) > self.height(right_right_vals[1]):
                self.big_left_rotation(idx, prev_idx)
            elif self.height(right_left_vals[1]) < self.height(right_right_vals[1]):
                self.small_left_rotation(idx, prev_idx)
        elif self.height(left_vals[1]) - self.height(right_vals[1]) == 2:
            left_left_idx = left_vals[2]
            left_right_idx = left_vals[3]
            left_left_vals = self.recipient[left_left_idx]
            left_right_vals = self.recipient[left_right_idx]
            if self.height(left_right_vals[1]) > self.height(left_left_vals[1]):
                self.big_right_rotation(idx, prev_idx)
            elif self.height(left_right_vals[1]) < self.height(left_left_vals[1]):
                self.small_right_rotation(idx, prev_idx)
        else:
            pass


    def big_left_rotation(self, idx, prev_idx):
        idx_vals = self.recipient[idx]

        left_vals = self.recipient[idx_vals[2]]
        right_vals = self.recipient[idx_vals[3]]

        right_left_vals = self.recipient[right_vals[2]]
        right_right_vals = self.recipient[right_vals[3]]

        right_left_left_vals = self.recipient[right_left_vals[2]]

        # prev
        prev_vals = self.recipient[prev_idx]
        if prev_vals[2] == idx:
            self.recipient[prev_idx] = (
                prev_vals[0],
                prev_vals[1],
                right_vals[2],
                prev_vals[3],
            )
        else:
            self.recipient[prev_idx] = (
                prev_vals[0],
                prev_vals[1],
                prev_vals[2],
                right_vals[2],
            )

        # a
        self.recipient[idx] = (
            idx_vals[0],
            idx_vals[1] - right_vals[1] + right_left_left_vals[1],
            idx_vals[2],
            right_left_vals[2],
        )

        # b
        self.recipient[idx_vals[3]] = (
            right_vals[0],
            right_vals[1] - right_left_left_vals[1] - 1,
            right_left_vals[3],
            right_vals[3]
        )

        # c
        self.recipient[right_vals[2]] = (
            right_vals[2],
            right_left_vals[1] + right_right_vals[1] + left_vals[1] + 2,
            idx,
            idx_vals[3],
        )

    def small_left_rotation(self, idx, prev_idx):
        prev_vals = self.recipient[prev_idx]

        idx_vals = self.recipient[idx]

        left_vals = self.recipient[idx_vals[2]]
        right_vals = self.recipient[idx_vals[3]]

        right_right_vals = self.recipient[right_vals[3]]

        # prev
        if prev_vals[2] == idx:
            self.recipient[prev_idx] = (
                prev_vals[0],
                prev_vals[1],
                idx_vals[3],
                prev_vals[3],
            )
        else:
            self.recipient[prev_idx] = (
                prev_vals[0],
                prev_vals[1],
                prev_vals[2],
                idx_vals[3],
            )

        a_0 = idx_vals[0]
        a_1 = idx_vals[1] - right_right_vals[1] - 1
        a_2 = idx_vals[2]
        a_3 = right_vals[2]
        a_idx = idx

        b_0 = right_vals[0]
        b_1 = right_vals[1] + left_vals[1] + 1
        b_2 = idx
        b_3 = right_vals[3]
        b_idx = idx_vals[3]

        # a
        self.recipient[a_idx] = (
            a_0,
            a_1,
            a_2,
            a_3
        )
        # b
        self.recipient[b_idx] = (
            b_0,
            b_1,
            b_2,
            b_3
        )

    def small_right_rotation(self, idx, prev_idx):
        idx_vals = self.recipient[idx]

        # prev
        prev_vals = self.recipient[prev_idx]
        if prev_vals[2] == idx:
            self.recipient[prev_idx] = (
                prev_vals[0],
                prev_vals[1],
                idx_vals[2],
                prev_vals[3],
            )
        else:
            self.recipient[prev_idx] = (
                prev_vals[0],
                prev_vals[1],
                prev_vals[2],
                idx_vals[2],
            )

        left_vals = self.recipient[idx_vals[2]]
        right_vals = self.recipient[idx_vals[3]]

        left_left_vals = self.recipient[left_vals[2]]

        a_0 = idx_vals[0]
        a_1 = idx_vals[1] - left_left_vals[1] - 1
        a_2 = left_vals[3]
        a_3 = idx_vals[3]
        a_idx = idx

        b_0 = left_vals[0]
        b_1 = left_vals[1] + right_vals[1] + 1
        b_2 = left_vals[2]
        b_3 = idx
        b_idx = idx_vals[2]

        # a
        self.recipient[a_idx] = (
            a_0,
            a_1,
            a_2,
            a_3
        )
        # b
        self.recipient[b_idx] = (
            b_0,
            b_1,
            b_2,
            b_3
        )

    def big_right_rotation(self, idx, prev_idx):
        # prev
        prev_vals = self.recipient[prev_idx]

        idx_vals = self.recipient[idx]

        left_vals = self.recipient[idx_vals[2]]
        right_vals = self.recipient[idx_vals[3]]

        left_left_vals = self.recipient[left_vals[2]]
        left_right_vals = self.recipient[left_vals[3]]

        left_right_right_vals = self.recipient[left_right_vals[3]]

        if prev_vals[2] == idx:
            self.recipient[prev_idx] = (
                prev_vals[0],
                prev_vals[1],
                left_vals[3],
                prev_vals[3]
            )
        else:
            self.recipient[prev_idx] = (
                prev_vals[0],
                prev_vals[1],
                prev_vals[2],
                left_vals[3],
            )

        a_0 = idx_vals[0]
        a_1 = idx_vals[1] - left_vals[1] + left_right_right_vals[1]
        a_2 = left_right_vals[3]
        a_3 = idx_vals[3]
        a_idx = idx

        b_0 = left_vals[0]
        b_1 = left_vals[1] - left_right_right_vals[1] - 1
        b_2 = left_vals[2]
        b_3 = left_right_vals[2]
        b_idx = idx_vals[2]

        c_0 = left_vals[3]
        c_1 = left_right_vals[1] + left_left_vals[1] + right_vals[1] + 2
        c_2 = idx_vals[2]
        c_3 = idx
        c_idx = left_vals[3]

        # a
        self.recipient[a_idx] = (
            a_0,
            a_1,
            a_2,
            a_3
        )
        # b
        self.recipient[b_idx] = (
            b_0,
            b_1,
            b_2,
            b_3
        )
        # c
        self.recipient[c_idx] = (
            c_0,
            c_1,
            c_2,
            c_3
        )

    def check_no_loop(self):
        """
        If "recipient" have "__len__".
        """

        s = set(range(1, len(self.recipient)))
        s_res = set()
        for i in range(len(self.recipient)):
            (
                _,
                _,
                left,
                right
            ) = self.recipient[i]
            if left is not None:
                if left in s_res:
                    raise Exception('Check_no_loop fail: 2 times "{}"'.format(left))
                if left is not None:
                    s_res.add(left)
            if right is not None:
                if right in s_res:
                    raise Exception('Check_no_loop fail: 2 times "{}"'.format(right))
                if right is not None:
                    s_res.add(right)

        assert s == s_res, (s - s_res, s_res - s)

    def get_left(self, val):
        if len(self.recipient) == 0:
            raise IndexError("Length of tree is zero.")
        idx_prev = None
        idx = 0

        if val is None:
            while True:
                idx_vals = self.recipient[idx]
                if idx_vals[2] is None:
                    return idx
                idx = idx_vals[2]

        while True:
            idx_vals = self.recipient[idx]
            if val <= self.base[idx_vals[0]][1]:
                idx_prev = idx
                idx = idx_vals[2]
                if idx is None:
                    return self.recipient[idx_prev][0]
            else:
                idx = idx_vals[3]
                if idx is None:
                    return self.recipient[idx_prev][0]

    def get_right(self, val):
        idx_prev = None
        idx = 0

        if val is None:
            while True:
                idx_vals = self.recipient[idx]
                if idx_vals[3] is None:
                    return idx
                idx = idx_vals[3]

        while True:
            idx_vals = self.recipient[idx]
            if val >= self.base[idx_vals[0]][1]:
                idx_prev = idx
                idx = idx_vals[3]
                if idx is None:
                    return idx_prev
            else:
                idx = idx_vals[2]
                if idx is None:
                    return idx_prev

    def _generator(self, left, right, idx):
        idx_vals = self.recipient[idx]
        vals = self.base[idx_vals[0]]
        val = vals[1]
        if idx_vals[2] is not None and (
            left is None
            or val >= left
        ):
            yield from self._generator(left, right, idx_vals[2])
        if (
            left is None
            or val >= left
        ) and (
            right is None
            or val <= right
        ):
            yield idx
        if idx_vals[3] is not None and (
            right is None
            or val <= right
        ):
            yield from self._generator(left, right, idx_vals[3])


    def generator(self, left=None, right=None):
        yield from self._generator(left, right, 0)
