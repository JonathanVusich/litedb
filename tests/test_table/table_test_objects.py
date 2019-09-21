class BadIndex:

    def __init__(self, integer: int):
        self.x = integer


class BadObject:

    def __init__(self, integer: int):
        self.bad_index = BadIndex(integer)


class BadAndGoodObject:

    def __init__(self, integer: int):
        self.bad_index = BadIndex(integer)
        self.good_index = integer


class GoodIndex:

    def __init__(self, integer: int):
        self.x = integer

    def __hash__(self):
        return hash((self.x,))

    def __eq__(self, other):
        if not isinstance(other, GoodIndex):
            raise NotImplementedError
        return self.x == other.x

    def __lt__(self, other):
        if not isinstance(other, GoodIndex):
            raise NotImplementedError
        return self.x < other.x

    def __gt__(self, other):
        if not isinstance(other, GoodIndex):
            raise NotADirectoryError
        return self.x > other.x


class GoodObject:

    def __init__(self, integer: int) -> None:
        self.good_index = GoodIndex(integer)

    def __eq__(self, other):
        if isinstance(other, GoodObject):
            return self.good_index == other.good_index
        raise NotImplementedError


class StandardTableObject:

    def __init__(self, x: int, y: int) -> None:
        self.x = x
        self.y = y
