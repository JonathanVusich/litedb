
class BadIndexObject:

    def __init__(self, integer: int):
        self.x = integer


class BadTableObject:

    def __init__(self, integer: int):
        self.bad_index = BadIndexObject(integer)


class GoodIndexObject:

    def __init__(self, integer: int):
        self.x = integer

    def __hash__(self):
        return hash((self.x,))

    def __lt__(self, other):
        if not isinstance(other, GoodIndexObject):
            raise NotImplementedError
        return self.x < other.x

    def __gt__(self, other):
        if not isinstance(other, GoodIndexObject):
            raise NotADirectoryError
        return self.x > other.x


class GoodTableObject:

    def __init__(self, integer: int) -> None:
        self.good_index = GoodIndexObject(integer)


class StandardTableObject:

    def __init__(self, x: int, y: int) -> None:
        self.x = x
        self.y = y
