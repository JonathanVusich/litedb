from decimal import Decimal


class ObjectNoClassVars:

    def __init__(self):
        self.int = 12
        self.bool = False
        self.float = 12.34
        self.str = "test"
        self.bytes = bytes(b"test")
        self.bytearray = bytearray(b"test")
        self.complex = complex(1234)
        self.decimal = Decimal("1234")


class ObjectClassVars:

    class_var = "class_var"

    def __init__(self):
        self.int = 12
        self.bool = False
        self.float = 12.34
        self.str = "test"
        self.bytes = bytes(b"test")
        self.bytearray = bytearray(b"test")
        self.complex = complex(1234)
        self.decimal = Decimal("1234")


class ObjectUnderscoreVars:

    def __init__(self):
        self._int = 12
        self._bool = False
        self._float = 12.34
        self._str = "test"
        self._bytes = bytes(b"test")
        self._bytearray = bytearray(b"test")
        self._noindex = Decimal("1234")
