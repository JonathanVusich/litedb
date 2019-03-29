class TestObjectNoClassVars:

    def __init__(self):
        self.int = 12
        self.bool = False
        self.float = 12.34
        self.str = "test"
        self.bytes = bytes(b"test")
        self.bytearray = bytearray(b"test")


class TestObjectClassVars:

    class_var = "class_var"

    def __init__(self):
        self.int = 12
        self.bool = False
        self.float = 12.34
        self.str = "test"
        self.bytes = bytes(b"test")
        self.bytearray = bytearray(b"test")
