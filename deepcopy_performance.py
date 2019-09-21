import sys


class A:
    def __init__(self):
        self.v = 10
        self.z = [2, 3, 4]

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result


class B:
    def __init__(self):
        self.v = 10
        self.z = [2, 3, 4]


def main():
    shard = [None] * 512
    print(sys.getsizeof(shard))


if __name__ == "__main__":
    main()
