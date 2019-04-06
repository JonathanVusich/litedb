from copy import deepcopy
from copy import copy
from timeit import timeit
import pickle

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
    a = A()
    b = B()
    print(timeit('deepcopy(a)', globals={'deepcopy': deepcopy, 'a': a}, number=100000))
    print(timeit('deepcopy(b)', globals={'deepcopy': deepcopy, 'b': b}, setup="from copy import deepcopy", number=100000))
    print(timeit('pickle.loads(pickle.dumps(a))', globals={'pickle': pickle, 'a': a}, number=100000))

if __name__ == "__main__":
    main()
