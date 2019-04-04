import time
from autodb.index import Index
from autodb.table import Table
from tests.test_utils.object_utils_test_objects import ObjectClassVars
import sys
import inspect
from decimal import Decimal
from pcpartpicker.parts import ThermalPaste
from moneyed import Money, USD
from autodb.utils.object_utils import retrieve_possible_object_indexes

def main():
    thermal_paste = ThermalPaste("Grizzly", "Arctic", 2.3, Money("12.34", USD))

    start = time.perf_counter()
    for i in range(10000000):
        retrieve_possible_object_indexes(thermal_paste)
    print((time.perf_counter() - start))



def get_size(obj, seen=None):
    """Recursively finds size of objects in bytes"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if hasattr(obj, '__dict__'):
        for cls in obj.__class__.__mro__:
            if '__dict__' in cls.__dict__:
                d = cls.__dict__['__dict__']
                if inspect.isgetsetdescriptor(d) or inspect.ismemberdescriptor(d):
                    size += get_size(obj.__dict__, seen)
                break
    if isinstance(obj, dict):
        size += sum((get_size(v, seen) for v in obj.values()))
        size += sum((get_size(k, seen) for k in obj.keys()))
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum((get_size(i, seen) for i in obj))

    if hasattr(obj, '__slots__'):  # can have __slots__ with __dict__
        size += sum(get_size(getattr(obj, s), seen) for s in obj.__slots__ if hasattr(obj, s))

    return size


if __name__ == "__main__":
    main()
