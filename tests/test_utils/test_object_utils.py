from autodb.utils.object_utils import retrieve_object_indexes
from .objects import ObjectNoClassVars, ObjectClassVars, ObjectUnderscoreVars

import time


def test_retrieve_object_indexes():
    # Test all basic types
    test_object = ObjectNoClassVars()
    indexes = retrieve_object_indexes(test_object)
    assert indexes == {'int': 12, 'bool': False, 'float': 12.34, 'str': 'test', 'bytes': b'test',
                       'bytearray': bytearray(b'test')}

    # Test with class variables
    test_object = ObjectClassVars()
    indexes = retrieve_object_indexes(test_object)
    assert indexes == {'int': 12, 'bool': False, 'float': 12.34, 'str': 'test', 'bytes': b'test',
                       'bytearray': bytearray(b'test')}

    # Test object with underscore variables
    test_object = ObjectUnderscoreVars()
    indexes = retrieve_object_indexes(test_object)
    assert not indexes

    start = time.perf_counter()
    for i in range(1000000):
        retrieve_object_indexes(test_object)
    print(time.perf_counter() - start)
