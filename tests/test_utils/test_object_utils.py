from decimal import Decimal

from litedb.utils.index import retrieve_possible_object_indexes
from .object_utils_test_objects import ObjectNoClassVars, ObjectClassVars, ObjectUnderscoreVars


def test_retrieve_object_indexes():
    # Test all basic types
    test_object = ObjectNoClassVars()
    indexes = retrieve_possible_object_indexes(test_object)
    assert indexes == {'int': 12, 'bool': False, 'float': 12.34, 'str': 'test', 'bytes': b'test',
                       'bytearray': bytearray(b'test'), 'complex': (1234 + 0j), 'decimal': Decimal('1234')}

    # Test with class variables
    test_object = ObjectClassVars()
    indexes = retrieve_possible_object_indexes(test_object)
    assert indexes == {'int': 12, 'bool': False, 'float': 12.34, 'str': 'test', 'bytes': b'test',
                       'bytearray': bytearray(b'test'), 'complex': (1234 + 0j), 'decimal': Decimal('1234')}

    # Test object with underscore variables
    test_object = ObjectUnderscoreVars()
    indexes = retrieve_possible_object_indexes(test_object)
    assert not indexes
