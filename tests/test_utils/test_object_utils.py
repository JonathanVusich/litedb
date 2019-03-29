from autodb.utils.object_utils import retrieve_object_indexes
from .objects import TestObjectNoClassVars, TestObjectClassVars

def test_retrieve_object_indexes():
    # Test all basic types
    test_object = TestObjectNoClassVars()
    indexes = retrieve_object_indexes(test_object)
    assert indexes == {'int': 12, 'bool': False, 'float': 12.34, 'str': 'test', 'bytes': b'test',
                       'bytearray': bytearray(b'test')}

    # Test with class variables
    test_object = TestObjectClassVars()
    indexes = retrieve_object_indexes(test_object)
    assert indexes == {'int': 12, 'bool': False, 'float': 12.34, 'str': 'test', 'bytes': b'test',
                       'bytearray': bytearray(b'test')}
