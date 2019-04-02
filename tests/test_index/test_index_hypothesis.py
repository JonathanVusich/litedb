import pytest
from hypothesis import given, settings, assume
from hypothesis.strategies import text, integers

from autodb.index import Index

test_index = Index(str)


@given(
    value=text(),
    index=integers()
)
@settings(max_examples=1000)
def test_insert(value, index):
    test_index.add(value, index)
    assert index in test_index.retrieve(value)


@given(
    value=text()
)
@settings(max_examples=100)
def test_retrieve(value):
    assume(value in test_index.indexes or value is None)
    value = test_index.retrieve(value)
    if value is not None:
        assert isinstance(value, set)
        assert len(value) > 0

@given(
    value=text(),
    index=integers()
)
@settings(max_examples=100)
def test_destroy(value, index):
    test_index = Index(str)
    for i in range(100):
        test_index.add(str(i), i)
    entry = test_index.retrieve(value)
    if entry is not None:
        if index in entry:
            test_index.destroy(value, index)
            new_entry = test_index.retrieve(value)
            if new_entry is not None:
                assert entry - new_entry == {index}
        else:
            with pytest.raises(KeyError):
                test_index.destroy(value, index)
