import pytest
from hypothesis import given, settings, assume
from hypothesis.strategies import text, binary, integers
from autodb.index import Index

test_index = Index(str)


@given(
    value=text(),
    index=integers()
)
@settings(max_examples=100)
def test_insert(value, index):
    test_index.create(value, index)
    assert 0 < len(test_index.indexes.items()) < 101


@given(
    value=text()
)
@settings(max_examples=100)
def test_retrieve(value):
    test_index.retrieve(value)
    assert 0 < len(test_index.indexes.items()) < 101


@given(
    old_value=text(),
    old_index=integers(),
    new_value=text(),
    new_index=integers()
)
@settings(max_examples=100)
def test_update(old_value, old_index, new_value, new_index):
    old_entry = test_index.retrieve(old_value)
    if old_entry:
        assume(old_index in old_entry)
    test_index.update(old_value, old_index, new_value, new_index)
    assert 0 < len(test_index.indexes.items()) < 101


@given(
    value=text(),
    index=integers()
)
@settings(max_examples=100)
def test_destroy(value, index):
    entry = test_index.retrieve(value)
    if entry:
        assume(index in entry)
    test_index.destroy(value, index)
    assert 0 <= len(test_index.indexes.items()) < 101





