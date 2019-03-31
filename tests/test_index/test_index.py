import pytest
from autodb.index import Index
from sortedcontainers import SortedDict


def test_index_init():
    index = Index(bytes)
    assert index.trie_type == bytes
    assert isinstance(index.indexes, SortedDict)
    assert len(index.indexes.items()) == 0


def test_insert():
    index = Index(bytes)
    index.create(b"23", 1)
    assert len(index.indexes.items()) == 1
    assert index.indexes[b"23"] == {1}
    index.create(b"23", 2)
    assert len(index.indexes.items()) == 1
    assert index.indexes[b"23"] == {1, 2}
    index.create(b"23", 2)
    assert len(index.indexes.items()) == 1
    assert index.indexes[b"23"] == {1, 2}


def test_bad_insert():
    index = Index(bytes)
    index.create(b"23", 1)
    with pytest.raises(ValueError):
        index.create("23", 2)


def test_retrieve():
    index = Index(bytes)
    index.create(b"23", 1)
    assert index.retrieve(b"23") == {1}
    index.create(b"24", 2)
    assert index.retrieve(b"24") == {2}


def test_bad_retrieve():
    index = Index(bytes)
    index.create(b"23", 1)
    with pytest.raises(ValueError):
        index.retrieve("23")


def test_update():
    index = Index(bytes)
    index.create(b"23", 1)
    index.update(b"23", 1, b"24", 2)
    assert index.retrieve(b"24") == {2}


def test_update_no_item():
    index = Index(bytes)
    index.create(b"23", 1)
    index.update(b"24", 2, b"25", 3)
    assert len(index.indexes.items()) == 1
    assert index.retrieve(b"23") == {1}
    assert not index.retrieve(b"25")


def test_bad_update():
    index = Index(bytes)
    index.create(b"23", 1)
    with pytest.raises(ValueError):
        index.update("24", 2, "25", 3)


def test_destroy():
    index = Index(bytes)
    index.create(b"23", 1)
    index.destroy(b"23", 1)
    assert len(index.indexes.items()) == 0
    assert not index.retrieve(b"23")
