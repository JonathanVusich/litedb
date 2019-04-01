import pytest
from autodb.index import Index
from sortedcontainers import SortedDict


def test_index_init():
    index = Index(bytes)
    assert index.trie_type == bytes
    assert isinstance(index.indexes, SortedDict)
    assert len(index) == 0


def test_insert():
    index = Index(bytes)
    index.add(b"23", 1)
    assert len(index) == 1
    assert index.indexes[b"23"] == {1}
    index.add(b"23", 2)
    assert len(index) == 1
    assert index.indexes[b"23"] == {1, 2}
    index.add(b"23", 2)
    assert len(index) == 1
    assert index.indexes[b"23"] == {1, 2}
    index.add(None, 1)
    assert len(index) == 2
    assert index.none_indexes == {1}


def test_bad_insert():
    index = Index(bytes)
    index.add(b"23", 1)
    with pytest.raises(ValueError):
        index.add("23", 2)


def test_retrieve():
    index = Index(bytes)
    index.add(b"23", 1)
    assert index.retrieve(b"23") == {1}
    index.add(b"24", 2)
    assert index.retrieve(b"24") == {2}
    index.add(None, 3)
    assert index.retrieve(None) == {3}


def test_bad_retrieve():
    index = Index(bytes)
    index.add(b"23", 1)
    with pytest.raises(ValueError):
        index.retrieve("23")


def test_retrieve_range_bytes():
    index = Index(bytes)
    index.add(None, 0)
    index.add(b"1", 1)
    index.add(b"2", 2)
    index.add(b"3", 3)
    index.add(b"4", 4)
    range = index.retrieve_range(None, b"3")
    assert range == {0, 1, 2, 3}
    range = index.retrieve_range(b"1", b"4")
    assert range == {1, 2, 3, 4}
    range = index.retrieve_range(b"5", b"7")
    assert range is None


def test_retrieve_range_int():
    index = Index(int)
    index.add(None, 0)
    index.add(1, 1)
    index.add(2, 2)
    index.add(3, 3)
    index.add(4, 4)
    range = index.retrieve_range(None, 3)
    assert range == {0, 1, 2, 3}
    range = index.retrieve_range(1, 4)
    assert range == {1, 2, 3, 4}
    range = index.retrieve_range(5, 7)
    assert range is None


def test_destroy():
    index = Index(bytes)
    index.add(b"23", 1)
    index.destroy(b"23", 1)
    assert len(index) == 0
    assert not index.retrieve(b"23")
