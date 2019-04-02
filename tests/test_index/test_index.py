import pytest
from sortedcontainers import SortedDict

from autodb.index import Index


def test_index_init():
    index = Index()
    assert isinstance(index.indexes, SortedDict)
    assert len(index) == 0


def test_insert():
    index = Index()
    index.add(b"23", 1)
    assert len(index) == 1
    assert index.indexes[b"23"] == 1
    index.add(b"23", 2)
    assert len(index) == 1
    assert index.indexes[b"23"] == {1, 2}
    index.add(b"23", 3)
    assert len(index) == 1
    assert index.indexes[b"23"] == {1, 2, 3}
    index.add(None, 1)
    assert len(index) == 2
    assert index.none_indexes == {1}


def test_retrieve():
    index = Index()
    index.add(b"23", 1)
    assert index.retrieve(b"23") == {1}
    index.add(b"23", 2)
    assert index.retrieve(b"23") == {1, 2}
    index.add(b"24", 2)
    assert index.retrieve(b"24") == {2}
    index.add(None, 3)
    assert index.retrieve(None) == {3}
    assert index.retrieve(b"26") is None


def test_retrieve_range_bytes():
    index = Index()
    index.add(b"3", 3)
    index.add(b"3", 4)
    index.add(b"4", 4)
    index.add(b"5", 5)
    index.add(b"6", 6)
    range = index.retrieve_range(None, None)
    assert range is None
    range = index.retrieve_range(None, b"2")
    assert range is None
    index.add(None, 0)
    range = index.retrieve_range(None, None)
    assert range == {0}
    range = index.retrieve_range(b"3", b"4")
    assert range == {3, 4}
    range = index.retrieve_range(b"3", b"8")
    assert range == {3, 4, 5, 6}


def test_destroy():
    index = Index()
    index.add(None, 0)
    index.add(b"23", 1)
    index.add(b"23", 2)
    index.add(b"23", 3)
    index.destroy(None, 0)
    assert len(index) == 1
    index.destroy(b"23", 1)
    assert len(index) == 1
    assert index.retrieve(b"23") == {2, 3}
    index.destroy(b"23", 2)
    assert len(index) == 1
    assert index.retrieve(b"23") == {3}
    index.destroy(b"23", 3)
    assert len(index) == 0
    assert index.retrieve(b"23") is None
    with pytest.raises(KeyError):
        index.destroy(None, 0)
    with pytest.raises(KeyError):
        index.destroy(b"23", 1)
