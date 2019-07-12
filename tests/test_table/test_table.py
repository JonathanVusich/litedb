import pytest

from autodb.errors import InvalidRange
from autodb.table import MemoryTable
from .table_test_objects import GoodObject, StandardTableObject, GoodIndex


def test_table_init():
    table = MemoryTable()
    assert len(table.unused_indexes) == 0
    assert len(table) == 0


def test_table_insert_objects():
    table = MemoryTable()

    for i in range(10):
        table._insert(StandardTableObject(i, -i))
    assert len(table.table) == 10


def test_table_retrieve_well_formed_queries():
    table = MemoryTable()

    for i in range(10):
        table._insert(StandardTableObject(i, -i))

    # test x queries

    indexes = table.index_manager.retrieve(x=9)
    assert len(indexes) == 1
    sto = table.table[indexes.pop()]
    assert isinstance(sto, StandardTableObject)
    assert sto.x == 9

    assert not table.index_manager.retrieve(x=10)

    indexes = table.index_manager.retrieve(x=(1, 3))
    assert len(indexes) == 3
    for x, item in enumerate(indexes):
        sto = table.table[item]
        assert isinstance(sto, StandardTableObject)
        assert sto.x == x + 1
        assert sto.y == -(x + 1)

    # test y queries

    indexes = table.index_manager.retrieve(y=-9)
    assert len(indexes) == 1
    sto = table.table[indexes.pop()]
    assert isinstance(sto, StandardTableObject)
    assert sto.y == -9

    assert not table.index_manager.retrieve(y=-10)

    indexes = table.index_manager.retrieve(y=(-3, -1))
    assert len(indexes) == 3
    for x, item in enumerate(indexes):
        sto = table.table[item]
        assert isinstance(sto, StandardTableObject)
        assert sto.x == x + 1
        assert sto.y == -(x + 1)

    # verify no references returned
    item = list(table.retrieve(x=1))[0]
    item.x = 2
    assert list(table.retrieve(x=1))[0].x == 1

    # verify that no kwargs returns all objects
    items = list(table.retrieve_all())
    assert len(items) == 10
    for item in items:
        assert isinstance(item, StandardTableObject)

    # Check multiple queries
    items = list(table.retrieve(x=9, y=-9))
    assert len(items) == 1
    assert items[0].x == 9
    assert items[0].y == -9

    assert not list(table.retrieve(x=9, y=9))


def test_table_retrieve_bad_queries():
    table = MemoryTable()

    for i in range(10):
        table._insert(StandardTableObject(i, -i))

    with pytest.raises(IndexError):
        table.index_manager.retrieve(z=12)

    with pytest.raises(InvalidRange):
        table.index_manager.retrieve(x=(1, 3, 5))

    with pytest.raises(ValueError):
        table.index_manager.retrieve(x=b"test")

    with pytest.raises(ValueError):
        table.index_manager.retrieve(x=(b"test", b"test2"))

    with pytest.raises(ValueError):
        table.index_manager.retrieve(x=(1, b"test"))


def test_table_unused_indexes():
    table = MemoryTable()
    table._insert(GoodObject(1))
    table._insert(GoodObject(2))
    table.delete(good_index=GoodIndex(1))
    assert table.unused_indexes == {0}
    table._insert(GoodObject(1))
    assert table.unused_indexes == set()
