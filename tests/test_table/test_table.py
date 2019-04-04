import pytest
from autodb.table import Table
from ..test_utils.object_utils_test_objects import ObjectNoClassVars, ObjectClassVars, ObjectUnderscoreVars
from .table_test_objects import BadTableObject, GoodTableObject, StandardTableObject


def test_table_init():
    table = Table()
    assert len(table.unused_indexes) == 0
    assert len(table) == 0
    assert len(table.index_blacklist) == 0


def test_table_insert_indexes():
    table = Table()

    bad_object1 = BadTableObject(12)
    bad_object2 = BadTableObject(13)

    table._index_item(bad_object1, 0)
    assert len(table.index_map) == 1
    table._index_item(bad_object2, 1)
    assert len(table.index_map) == 0
    assert "bad_index" in table.index_blacklist

    good_object1 = GoodTableObject(12)
    good_object2 = GoodTableObject(13)

    table = Table()

    table._index_item(good_object1, 0)
    assert len(table.index_map) == 1
    table._index_item(good_object2, 1)
    assert len(table.index_map) == 1
    assert "bad_index" not in table.index_blacklist


def test_table_insert_objects():
    table = Table()

    for i in range(10):
        table.insert(StandardTableObject(i, -i))
    assert len(table.table) == 10
    assert len(table.index_map) == 2
    assert len(table.index_blacklist) == 0


def test_table_retrieve_objects():
    table = Table()

    for i in range(10):
        table.insert(StandardTableObject(i, -i))

    items = table.retrieve(x=9)
    assert len(items) == 1
    assert isinstance(items[0], StandardTableObject)
    assert items[0].x == 9

    assert not table.retrieve(x=10)

    items = table.retrieve(x=(1, 3))
    assert len(items) == 3
    for x, item in enumerate(items):
        assert isinstance(item, StandardTableObject)
        assert item.x == x + 1
        assert item.y == -(x + 1)


