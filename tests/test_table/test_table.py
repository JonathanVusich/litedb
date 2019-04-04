import pytest
from autodb.table import Table
from ..test_utils.object_utils_test_objects import ObjectNoClassVars, ObjectClassVars, ObjectUnderscoreVars
from .table_test_objects import BadObject, GoodObject, StandardTableObject, GoodIndex, BadAndGoodObject


def test_table_init():
    table = Table()
    assert len(table.unused_indexes) == 0
    assert len(table) == 0
    assert len(table.index_blacklist) == 0


def test_table_insert_indexes():
    table = Table()

    bad_object1 = BadObject(12)
    bad_object2 = BadObject(13)

    table._index_item(bad_object1, 0)
    assert len(table.index_map) == 1
    table._index_item(bad_object2, 1)
    assert len(table.index_map) == 0
    assert "bad_index" in table.index_blacklist

    good_object1 = GoodObject(12)
    good_object2 = GoodObject(13)

    table = Table()

    table._index_item(good_object1, 0)
    assert len(table.index_map) == 1
    table._index_item(good_object2, 1)
    assert len(table.index_map) == 1
    assert "bad_index" not in table.index_blacklist


def test_table_unindex_item():
    table = Table()

    table.insert(StandardTableObject(1, 2))
    table.insert(StandardTableObject(3, 4))
    assert len(table.index_map["x"]) == 2
    assert len(table.index_map["y"]) == 2

    table._unindex_item(StandardTableObject(1, 2), 0)
    assert len(table.index_map["x"]) == 1
    assert len(table.index_map["y"]) == 1

    table = Table()

    table.insert(BadAndGoodObject(1))
    table.insert(BadAndGoodObject(3))
    assert len(table.index_map) == 1
    assert len(table.index_map["good_index"]) == 2

    assert len(table.index_blacklist) == 1
    assert "bad_index" in table.index_blacklist

    table._unindex_item(BadAndGoodObject(1), 0)
    table._unindex_item(BadAndGoodObject(3), 1)

    assert "bad_index" in table.index_blacklist
    assert len(table.index_map) == 1
    assert len(table.index_map["good_index"]) == 0


def test_table_insert_objects():
    table = Table()

    for i in range(10):
        table.insert(StandardTableObject(i, -i))
    assert len(table.table) == 10
    assert len(table.index_map) == 2
    assert len(table.index_blacklist) == 0


def test_table_retrieve_well_formed_queries():
    table = Table()

    for i in range(10):
        table.insert(StandardTableObject(i, -i))

    # test x queries

    indexes = table._retrieve(x=9)
    assert len(indexes) == 1
    sto = table.table[indexes.pop()]
    assert isinstance(sto, StandardTableObject)
    assert sto.x == 9

    assert not table._retrieve(x=10)

    indexes = table._retrieve(x=(1, 3))
    assert len(indexes) == 3
    for x, item in enumerate(indexes):
        sto = table.table[item]
        assert isinstance(sto, StandardTableObject)
        assert sto.x == x + 1
        assert sto.y == -(x + 1)

    # test y queries

    indexes = table._retrieve(y=-9)
    assert len(indexes) == 1
    sto = table.table[indexes.pop()]
    assert isinstance(sto, StandardTableObject)
    assert sto.y == -9

    assert not table._retrieve(y=-10)

    indexes = table._retrieve(y=(-3, -1))
    assert len(indexes) == 3
    for x, item in enumerate(indexes):
        sto = table.table[item]
        assert isinstance(sto, StandardTableObject)
        assert sto.x == x + 1
        assert sto.y == -(x + 1)

    # verify no references returned
    item = table.retrieve(x=1)[0]
    item.x = 2
    assert table.retrieve(x=1)[0].x == 1


def test_table_retrieve_bad_queries():
    table = Table()

    for i in range(10):
        table.insert(StandardTableObject(i, -i))

    with pytest.raises(IndexError):
        table._retrieve(z=12)

    with pytest.raises(IndexError):
        table._retrieve(x=(1, 3, 5))

    with pytest.raises(IndexError):
        table._retrieve(x=b"test")

    with pytest.raises(IndexError):
        table._retrieve(x=(b"test", b"test2"))

    with pytest.raises(IndexError):
        table._retrieve(x=(1, b"test"))


def test_table_unused_indexes():
    table = Table()
    table.insert(GoodObject(1))
    table.insert(GoodObject(2))
    table.delete(good_index=GoodIndex(1))
    assert table.unused_indexes == [0]
    table.insert(GoodObject(1))
    assert table.unused_indexes == []







