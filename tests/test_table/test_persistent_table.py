import pytest
from sortedcontainers import SortedList

from litedb.table.persistent_table import PersistentTable
from litedb import Config
from tests.test_table.table_test_objects import GoodObject, GoodIndex


@pytest.fixture
def table_dir(tmpdir):
    return tmpdir.mkdir("table")


@pytest.fixture
def table(table_dir):
    return PersistentTable._new(Config(), table_dir, GoodObject)


@pytest.fixture
def test_objects():
    return [GoodObject(x) for x in range(1000)]


def test_new_table(table_dir):
    table = PersistentTable._new(Config(), table_dir, table_type=GoodObject)
    assert table._directory == table_dir
    assert table._index_path == table_dir.join("index")
    assert table._info_path == table_dir.join("info")
    assert table._shard_manager is not None
    assert table._index_manager is not None
    assert table._table_type == GoodObject
    assert len(table) == 0
    assert table._unused_indexes == SortedList()


def test_insert(table, table_dir):
    good_object = GoodObject(12)
    good_object2 = GoodObject(13)
    table._insert(good_object)
    table._insert(good_object2)
    table.commit()
    del table
    table = PersistentTable._from_file(table_dir)
    assert table._table_type == GoodObject
    assert table._unused_indexes == SortedList()
    assert len(table) == 2


def test_delete_all(table, test_objects):
    for item in test_objects:
        table._insert(item)
    table.clear()
    assert len(table) == 0
    assert table._unused_indexes == []
    assert list(table) == []


def test_delete_some(table, test_objects):
    for item in test_objects:
        table._insert(item)
    table.delete(good_index=(GoodIndex(0), GoodIndex(499)))
    assert len(table) == 500
    assert table._unused_indexes == [x for x in range(500)]
    assert list(table) == test_objects[500:]


def test_retrieve_all(table, test_objects):
    for item in test_objects:
        table._insert(item)
    assert list(table) == test_objects


def test_retrieve_some(table, test_objects):
    for item in test_objects:
        table._insert(item)
    assert list(table.retrieve(good_index=(GoodIndex(0), GoodIndex(10)))) == test_objects[:11]


def test_table_unused_indexes(table, test_objects, table_dir):
    for item in test_objects:
        table._insert(item)
    table.delete(good_index=GoodIndex(1))
    table._insert(GoodObject(1))
    table.commit()
    del table
    table = PersistentTable._from_file(table_dir)
    assert list(table.retrieve(good_index=(GoodIndex(0), GoodIndex(10)))) == test_objects[:11]

