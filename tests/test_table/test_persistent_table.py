import pytest
from sortedcontainers import SortedList

from autodb.table.persistent_table import PersistentTable
from tests.test_table.table_test_objects import GoodObject, GoodIndex


@pytest.fixture
def table_dir(tmpdir):
    return tmpdir.mkdir("table")


@pytest.fixture
def table(table_dir):
    return PersistentTable.new(table_dir, GoodObject)


@pytest.fixture
def test_objects():
    return [GoodObject(x) for x in range(1000)]


def test_new_table(table_dir):
    table = PersistentTable.new(table_dir, table_type=GoodObject)
    assert table.directory == table_dir
    assert table.index_path == table_dir.join("index")
    assert table.info_path == table_dir.join("info")
    assert table.shard_manager is not None
    assert table.index_manager is not None
    assert table.table_type == GoodObject
    assert table.size == 0
    assert table.unused_indexes == SortedList()


def test_persist(table, table_dir):
    table.unused_indexes.add(12)
    table.unused_indexes.add(13)
    table.size = 1
    table.persist()
    del table
    table = PersistentTable.from_file(table_dir)
    assert table.size == 1
    assert table.table_type == GoodObject
    assert table.unused_indexes.pop() == 13
    assert table.unused_indexes.pop() == 12


def test_insert(table, table_dir):
    good_object = GoodObject(12)
    good_object2 = GoodObject(13)
    table.insert(good_object)
    table.insert(good_object2)
    del table
    table = PersistentTable.from_file(table_dir)
    assert table.table_type == GoodObject
    assert table.unused_indexes == SortedList()
    assert table.size == 2


def test_batch_insert(table, table_dir):
    objects_to_insert = [GoodObject(12), GoodObject(13)]
    table.batch_insert(objects_to_insert)
    assert table.size == 2
    assert table.unused_indexes == SortedList()


def test_delete_all(table, test_objects):
    table.batch_insert(test_objects)
    table.clear()
    assert table.size == 0
    assert table.unused_indexes == []
    assert list(table.retrieve_all()) == []


def test_delete_some(table, test_objects):
    table.batch_insert(test_objects)
    table.delete(good_index=(GoodIndex(0), GoodIndex(499)))
    assert table.size == 500
    assert table.unused_indexes == [x for x in range(500)]
    assert list(table.retrieve_all()) == test_objects[500:]


def test_retrieve_all(table, test_objects):
    table.batch_insert(test_objects)
    assert list(table.retrieve_all()) == test_objects


def test_retrieve_some(table, test_objects):
    table.batch_insert(test_objects)
    assert list(table.retrieve(good_index=(GoodIndex(0), GoodIndex(10)))) == test_objects[:11]
