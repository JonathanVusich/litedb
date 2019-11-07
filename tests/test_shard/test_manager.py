import pytest
from sortedcontainers import SortedDict

from litedb import Config
from litedb.shard.manager import ShardManager
from litedb.utils.serialization import serialize, dump_object


@pytest.fixture()
def shard_manager(tmpdir):
    temp_directory = tmpdir.mkdir("table")
    table_dir = str(temp_directory)
    s = serialize
    shard = [s(item) for item in ([None] * 512)]
    dump_object(temp_directory.join("shard0"), shard)
    return ShardManager(table_dir, Config())


@pytest.fixture(scope='module')
def small_vals():
    vals = (x for x in range(512))
    items = (x for x in range(512))
    indexed_items = [(index, item) for index, item in zip(vals, items)]
    return indexed_items


@pytest.fixture(scope='module')
def large_vals():
    vals = (x for x in range(1024))
    items = (x for x in range(1024))
    indexed_items = [(index, item) for index, item in zip(vals, items)]
    return indexed_items


@pytest.fixture(scope='module')
def odd_vals():
    vals = (x for x in range(1024) if x % 2 == 0)
    items = (x for x in range(1024) if x % 2 == 0)
    indexed_items = [(index, item) for index, item in zip(vals, items)]
    return indexed_items


def test_insert(shard_manager, small_vals):
    for index, item in small_vals:
        shard_manager.insert(item, index)
    for x, value in enumerate(shard_manager.buffer[0]):
        assert x == value


def test_insert_multiple_shards(shard_manager, large_vals):
    for index, item in large_vals:
        shard_manager.insert(item, index)
    for x, value in enumerate(shard_manager.buffer[0]):
        assert x == value
    for x, value in enumerate(shard_manager.buffer[1]):
        assert x + 512 == value


def test_retrieve(shard_manager, odd_vals):
    for index, item in odd_vals:
        shard_manager.insert(item, index)
    indexes = list(filter(lambda x: x % 2 == 0, range(1024)))
    values = list(shard_manager.retrieve(indexes))
    assert values == indexes


def test_delete(shard_manager, large_vals):
    for index, item in large_vals:
        shard_manager.insert(item, index)
    shard_manager.delete(range(1024))
    assert list(shard_manager.retrieve_all()) == []


def test_retrieve_all(shard_manager, large_vals):
    for index, item in large_vals:
        shard_manager.insert(item, index)
    for x, value in enumerate(shard_manager.retrieve_all()):
        assert x == value
