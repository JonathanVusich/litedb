from autodb.shard.manager import ShardManager
from sortedcontainers import SortedDict
from autodb.utils.serialization import serialize, deserialize, dump
import pytest


@pytest.fixture()
def shard_manager(tmpdir):
    temp_directory = tmpdir.mkdir("table")
    table_dir = str(temp_directory)
    paths = {0: str(temp_directory.join("shard0"))}
    s = serialize
    shard = [s(item) for item in ([None] * 512)]
    dump(temp_directory.join("shard0"), shard)
    return ShardManager(table_dir)


@pytest.fixture(scope='module')
def small_vals():
    vals = SortedDict()
    for x in range(512):
        vals[x] = x
    return vals


@pytest.fixture(scope='module')
def large_vals():
    vals = SortedDict()
    for x in range(1024):
        vals[x] = x
    return vals


@pytest.fixture(scope='module')
def odd_vals():
    vals = SortedDict()
    for x in filter(lambda x: x % 2 == 0, range(1024)):
        vals[x] = x
    return vals


def test_insert(shard_manager, small_vals):
    ds = deserialize
    shard_manager.insert(small_vals)
    for x, value in enumerate(shard_manager.buffer[0]):
        assert small_vals[x] == ds(value)


def test_insert_multiple_shards(shard_manager, large_vals):
    ds = deserialize
    shard_manager.insert(large_vals)
    for x, value in enumerate(shard_manager.buffer[0]):
        assert large_vals[x] == ds(value)
    for x, value in enumerate(shard_manager.buffer[1]):
        assert large_vals[x+512] == ds(value)


def test_retrieve(shard_manager, odd_vals):
    shard_manager.insert(odd_vals)
    indexes = list(filter(lambda x : x % 2 == 0, range(1024)))
    values = list(shard_manager.retrieve(indexes))
    assert values == indexes


def test_delete(shard_manager, large_vals):
    shard_manager.insert(large_vals)
    shard_manager.delete(range(1024))
    assert list(shard_manager.retrieve_all()) == []


def test_retrieve_all(shard_manager, large_vals):
    shard_manager.insert(large_vals)
    for x, value in enumerate(shard_manager.retrieve_all()):
        assert large_vals[x] == value


