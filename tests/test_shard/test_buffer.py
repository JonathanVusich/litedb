from autodb.shard.buffer import ShardBuffer
from collections import deque
import pytest
import pickle


@pytest.fixture()
def buffer(tmpdir):
    temp_directory = tmpdir.mkdir("table")
    table_dir = str(temp_directory)
    paths = {0: str(temp_directory.join("shard0"))}
    with open(temp_directory.join("shard0"), "wb") as f:
        pickle.dump([None] * 512, f, pickle.HIGHEST_PROTOCOL)
    buffer = ShardBuffer(table_dir, paths)
    return buffer


@pytest.fixture()
def empty_buffer():
    return ShardBuffer("tabledir", {})


def test_buffer_init(buffer):
    assert buffer.current_shard_index == -1
    assert isinstance(buffer.table_dir, str)
    assert 0 in buffer.shard_paths
    assert buffer.mru.mru == deque([])


def test_empty_buffer_iter(empty_buffer):
    for shard in empty_buffer:
        raise AssertionError


def test_buffer_iter(buffer):
    for x, shard in enumerate(buffer):
        assert x < 1
        assert shard == [None] * 512


def test_buffer_get_item(buffer):
    empty_shard = buffer[0]
    assert len(buffer.shard_paths) == 1
    assert len(buffer.loaded_shards) == 1
    assert empty_shard == [None] * 512
    second_shard = buffer[1]
    assert len(buffer.shard_paths) == 2
    assert len(buffer.loaded_shards) == 2
    assert second_shard == [None] * 512


def test_buffer_create_new_path(buffer, tmpdir):
    assert buffer._create_new_shard_path() == str(tmpdir.join("table").join("shard1"))
    # add new shard
    buffer[1]
    assert buffer._create_new_shard_path() == str(tmpdir.join("table").join("shard2"))


def test_buffer_ensure_shard_loaded(buffer):
    first_shard = buffer[0]
    first_shard[0] = b"test"
    # fill up the buffer
    buffer._ensure_shard_loaded(1)
    buffer._ensure_shard_loaded(2)
    buffer._ensure_shard_loaded(3)
    buffer._ensure_shard_loaded(4)
    # shard should be evicted
    assert 0 not in buffer.loaded_shards
    assert 0 in buffer.shard_paths
    # get first shard
    first_shard = buffer[0]
    assert first_shard[0] == b"test"


def test_buffer_persist_shard(buffer, tmpdir):
    empty_shard = buffer[0]
    assert 0 in buffer.loaded_shards
    buffer._persist_shard(0)
    shard_dir = buffer.shard_paths[0]
    with open(shard_dir, "rb") as f:
        file_shard = pickle.load(f)
    assert empty_shard == file_shard

    empty_shard[0] = b"test"
    buffer._persist_shard(0)
    with open(shard_dir, "rb") as f:
        file_shard = pickle.load(f)
    assert file_shard[0] == b"test"


def test_buffer_free_shard(buffer):
    empty_shard = buffer[0]
    empty_shard[0] = b"test"
    buffer._free_shard(0)
    shard_dir = buffer.shard_paths[0]
    with open(shard_dir, "rb") as f:
        file_shard = pickle.load(f)
    assert file_shard == empty_shard
    assert 0 not in buffer.loaded_shards
    assert len(buffer.loaded_shards) == 0
