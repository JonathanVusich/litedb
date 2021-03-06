from collections import deque

import pytest

from litedb.shard.buffer import ShardBuffer
from litedb.shard.shard import Shard
from litedb.utils.serialization import dump_shard, load_shard
from litedb import Config


@pytest.fixture()
def buffer(tmpdir):
    temp_directory = tmpdir.mkdir("table")
    table_dir = str(temp_directory)
    paths = {0: str(temp_directory.join("shard0"))}
    shard = Shard()
    dump_shard(temp_directory.join("shard0"), shard.to_bytes())
    buffer = ShardBuffer(table_dir, paths, Config())
    return buffer


@pytest.fixture()
def empty_buffer():
    return ShardBuffer("tabledir", {}, Config())


def test_buffer_init(buffer):
    assert buffer.current_shard_index == -1
    assert isinstance(buffer.table_dir, str)
    assert 0 in buffer.shard_paths
    assert buffer.lru.mru == deque([])


def test_empty_buffer_iter(empty_buffer):
    for _ in empty_buffer:
        raise AssertionError


def test_buffer_iter(buffer):
    for x, shard in enumerate(buffer):
        assert x < 1
        empty_shard = Shard()
        assert shard.binary_blobs == empty_shard.binary_blobs
        assert shard.checksum == empty_shard.checksum
        assert shard.none_constant == empty_shard.none_constant


def test_buffer_get_item(buffer):
    blank_shard = Shard()
    empty_shard = buffer[0]
    assert len(buffer.shard_paths) == 1
    assert len(buffer.loaded_shards) == 1
    assert empty_shard.binary_blobs == blank_shard.binary_blobs
    assert empty_shard.checksum == blank_shard.checksum
    second_shard = buffer[1]
    assert len(buffer.shard_paths) == 2
    assert len(buffer.loaded_shards) == 2
    assert second_shard.binary_blobs == blank_shard.binary_blobs
    assert second_shard.checksum == blank_shard.checksum


def test_buffer_create_new_path(buffer, tmpdir):
    assert buffer._create_new_shard_path() == str(tmpdir.join("table").join("shard1"))
    # add new shard
    buffer[1]
    assert buffer._create_new_shard_path() == str(tmpdir.join("table").join("shard2"))


def test_buffer_ensure_shard_loaded(buffer):
    default_config = Config()
    first_shard = buffer[0]
    first_shard[0] = b"test"
    # fill up the buffer
    for i in range(1, default_config.page_cache + 1):
        buffer._ensure_shard_loaded(i)
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
    file_shard = Shard.from_bytes(load_shard(shard_dir), 512)
    assert empty_shard.binary_blobs == file_shard.binary_blobs
    assert empty_shard.checksum == file_shard.checksum

    empty_shard[0] = b"test"
    buffer._persist_shard(0)
    file_shard = Shard.from_bytes(load_shard(shard_dir), 512)
    assert file_shard[0] == b"test"


def test_buffer_free_shard(buffer):
    empty_shard = buffer[0]
    empty_shard[0] = b"test"
    buffer._free_shard(0)
    shard_dir = buffer.shard_paths[0]
    file_shard = Shard.from_bytes(load_shard(shard_dir), 512)
    assert file_shard.checksum == empty_shard.checksum
    assert file_shard.binary_blobs == empty_shard.binary_blobs
    assert 0 not in buffer.loaded_shards
    assert len(buffer.loaded_shards) == 0
