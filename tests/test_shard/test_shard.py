from pydb.shard.shard import Shard


def test_basic_init():
    shard = Shard()
    assert shard.binary_blobs == [None] * shard.max_size
    assert shard.max_size == 512
    assert shard.checksum == 0


def test_fill():
    shard = Shard()
    for i in range(shard.max_size):
        shard[i] = i
    for i in range(shard.max_size):
        val = shard[i]
        assert val == i


def test_serialize():
    shard = Shard()
    for i in range(shard.max_size):
        shard[i] = i
    bytes = shard.to_bytes()
    deserialized_shard = Shard.from_bytes(bytes)
    assert deserialized_shard.checksum == shard.checksum
    assert deserialized_shard.binary_blobs == shard.binary_blobs
