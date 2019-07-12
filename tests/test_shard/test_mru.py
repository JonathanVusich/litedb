from autodb.shard.queue import ShardLRU
from collections import deque


def test_mru_instantiate():
    mru = ShardLRU()
    assert mru.mru.maxlen == 5


def test_mru_add_shards():
    mru = ShardLRU()
    mru.update(1)
    mru.update(2)
    mru.update(3)
    mru.update(4)
    assert mru.mru == deque([4, 3, 2, 1])


def test_mru_set_priority():
    mru = ShardLRU()
    mru.update(1)
    mru.update(2)
    mru.update(3)
    mru.update(4)
    mru.update(1)
    assert mru.mru == deque([1, 4, 3, 2])
    mru.update(3)
    assert mru.mru == deque([3, 1, 4, 2])
