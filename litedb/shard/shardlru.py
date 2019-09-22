from collections import deque
from typing import Optional


class ShardLRU:
    """
    This is a most recently used cache that tracks when shards
    should be removed from memory.
    """

    def __init__(self, max_len=64) -> None:
        self.max_len: int = max_len
        self.mru = deque(maxlen=max_len + 1)

    def update(self, shard_index: int) -> Optional[int]:
        """Handles evicting old shards when a new one is added."""
        try:
            self.mru.remove(shard_index)
        except ValueError:
            pass
        self.mru.appendleft(shard_index)
        if len(self.mru) > self.max_len:
            return self.mru.pop()
