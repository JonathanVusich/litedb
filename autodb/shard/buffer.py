import os
import pickle
from typing import List, Dict, Optional

from .queue import ShardLRU
from ..utils.checksum import checksum
from ..utils.serialization import load, dump, get_checksum

SHARD_SIZE = 512


class ShardBuffer:
    """
    Manages serializing and deserializing shards on the fly.
    Also allows iteration for easy object collection.
    """

    def __init__(self, table_dir: str, shard_paths: Dict[int, str]) -> None:
        self.table_dir = table_dir
        self.loaded_shards = {}
        self.shard_paths = shard_paths
        self.current_shard_index: int = -1
        self.mru = ShardLRU()

    def __iter__(self):
        self.current_shard_index = -1
        return self

    def __next__(self) -> List[Optional[bytes]]:
        self.current_shard_index += 1
        if self.current_shard_index in self.shard_paths:
            self._ensure_shard_loaded(self.current_shard_index)
            return self.loaded_shards[self.current_shard_index]
        else:
            raise StopIteration

    def __getitem__(self, shard_index: int) -> List[Optional[bytes]]:
        self._ensure_shard_loaded(shard_index)
        return self.loaded_shards[shard_index]

    def _create_new_shard_path(self) -> str:
        """Creates a new shard path that will not collide with any others."""
        shard_name = f"shard{len(self.shard_paths)}"
        return os.path.join(self.table_dir, shard_name)

    def _ensure_shard_loaded(self, shard_index: int) -> None:
        """
        Ensures that the given shard index has been loaded.
        :param shard_index:
        :return:
        """
        shard_to_persist = self.mru.update(shard_index)
        if shard_to_persist is not None:
            self._free_shard(shard_to_persist)
        if shard_index not in self.loaded_shards:
            if shard_index in self.shard_paths:
                self.loaded_shards.update({shard_index: load(self.shard_paths[shard_index])})
            else:
                self.loaded_shards.update({shard_index: [None] * SHARD_SIZE})
                self.shard_paths.update({shard_index: self._create_new_shard_path()})

    def _free_shard(self, shard: int) -> None:
        """Clears a shard from memory and saves it to disk."""
        self._persist_shard(shard)
        if shard in self.loaded_shards:
            self.loaded_shards.pop(shard)

    def _persist_shard(self, shard: int) -> None:
        """Saves a shard to disk."""
        if not self._shard_has_changes(shard):
            return
        if shard in self.loaded_shards:
            shard_path = self.shard_paths[shard]
            shard_data = self.loaded_shards[shard]
            dump(shard_path, shard_data)

    def _calculate_checksum(self, shard: int) -> bytes:
        """Gets the checksum of a given shard index."""
        if shard not in self.loaded_shards:
            raise ValueError("Cannot calculate checksum of persisted shard!")
        pickled_shard = pickle.dumps(self.loaded_shards[shard], pickle.HIGHEST_PROTOCOL)
        return checksum(pickled_shard)

    def _shard_has_changes(self, shard: int) -> bool:
        """Uses checksums to calculate if a shard has changed."""
        if os.path.exists(self.shard_paths[shard]):
            saved_checksum = get_checksum(self.shard_paths[shard])
            return not saved_checksum == self._calculate_checksum(shard)
        return True

    def commit(self) -> None:
        """Persists all shards."""
        for shard in self.loaded_shards:
            self._persist_shard(shard)
