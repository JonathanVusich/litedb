import os
from typing import Dict

from .shard import Shard
from .shardlru import ShardLRU
from ..utils.serialization import load_shard, dump_shard, get_checksum
from ..database.config import Config


class ShardBuffer:
    """
    Manages serializing and deserializing shards on the fly.
    Also allows iteration for easy object collection.
    """

    def  __init__(self, table_dir: str, shard_paths: Dict[int, str], config: Config) -> None:
        self.table_dir = table_dir
        self.loaded_shards: Dict[int, Shard] = {}
        self.shard_paths = shard_paths
        self.current_shard_index: int = -1
        self.lru = ShardLRU(max_len=config.page_cache)
        self.config = config

    def __iter__(self):
        self.current_shard_index = -1
        return self

    def __next__(self) -> Shard:
        self.current_shard_index += 1
        if self.current_shard_index in self.shard_paths:
            self._ensure_shard_loaded(self.current_shard_index)
            return self.loaded_shards[self.current_shard_index]
        else:
            raise StopIteration

    def __getitem__(self, shard_index: int) -> Shard:
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
        shard_to_persist = self.lru.update(shard_index)
        if shard_to_persist is not None:
            self._free_shard(shard_to_persist)
        if shard_index not in self.loaded_shards:
            if shard_index in self.shard_paths:
                self.loaded_shards.update(
                    {shard_index: Shard.from_bytes(load_shard(self.shard_paths[shard_index]), self.config.page_size)})
            else:
                self.loaded_shards.update({shard_index: Shard()})
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
            dump_shard(shard_path, shard_data.to_bytes())

    def _shard_has_changes(self, shard: int) -> bool:
        """Uses checksums to calculate if a shard has changed."""
        if os.path.exists(self.shard_paths[shard]):
            saved_checksum = get_checksum(self.shard_paths[shard])
            return not saved_checksum == self.loaded_shards[shard].checksum
        return True

    def commit(self) -> None:
        """Persists all shards."""
        for shard in self.loaded_shards:
            self._persist_shard(shard)
