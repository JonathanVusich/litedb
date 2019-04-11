from typing import List, Dict, Tuple, Union, Optional
import os
from .queue import ShardMRU

from ..utils.serialization_utils import load_shard, dump_shard, serialize, deserialize

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
        self.mru = ShardMRU()

    def __iter__(self):
        return self

    def __next__(self) -> List[Optional[bytes]]:
        self.current_shard_index += 1
        if self.current_shard_index in self.shard_paths:
            self._ensure_shard_loaded(self.current_shard_index)
            return self.loaded_shards[self.current_shard_index]
        else:
            self.current_shard_index = -1
            raise StopIteration

    def __getitem__(self, shard_index: int) -> List[Optional[bytes]]:
        self._ensure_shard_loaded(shard_index)
        return self.loaded_shards[shard_index]

    def _create_new_shard_path(self) -> str:
        shard_name = f"shard{len(self.shard_paths)}"
        return os.path.join(self.table_dir, shard_name)

    def _ensure_shard_loaded(self, shard_index: int) -> None:
        """
        This function ensures that the given shard index has been loaded.
        :param shard_index:
        :return:
        """
        shard_to_persist = self.mru.update(shard_index)
        if shard_to_persist is not None:
            self._free_shard(shard_to_persist)
        if shard_index not in self.loaded_shards:
            if shard_index in self.shard_paths:
                self.loaded_shards.update({shard_index: load_shard(self.shard_paths[shard_index])})
            else:
                self.loaded_shards.update({shard_index: [None] * SHARD_SIZE})
                self.shard_paths.update({shard_index: self._create_new_shard_path()})

    def _free_shard(self, shard: int) -> None:
        self._persist_shard(shard)
        if shard in self.loaded_shards:
            self.loaded_shards.pop(shard)

    def _persist_shard(self, shard: int) -> None:
        if shard in self.loaded_shards:
            shard_path = self.shard_paths[shard]
            shard_data = self.loaded_shards[shard]
            dump_shard(shard_path, shard_data)
