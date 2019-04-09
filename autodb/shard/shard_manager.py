from typing import Dict, List, Tuple, Optional, Generator, Set, Iterable

from collections import deque
import os
from ..utils.serialization_utils import serialize, deserialize, load_shard, dump_shard
import itertools

SHARD_SIZE = 12
MAX_SHARDS = 4


class ShardManager:

    def __init__(self, table_dir: str, shard_paths: Dict[int, str], shards: Dict[int, List[Optional[bytes]]]) -> None:
        self.table_dir = table_dir
        self.shard_paths = shard_paths
        self.shards = shards
        self.loaded_shards = deque()

    def retrieve(self, indexes: Iterable[int]) -> Generator[object, None, None]:
        shard_indexes = [calculate_shard_number(index) for index in indexes]
        shard_indexes.sort(key=lambda x: x[0])
        for shard, index in shard_indexes:
            self._ensure_shard_loaded(shard)
            yield deserialize(self.shards[shard][index])

    def retrieve_all(self) -> Generator[object, None, None]:
        for shard_number in itertools.count():
            if shard_number not in self.shard_paths:
                raise StopIteration
            elif shard_number not in self.shards:
                self._ensure_shard_loaded(shard_number)
            for item in (item for item in self.shards[shard_number] if item is not None):
                yield item

    def insert(self, item: object, index: int) -> None:
        item = serialize(item)
        shard, index = calculate_shard_number(index)
        self._ensure_shard_loaded(shard)
        self.shards[shard][index] = item

    def delete(self, indexes: Iterable[int]) -> None:
        shard_indexes = [calculate_shard_number(index) for index in indexes]
        shard_indexes.sort(key=lambda x: x[0])
        for shard, index in shard_indexes:
            self._ensure_shard_loaded(shard)
            self.shards[shard][index] = None

    def _ensure_shard_loaded(self, shard: int) -> None:
        if shard not in self.shards:
            if shard in self.shard_paths:
                self.shards.update({shard: load_shard(self.shard_paths[shard])})
            else:
                self.shards.update({shard: [None] * SHARD_SIZE})
                self.shard_paths.update({shard: self._create_new_shard_path()})
            self._check_active_shards(shard)

    def _check_active_shards(self, shard: int) -> None:
        if shard in self.loaded_shards:
            if self.loaded_shards[0] == shard:
                return
            else:
                self.loaded_shards.remove(shard)
        self.loaded_shards.appendleft(shard)
        if len(self.loaded_shards) > 4:
            shard_to_persist = self.loaded_shards.pop()
            self._save_shard(shard_to_persist)

    def _save_shard(self, shard: int) -> None:
        shard_path = self.shard_paths[shard]
        shard_data = self.shards[shard]
        dump_shard(shard_path, shard_data)
        self.shards.pop(shard)

    def _create_new_shard_path(self) -> str:
        shard_name = f"shard{len(self.shard_paths)}"
        return os.path.join(self.table_dir, shard_name)


def calculate_shard_number(index: int) -> Tuple[int, int]:
    return index // SHARD_SIZE, index % SHARD_SIZE
