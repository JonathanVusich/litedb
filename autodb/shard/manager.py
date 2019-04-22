from typing import Dict, Tuple, Generator, Iterable
from sortedcontainers import SortedDict

from ..utils.serialization import serialize, deserialize
from .buffer import ShardBuffer

SHARD_SIZE = 512


class ShardManager:

    def __init__(self, table_dir: str, shard_paths: Dict[int, str]) -> None:
        self.buffer = ShardBuffer(table_dir, shard_paths)

    def retrieve(self, indexes: Iterable[int]) -> Generator[object, None, None]:
        """
        Retrieves objects based on their indexes.
        :param indexes:
        :return:
        """
        ds = deserialize  # For faster local lookup
        shard_indexes = [calculate_shard_number(index) for index in indexes]
        shard_indexes.sort(key=lambda x: x[0])
        for shard, index in shard_indexes:
            yield ds(self.buffer[shard][index])

    def retrieve_all(self) -> Generator[object, None, None]:
        """
        Retrieves all objects in the table.
        :return:
        """
        ds = deserialize
        for shard in self.buffer:
            for byte in filter(lambda x: x is not None, shard):
                yield ds(byte)

    def insert(self, items: SortedDict) -> None:
        sl = serialize  # local for faster performance
        prepped_items = SortedDict()
        for key, value in items.items():
            prepped_items[calculate_shard_number(key)] = sl(value)
        for shard_index, value in prepped_items.items():
            shard, index = shard_index
            self.buffer[shard][index] = value
        self.buffer.commit()

    def delete(self, indexes: Iterable[int]) -> None:
        shard_indexes = [calculate_shard_number(index) for index in indexes]
        shard_indexes.sort(key=lambda x: x[0])
        for shard, index in shard_indexes:
            self.buffer[shard][index] = None
        self.buffer.commit()


def calculate_shard_number(index: int) -> Tuple[int, int]:
    return index // SHARD_SIZE, index % SHARD_SIZE
