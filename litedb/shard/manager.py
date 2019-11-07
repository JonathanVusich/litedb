from typing import Tuple, Generator, Iterable

from sortedcontainers import SortedDict

from .buffer import ShardBuffer
from ..utils.path import get_shard_file_paths
from ..database.config import Config


class ShardManager:
    """This class handles the high-level shard operations by manipulating the shard buffer."""

    def __init__(self, table_dir: str, config: Config) -> None:
        shard_paths = get_shard_file_paths(table_dir)
        self.buffer = ShardBuffer(table_dir, shard_paths, config)
        self.config = config

    def retrieve(self, indexes: Iterable[int]) -> Generator[object, None, None]:
        """
        Retrieves objects based on their indexes.
        :param indexes:
        :return:
        """
        shard_indexes = [self.calculate_shard_number(index) for index in indexes]
        shard_indexes.sort(key=lambda x: x[0])
        for shard, index in shard_indexes:
            yield self.buffer[shard][index]

    def retrieve_all(self) -> Generator[object, None, None]:
        """
        Retrieves all objects in the table.
        :return:
        """
        for shard in self.buffer:
            for byte in filter(lambda x: x is not None, shard):
                yield byte

    def insert(self, item: object, index: int) -> None:
        """Inserts and persists the given collection of items."""
        shard, index = self.calculate_shard_number(index)
        self.buffer[shard][index] = item

    def delete(self, indexes: Iterable[int]) -> None:
        """Removes the items with the given indexes."""
        shard_indexes = [self.calculate_shard_number(index) for index in indexes]
        shard_indexes.sort(key=lambda x: x[0])
        for shard, index in shard_indexes:
            self.buffer[shard][index] = None

    def commit(self):
        """Persists all data to disk."""
        self.buffer.commit()

    def calculate_shard_number(self, index: int) -> Tuple[int, int]:
        """Calculates the shard index and the item index within a shard."""
        return index // self.config.page_size, index % self.config.page_size
