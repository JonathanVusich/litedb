import os

from .memory_index import MemoryIndex
from ..utils.serialization import dump, load


class PersistentIndex(MemoryIndex):

    def __init__(self, index_path: str) -> None:
        super().__init__()
        self.index_path = index_path
        self.blacklist_path = os.path.join(self.index_path, "blacklist")
        self.map_path = os.path.join(self.index_path, "map")
        self.load()

    def load(self) -> None:
        index_map = load(self.map_path)
        if index_map is not None:
            self.index_map = index_map
        blacklist = load(self.blacklist_path)
        if blacklist is not None:
            self.index_blacklist = blacklist

    def persist(self) -> None:
        dump(self.blacklist_path, self.index_blacklist)
        dump(self.map_path, self.index_map)
