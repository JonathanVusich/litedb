import os

from .memory_index import MemoryIndex
from ..utils.serialization import dump_object, load_object


class PersistentIndex(MemoryIndex):
    """An extension of the in-memory index class that commits index
    changes to disk."""

    def __init__(self, index_path: str) -> None:
        super().__init__()
        self.index_path = index_path
        self.blacklist_path = os.path.join(self.index_path, "blacklist")
        self.map_path = os.path.join(self.index_path, "map")
        self.load()

    def load(self) -> None:
        """Loads the index from disk."""
        index_map = load_object(self.map_path)
        if index_map is not None:
            self.index_map = index_map
        blacklist = load_object(self.blacklist_path)
        if blacklist is not None:
            self.index_blacklist = blacklist

    def commit(self) -> None:
        """Persists the index to disk."""
        dump_object(self.blacklist_path, self.index_blacklist)
        dump_object(self.map_path, self.index_map)
