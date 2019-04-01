from typing import Union, Optional, List, Set
from sortedcontainers import SortedDict


class Index:
    """
    This class stores maps valid index values to their corresponding list index.
    """

    def __init__(self, trie_type):
        self.indexes: SortedDict[trie_type, Set[int]] = SortedDict()
        self.none_indexes: Set[int] = set()
        self.trie_type = trie_type

    def __len__(self):
        return len(self.indexes) + len(self.none_indexes)

    def add(self, value, index: int) -> None:
        """This method adds an index for a given value."""
        if value is None:
            self.none_indexes.add(index)
            return
        if not isinstance(value, self.trie_type):
            raise ValueError(f"{value} is not an instance of {self.trie_type}!")
        if value in self.indexes:
            self.indexes[value].add(index)
        else:
            self.indexes.update({value: {index}})

    def retrieve(self, value) -> Optional[frozenset]:
        if value is None:
            return frozenset(self.none_indexes)
        elif not isinstance(value, self.trie_type):
            raise ValueError(f"{value} is not an instance of {self.trie_type}!")
        if value in self.indexes:
            return frozenset(self.indexes[value])

    def retrieve_range(self, low, high) -> Optional[frozenset]:
        if low is not None and not isinstance(low, self.trie_type):
            raise ValueError(f"{low} is not an instance of {self.trie_type}!")
        elif high is not None and not isinstance(high, self.trie_type):
            raise ValueError(f"{high} is not an instance of {self.trie_type}!")

        return_set: Set[int] = set()
        if high is None:
            return
        max_index = self.indexes.bisect_right(high)
        if low is None:
            return_set = self.none_indexes
            min_index = 0
        else:
            min_index = self.indexes.bisect_left(low)
        index_sets = self.indexes.values()[min_index:max_index]

        if not index_sets:
            return
        return_set = return_set.union(*index_sets)
        return frozenset(return_set)

    def destroy(self, value, index: int) -> None:
        entry = self.indexes[value] if value in self.indexes else None
        if entry:
            entry.remove(index)
            if len(entry) == 0:
                self.indexes.pop(value)
