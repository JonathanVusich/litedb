from typing import Union, Optional, List, Set
from sortedcontainers import SortedDict


class Index:
    """
    This class stores maps valid index values to their corresponding list index.
    """

    def __init__(self, trie_type):
        self.indexes: SortedDict[trie_type, Set[int]] = SortedDict()
        self.trie_type = trie_type

    def create(self, value, index: int) -> None:
        if not isinstance(value, self.trie_type):
            raise ValueError(f"{value} is not an instance of {self.trie_type}!")
        if value in self.indexes:
            self.indexes[value].add(index)
        else:
            self.indexes.update({value: {index}})

    def retrieve(self, value) -> Optional[Set[int]]:
        if not isinstance(value, self.trie_type):
            raise ValueError(f"{value} is not an instance of {self.trie_type}!")
        if value in self.indexes:
            return self.indexes[value]

    def update(self, old_value, old_index: int, new_value, new_index: int) -> None:
        old_entry = self.retrieve(old_value)
        if old_entry:
            old_entry.remove(old_index)
            self.create(new_value, new_index)

    def destroy(self, value, index: int) -> None:
        entry = self.retrieve(value)
        if entry:
            entry.remove(index)
            if len(entry) == 0:
                self.indexes.pop(value)
