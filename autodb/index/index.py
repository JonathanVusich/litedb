from typing import Union, Optional, Set

from sortedcontainers import SortedDict


class Index:
    """
    This class stores maps valid index values to their corresponding list index.
    """

    def __init__(self, index_type):
        self.indexes: SortedDict[object, Union[int, Set[int]]] = SortedDict()
        self.none_indexes: Set[int] = set()
        self.index_type = index_type

    def __len__(self):
        return len(self.indexes) + len(self.none_indexes)

    def add(self, value, index: int) -> None:
        """This method adds an index for a given value."""
        if value is None:
            self.none_indexes.add(index)
            return
        if value in self.indexes:
            indexes = self.indexes[value]
            if isinstance(indexes, set):
                self.indexes[value].add(index)
            else:
                self.indexes[value] = {indexes, index}
        else:
            self.indexes.update({value: index})

    def retrieve(self, value) -> Set[int]:
        """Return a set that contains the indexes that match the specified value."""
        if value is None:
            if len(self.none_indexes) > 0:
                return self.none_indexes
        elif value in self.indexes:
            indexes = self.indexes[value]
            if isinstance(indexes, int):
                return {indexes}
            return self.indexes[value]
        else:
            return set()

    def retrieve_range(self, low, high) -> Optional[Set[int]]:
        """This function retrieves a range of values depending on the high and low indexes given."""
        min_index = self.indexes.bisect_left(low) if low is not None else 0
        max_index = self.indexes.bisect_right(high) if high is not None else 0
        if low is None and high is None:
            if len(self.none_indexes) == 0:
                return
            return self.none_indexes.copy()
        elif low is None or high is None:
            return_set = self.none_indexes.copy()
        else:
            return_set = set()
        index_sets = self.indexes.values()[min_index:max_index]

        if len(index_sets) == 0 and len(return_set) == 0:
            return
        for index in index_sets:
            if isinstance(index, set):
                return_set.update(index)
            else:
                return_set.add(index)
        return return_set

    def destroy(self, value, index: int) -> None:
        if value is None:
            self.none_indexes.remove(index)
            return
        entry = self.indexes[value] if value in self.indexes else None
        if entry is not None:
            if isinstance(entry, set):
                if index not in entry:
                    raise KeyError
                entry.remove(index)
                if len(entry) == 1:
                    self.indexes[value] = entry.pop()
            else:
                if index == entry:
                    self.indexes.pop(value)
                else:
                    raise KeyError
        else:
            raise KeyError
