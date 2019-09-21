import os
from typing import List, Generator, Union, Set

from sortedcontainers import SortedList, SortedDict

from pydb.abc.table import Table
from ..index import PersistentIndex
from ..shard import ShardManager
from ..utils.io import empty_directory
from ..utils.path import create_info_path, create_index_path
from ..utils.serialization import load_object, dump_object


class PersistentTable(Table):
    """
    This class stores elements of a certain class type within a regular Python list.
    Object attributes are indexed and stored within indexes that can then be used to retrieve
    objects from the list.
    """

    def __init__(self, directory: str = None,
                 table_type=None) -> None:
        """
        This class can be instantiated as either a fresh new table or as an existing one from a file structure.
        To create a new table, an empty table directory and table type must be specified. Otherwise path info for
        the existing table must be supplied.
        :param directory:
        :param table_type:
        """

        self.directory = directory
        self._modified = False
        if not os.path.exists(directory):
            os.mkdir(directory)
        self.index_path = create_index_path(self.directory)
        self.info_path = create_info_path(self.directory)
        self.shard_manager = ShardManager(self.directory)
        self.index_manager: PersistentIndex = PersistentIndex(self.index_path)
        if table_type is not None:
            self.table_type = table_type
            self.size = 0
            self.unused_indexes: SortedList = SortedList()
        elif table_type is None:
            self.table_type = load_object(os.path.join(self.info_path, "table_type"))
            self.size = load_object(os.path.join(self.info_path, "size"))
            self.unused_indexes: SortedList = load_object(os.path.join(self.info_path, "unused_indexes"))
        else:
            raise AttributeError

    def __repr__(self):
        return f"Table(size={self.size})"

    def __len__(self):
        return self.size

    @classmethod
    def _from_file(cls, directory: str):
        """Internal method to load a table from disk."""
        return cls(directory=directory)

    @classmethod
    def _new(cls, directory: str, table_type):
        """Creates a new table with the given directory as the persistence location."""
        return cls(directory=directory, table_type=table_type)

    def commit(self) -> None:
        """Commits any changes made to this table to disk."""
        if self._modified:
            dump_object(os.path.join(self.info_path, "table_type"), self.table_type)
            dump_object(os.path.join(self.info_path, "size"), self.size)
            dump_object(os.path.join(self.info_path, "unused_indexes"), self.unused_indexes)
            self.index_manager.commit()
            self.shard_manager.commit()
            self._modified = False

    def _insert(self, item: object) -> None:
        """Internal method that indexes and stores an object."""
        if len(self.unused_indexes) > 0:
            index = self.unused_indexes.pop()
        else:
            index = self.size
            item_dict = SortedDict()
            item_dict[index] = item
            self.shard_manager.insert(item_dict)
        self.size += 1
        self.index_manager.index_item(item, index)
        self._modified = True

    def _batch_insert(self, item_list: List[object]) -> None:
        """Internal method that indexes and stores a list of objects."""
        first_item_type = type(item_list[0])
        item_dict = SortedDict()
        for item in item_list:
            if type(item) != first_item_type:
                raise ValueError("Batch insert requires all elements to be of the same type!")
            if len(self.unused_indexes) > 0:
                index = self.unused_indexes.pop()
            else:
                index = self.size
            self.size += 1
            item_dict[index] = item
        self.shard_manager.insert(item_dict)
        for index, item in item_dict.items():
            self.index_manager.index_item(item, index)
        self._modified = True

    def retrieve(self, **kwargs) -> [Generator[object, None, None]]:
        """Retrieves items in this table based on the given argument descriptors."""
        if len(kwargs) == 0:
            raise ValueError
        indexes = self.index_manager.retrieve(**kwargs)
        if indexes:
            return self.shard_manager.retrieve(indexes)
        else:
            return ([])

    def retrieve_all(self) -> [Generator[object, None, None]]:
        """Retrieves all items in this table."""
        return self.shard_manager.retrieve_all()

    @property
    def indexes(self) -> List[str]:
        """Returns a list of all the indexes in this table."""
        return list(self.index_manager.index_map.keys())

    @property
    def modified(self) -> bool:
        """Indicates whether this table has unsaved changes."""
        return self._modified

    def delete(self, **kwargs):
        """Removes items from this table based on the given descriptors."""
        if len(kwargs) == 0:
            raise ValueError
        indexes_to_delete = self.index_manager.retrieve(**kwargs)
        if indexes_to_delete:
            self._modified = True
            self._delete_indexes(indexes_to_delete)

    def _delete_indexes(self, indexes: Union[Set[int], List[int]]) -> None:
        """Internal method to actually perform deletion."""
        indexes_to_delete = sorted(indexes)  # Sort indexes for shards
        self.size -= len(indexes_to_delete)  # Decrement the size of the table
        if indexes_to_delete:
            self._modified = True
            items_to_delete = self.shard_manager.retrieve(indexes_to_delete)
            for item, index in zip(items_to_delete, indexes_to_delete):
                self.index_manager.unindex_item(item, index)
            self.unused_indexes.update(indexes_to_delete)
            self.shard_manager.delete(indexes_to_delete)

    def clear(self):
        """Removes any stored data relating to this table and clears out all items from this table."""
        empty_directory(self.directory)
        self.shard_manager = ShardManager(self.directory)
        self.index_manager = PersistentIndex(self.index_path)
        self.size = 0
        self.unused_indexes: SortedList = SortedList()
