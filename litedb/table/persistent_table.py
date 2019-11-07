import os
from typing import List, Generator, Union, Set

from sortedcontainers import SortedList

from litedb.abc.table import Table
from ..database.config import Config
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

    def __init__(self, config: Config = None, directory: str = None,
                 table_type=None) -> None:
        """
        This class can be instantiated as either a fresh new table or as an existing one from a file structure.
        To create a new table, an empty table directory and table type must be specified. Otherwise path info for
        the existing table must be supplied.
        :param directory:
        :param table_type:
        """

        self._directory = directory
        self._modified = False
        if not os.path.exists(directory):
            os.mkdir(directory)
        self._info_path = create_info_path(self._directory)

        if table_type is not None:
            self._table_type = table_type
            self._size = 0
            self._unused_indexes: SortedList = SortedList()
            self._config = config
        else:
            self._table_type = load_object(os.path.join(self._info_path, "table_type"))
            # noinspection PyTypeChecker
            self._size: int = load_object(os.path.join(self._info_path, "size"))
            # noinspection PyTypeChecker
            self._unused_indexes: SortedList = load_object(os.path.join(self._info_path, "unused_indexes"))
            # noinspection PyTypeChecker
            self._config: Config = load_object(os.path.join(self._info_path, "config"))

        self._index_path = create_index_path(self._directory)
        self._shard_manager = ShardManager(self._directory, self._config)
        self._index_manager: PersistentIndex = PersistentIndex(self._index_path)

    def __repr__(self):
        return f"Table(size={self._size})"

    def __len__(self):
        return self._size

    def __iter__(self):
        return self._shard_manager.retrieve_all()

    @classmethod
    def _from_file(cls, directory: str):
        """Internal method to load a table from disk."""
        return cls(directory=directory)

    @classmethod
    def _new(cls, config: Config, directory: str, table_type):
        """Creates a new table with the given directory as the persistence location."""
        return cls(config=config, directory=directory, table_type=table_type)

    @property
    def indexes(self) -> List[str]:
        """Returns a list of all the indexes in this table."""
        return list(self._index_manager.index_map.keys())

    @property
    def modified(self) -> bool:
        """Indicates whether this table has unsaved changes."""
        return self._modified

    def retrieve(self, **kwargs) -> [Generator[object, None, None]]:
        """Retrieves items in this table based on the given argument descriptors."""
        if len(kwargs) == 0:
            raise ValueError
        indexes = self._index_manager.retrieve(**kwargs)
        if indexes:
            return self._shard_manager.retrieve(indexes)
        else:
            return ([])

    def delete(self, **kwargs):
        """Removes items from this table based on the given descriptors."""
        if len(kwargs) == 0:
            raise ValueError
        indexes_to_delete = self._index_manager.retrieve(**kwargs)
        if indexes_to_delete:
            self._modified = True
            self._delete_indexes(indexes_to_delete)

    def clear(self):
        """Removes any stored data relating to this table and clears out all items from this table."""
        empty_directory(self._directory)
        self._shard_manager = ShardManager(self._directory, self._config)
        self._index_manager = PersistentIndex(self._index_path)
        self._size = 0
        self._unused_indexes: SortedList = SortedList()

    def commit(self) -> None:
        """Commits any changes made to this table to disk."""
        if self._modified:
            dump_object(os.path.join(self._info_path, "table_type"), self._table_type)
            dump_object(os.path.join(self._info_path, "size"), self._size)
            dump_object(os.path.join(self._info_path, "unused_indexes"), self._unused_indexes)
            dump_object(os.path.join(self._info_path, "config"), self._config)
            self._index_manager.commit()
            self._shard_manager.commit()
            self._modified = False

    def _delete_indexes(self, indexes: Union[Set[int], List[int]]) -> None:
        """Internal method to actually perform deletion."""
        indexes_to_delete = sorted(indexes)  # Sort indexes for shards
        self._size -= len(indexes_to_delete)  # Decrement the size of the table
        if indexes_to_delete:
            self._modified = True
            items_to_delete = self._shard_manager.retrieve(indexes_to_delete)
            for item, index in zip(items_to_delete, indexes_to_delete):
                self._index_manager.unindex_item(item, index)
            self._unused_indexes.update(indexes_to_delete)
            self._shard_manager.delete(indexes_to_delete)

    def _insert(self, item: object) -> None:
        """Internal method that indexes and stores an object."""
        if len(self._unused_indexes) > 0:
            index = self._unused_indexes.pop()
        else:
            index = self._size
            self._shard_manager.insert(item, index)
        self._size += 1
        self._index_manager.index_item(item, index)
        self._modified = True
