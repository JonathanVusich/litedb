import os
from typing import List, Generator

from sortedcontainers import SortedList, SortedDict

from .table import Table
from ..index import IndexManager
from ..shard import ShardManager
from ..utils.path import create_info_path, create_index_path
from ..utils.serialization import load, dump


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
        :param path_info:
        :param table_type:
        """

        self.directory = directory
        if not os.path.exists(directory):
            os.mkdir(directory)
        self.index_path = create_index_path(self.directory)
        self.info_path = create_info_path(self.directory)
        self.shard_manager = ShardManager(self.directory)
        self.index_manager = IndexManager(self.index_path)
        if table_type is not None:
            self.table_type = table_type
            self.size = 0
            self.unused_indexes: SortedList = SortedList()
        elif table_type is None:
            self.table_type = load(os.path.join(self.info_path, "table_type"))
            self.size = load(os.path.join(self.info_path, "size"))
            self.unused_indexes: SortedList = load(os.path.join(self.info_path, "unused_indexes"))
        else:
            raise AttributeError

    def __repr__(self):
        return f"Table(size={self.size})"

    def __len__(self):
        return self.size

    @classmethod
    def from_file(cls, directory: str):
        return cls(directory=directory)

    @classmethod
    def new(cls, directory: str, table_type):
        return cls(directory=directory, table_type=table_type)

    def persist(self):
        dump(os.path.join(self.info_path, "table_type"), self.table_type)
        dump(os.path.join(self.info_path, "size"), self.size)
        dump(os.path.join(self.info_path, "unused_indexes"), self.unused_indexes)
        self.index_manager.persist()

    def insert(self, item: object) -> None:
        if len(self.unused_indexes) > 0:
            index = self.unused_indexes.pop()
        else:
            index = self.size
            item_dict = SortedDict()
            item_dict[index] = item
            self.shard_manager.insert(item_dict)
        self.size += 1
        self.index_manager.index_item(item, index)
        self.persist()

    def batch_insert(self, item_list: List[object]) -> None:
        item_dict = SortedDict()
        for item in item_list:
            if len(self.unused_indexes) > 0:
                index = self.unused_indexes.pop()
            else:
                index = self.size
            self.size += 1
            item_dict[index] = item
        self.shard_manager.insert(item_dict)
        for index, item in item_dict.items():
            self.index_manager.index_item(item, index)
        self.persist()

    def retrieve(self, **kwargs) -> [Generator[object, None, None]]:

        if len(kwargs) == 0:
            return self.shard_manager.retrieve_all()
        indexes = self.index_manager.retrieve(**kwargs)
        if indexes:
            return self.shard_manager.retrieve(indexes)
        else:
            return ([])

    def retrieve_valid_indexes(self) -> List[str]:
        return [index for index in self.index_manager.index_map]

    def delete(self, **kwargs):
        indexes_to_delete = self.index_manager.retrieve(**kwargs)
        # Sort indexes for shards
        indexes_to_delete = sorted(list(indexes_to_delete))
        if indexes_to_delete:
            items_to_delete = self.shard_manager.retrieve(indexes_to_delete)
            for item, index in zip(items_to_delete, indexes_to_delete):
                self.index_manager.unindex_item(item, index)
            self.unused_indexes.extend(indexes_to_delete)
            self.shard_manager.delete(indexes_to_delete)

    def _delete(self, object_index: int) -> None:
        self.index_manager.unindex_item(self.shard_manager.retrieve((object_index,)), object_index)
        self.shard_manager.delete((object_index,))
        self.unused_indexes.add(object_index)
        self.size -= 1


