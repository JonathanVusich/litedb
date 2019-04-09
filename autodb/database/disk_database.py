import pickle

from .database import Database
from ..table import PersistentTable
from ..utils.io_utils import dir_empty, has_class_map, load_tables


class DiskDatabase(Database):

    def __init__(self, directory: str):
        self.directory = directory
        if not dir_empty(directory) and has_class_map(directory):
            self.class_map = pickle.load("cmap")
            for table_info in load_tables(directory):
                table = PersistentTable(table_info)
                self.class_map[table.table_type] = table

    def __len__(self):
        pass

    def __repr__(self):
        pass

    def insert(self, item):
        pass

    def retrieve(self, class_type=None, **kwargs):
        pass

    def delete(self, class_type=None, **kwargs):
        pass
