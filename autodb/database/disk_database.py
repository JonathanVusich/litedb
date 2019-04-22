import pickle
import os

from .database import Database
from ..table import PersistentTable
from ..utils.path import load_tables


class DiskDatabase(Database):

    def __init__(self, directory: str):
        self.tables = {}
        self.directory = directory
        if os.path.exists(directory):
            for table_info in load_tables(directory):
                table = PersistentTable.from_file(table_info)
                self.tables.update({table.table_type: table})

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
