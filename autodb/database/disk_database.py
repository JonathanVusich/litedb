import os
from typing import Dict, Generator

from autodb.abc.database import Database
from ..errors import DatabaseNotFound
from ..table import PersistentTable
from ..utils.path import load_tables
from ..utils.io import empty_directory


class DiskDatabase(Database):

    def __init__(self, directory: str):
        self.selected_table = None
        self.tables: Dict[object, PersistentTable] = {}
        self.directory = directory
        if not os.path.exists(directory):
            os.mkdir(directory)
        if os.path.exists(directory):
            try:
                for table in load_tables(directory):
                    table = PersistentTable.from_file(table)
                    self.tables.update({table.table_type: table})
            except DatabaseNotFound:
                pass

    def __iter__(self):
        return iter(self.tables.keys())

    def select(self, cls):
        if self.selected_table is not None:
            raise RuntimeError
        if cls in self.tables:
            self.selected_table = cls
        else:
            raise KeyError(f"No table of {cls} exists in this database!")
        return self

    def __len__(self):
        return sum(len(table) for table in self.tables.values())

    def __repr__(self):
        return f"DiskDatabase({self.directory})"

    def insert(self, item: object) -> None:
        """Inserts an arbitrary Python object into the database. Do not use this
        database to store raw types."""
        class_name = type(item)
        try:
            self.tables[class_name].insert(item)
        except KeyError:
            self.tables.update(
                {class_name: PersistentTable.new(os.path.join(self.directory, hex(abs(hash(class_name)))),
                                                 table_type=class_name)})
            self.tables[class_name].insert(item)

    def batch_insert(self, items: list) -> None:
        """Inserts a list of similar Python objects into the database."""
        if len(items) == 0:
            return
        else:
            first_item_type = type(items[0])
            if any((type(x) != first_item_type for x in items)):
                raise ValueError("Batch insert requires all elements to be of the same type!")
            try:
                self.tables[first_item_type].batch_insert(items)
            except KeyError:
                self.tables.update(
                    {first_item_type: PersistentTable.new(os.path.join(self.directory, hex(abs(hash(first_item_type)))),
                                                          table_type=first_item_type)})
                self.tables[first_item_type].batch_insert(items)

    def retrieve(self, **kwargs):
        if self.selected_table is None:
            raise RuntimeError
        try:
            items = self.tables[self.selected_table].retrieve(**kwargs)
        finally:
            self.selected_table = None
        return items

    def retrieve_all(self) -> Generator[object, None, None]:
        if self.selected_table is None:
            raise RuntimeError
        try:
            items = self.tables[self.selected_table].retrieve_all()
        finally:
            self.selected_table = None
        return items

    def retrieve_valid_indexes(self) -> list:
        if self.selected_table is None:
            raise RuntimeError
        try:
            indexes = self.tables[self.selected_table].retrieve_valid_indexes()
        finally:
            self.selected_table = None
        return indexes

    def delete(self, **kwargs):
        if self.selected_table is None:
            raise RuntimeError
        try:
            self.tables[self.selected_table].delete(**kwargs)
        finally:
            self.selected_table = None

    def clear(self):
        empty_directory(self.directory)
        self.tables = {}
