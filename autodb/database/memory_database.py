from itertools import chain
from typing import Dict, Optional, Generator, Union

from autodb.abc.database import Database
from ..table import MemoryTable


class MemoryDatabase(Database):
    """In memory implementation of the AutoDB interface."""

    def __init__(self) -> None:
        self.tables: Dict[object, MemoryTable] = {}
        self.selected_table = None

    def __len__(self):
        return sum((len(table) for table in self.tables.values()))

    def __repr__(self):
        return "".join([f"{str(cls)}: size={self.tables[cls].size}\n" for cls in self.tables.keys()])

    def __iter__(self):
        return iter(self.tables.keys())

    def insert(self, complex_object: object):
        class_type = type(complex_object)
        if class_type not in self.tables:
            self.tables.update({class_type: MemoryTable()})
        self.tables[class_type].insert(complex_object)

    def select(self, cls):
        if self.selected_table is not None:
            raise RuntimeError
        if cls in self.tables:
            self.selected_table = cls
        else:
            raise KeyError(f"No table of {cls} exists in this database!")
        return self

    def retrieve(self, **kwargs) -> Union[Optional[Generator[object, None, None]], chain]:
        """
        This method retrieves objects from the database depending on the user specified query.
        Note: This method will not throw query errors when performing a search on the entire
        database. Typically this type of behavior should be avoided.
        :param kwargs:
        :return:
        """
        if self.selected_table is None:
            raise RuntimeError
        try:
            return self.tables[self.selected_table].retrieve(**kwargs)
        finally:
            self.selected_table = None

    def retrieve_all(self):
        if self.selected_table is None:
            raise RuntimeError
        try:
            return self.tables[self.selected_table].retrieve_all()
        finally:
            self.selected_table = None

    def retrieve_valid_indexes(self):
        if self.selected_table is None:
            raise RuntimeError
        try:
            return self.tables[self.selected_table].retrieve_valid_indexes()
        finally:
            self.selected_table = None

    def delete(self, **kwargs) -> None:
        if self.selected_table is None:
            raise RuntimeError
        if len(kwargs) == 0:
            raise ValueError
        try:
            self.tables[self.selected_table].delete(**kwargs)
        finally:
            self.selected_table = None

    def clear(self):
        self.tables = {}
