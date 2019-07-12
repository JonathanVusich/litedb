from typing import Dict

from autodb.abc.database import Database
from autodb.abc.table import Table
from ..table import MemoryTable


class MemoryDatabase(Database):
    """In memory implementation of the AutoDB interface."""

    def __init__(self) -> None:
        self.tables: Dict[object, MemoryTable] = {}
        self.selected_table = None

    def __len__(self):
        """Returns the number of objects contained in this database."""
        return sum((len(table) for table in self.tables.values()))

    def __repr__(self):
        return "".join([f"{str(cls)}: size={self.tables[cls].size}\n" for cls in self.tables.keys()])

    def __iter__(self):
        """Returns the various table types that this database contains."""
        return iter(self.tables.keys())

    def insert(self, complex_object: object):
        """Inserts an arbitrary Python class into the database. Do not use this
        database to store raw types."""
        class_type = type(complex_object)
        if class_type not in self.tables:
            self.tables.update({class_type: MemoryTable()})
        self.tables[class_type]._insert(complex_object)

    def select(self, cls) -> Table:
        """Returns the table that contains classes of the given type."""
        if cls in self.tables:
            return self.tables[cls]
        else:
            raise KeyError(f"No table of {cls} exists in this database!")
