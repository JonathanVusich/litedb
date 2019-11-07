from typing import Dict, ValuesView

from litedb.abc.database import Database
from litedb.abc.table import Table
from ..table import MemoryTable


class MemoryDatabase(Database):
    """In memory implementation of the AutoDB interface."""

    def __init__(self) -> None:
        self._tables: Dict[object, MemoryTable] = {}
        self.selected_table = None

    def __len__(self):
        """Returns the number of objects contained in this database."""
        return sum((len(table) for table in self._tables.values()))

    def __repr__(self):
        return "".join([f"{str(cls)}: size={self._tables[cls].size}\n" for cls in self._tables.keys()])

    def __iter__(self):
        """Returns the various table types that this database contains."""
        return iter(self._tables.keys())

    @property
    def tables(self) -> ValuesView[Table]:
        """Returns a view of all of the tables in this database."""
        return self._tables.values()

    def insert(self, complex_object: object):
        """Inserts an arbitrary Python class into the database. Do not use this
        database to store raw types."""

        if isinstance(complex_object, (dict, tuple, set, list, bytes, bytearray, str, int, bool, float, complex,
                                       memoryview, frozenset, range)):
            raise TypeError

        class_type = type(complex_object)
        if class_type not in self._tables:
            self._tables.update({class_type: MemoryTable()})
        self._tables[class_type]._insert(complex_object)

    def select(self, cls) -> Table:
        """Returns the table that contains classes of the given type."""
        if cls in self._tables:
            return self._tables[cls]
        else:
            raise KeyError(f"No table of {cls} exists in this database!")
