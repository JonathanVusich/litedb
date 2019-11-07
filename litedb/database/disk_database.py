import os
from typing import Dict, ValuesView

from litedb.abc.database import Database
from ..database.config import Config
from ..errors import DatabaseNotFound
from ..table import PersistentTable
from ..utils.path import load_tables


class DiskDatabase(Database):
    """Persistent implementation of the AutoDB interface."""

    def __init__(self, directory: str, config: Config = Config()):
        self._tables: Dict[object, PersistentTable] = {}
        self.directory = directory
        self._config = config
        if not os.path.exists(directory):
            os.mkdir(directory)
        if os.path.exists(directory):
            if not os.path.isdir(directory):
                raise IOError("liteDB instances can only be created in a folder!")
            try:
                for table in load_tables(directory):
                    table = PersistentTable._from_file(table)
                    self._tables.update({table._table_type: table})
            except DatabaseNotFound:
                pass

    def __iter__(self):
        """Returns the various table types that this database contains."""
        return iter(self._tables.keys())

    def __len__(self):
        """Returns the number of objects contained in this database."""
        return sum(len(table) for table in self._tables.values())

    def __repr__(self):
        return f"DiskDatabase({self.directory})"

    @property
    def tables(self) -> ValuesView[PersistentTable]:
        """Returns a view of all of the tables present in this database."""
        return self._tables.values()

    @property
    def modified(self):
        """Returns True if there are unsaved changes in this database."""
        return all(table.modified for table in self._tables.values())

    def insert(self, item: object) -> None:
        """Inserts an arbitrary Python class into the database. Do not use this
        database to store raw types."""

        if isinstance(item, (dict, tuple, set, list, bytes, bytearray, str, int, bool, float, complex,
                             memoryview, frozenset, range)):
            raise TypeError

        class_name = type(item)
        try:
            self._tables[class_name]._insert(item)
        except KeyError:
            self._tables.update(
                {class_name: PersistentTable._new(self._config,
                                                  os.path.join(self.directory, hex(abs(hash(class_name)))),
                                                  table_type=class_name)})
            self._tables[class_name]._insert(item)

    def select(self, cls):
        """Retrieves the table that contains classes of the given type."""
        if cls in self._tables:
            return self._tables[cls]
        else:
            raise KeyError(f"No table of {cls} exists in this database!")

    def commit(self):
        """Commits all data in this database to disk."""
        for table in self._tables.values():
            table.commit()
