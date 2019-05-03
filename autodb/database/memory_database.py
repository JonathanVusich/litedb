from itertools import chain
from typing import Dict, Optional, Generator, Union

from .database import Database
from ..errors import InvalidRange
from ..table import MemoryTable


class MemoryDatabase(Database):
    """In memory implementation of the AutoDB interface."""

    def __init__(self) -> None:
        self.class_map: Dict[object, MemoryTable] = {}

    def __len__(self):
        return sum((len(table) for table in self.class_map.values()))

    def __repr__(self):
        return "".join([f"{str(cls)}: size={self.class_map[cls].size}\n" for cls in self.class_map.keys()])

    def insert(self, complex_object: object):
        cls = type(complex_object)
        if cls not in self.class_map:
            self.class_map.update({cls: MemoryTable()})
        self.class_map[cls].insert(complex_object)

    def retrieve(self, cls=None, **kwargs) -> Union[Optional[Generator[object, None, None]], chain]:
        """
        This method retrieves objects from the database depending on the user specified query.
        Note: This method will not throw query errors when performing a search on the entire
        database. Typically this type of behavior should be avoided.
        :param cls:
        :param kwargs:
        :return:
        """
        if cls is None:
            object_generators = []
            for table in self.class_map.values():
                try:
                    table_results = table.retrieve(**kwargs)
                except (IndexError, ValueError) as _:
                    continue
                except InvalidRange:
                    raise InvalidRange(f"An invalid range query was given!")
                if table_results is not None:
                    object_generators.append(table_results)
            if object_generators:
                return chain(*object_generators)
        else:
            if cls not in self.class_map:
                raise IndexError
            return self.class_map[cls].retrieve(**kwargs)

    def delete(self, cls=None, **kwargs) -> None:
        if cls is None:
            for table in self.class_map.values():
                try:
                    table.delete(**kwargs)
                except (IndexError, ValueError) as _:
                    continue
                except InvalidRange:
                    raise InvalidRange(f"An invalid range query was given!")
        else:
            if cls not in self.class_map:
                raise IndexError
            self.class_map[cls].delete(**kwargs)
