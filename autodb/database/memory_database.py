from ..table import Table
from .database import Database

from typing import List, Dict, Optional, Generator, Union
from itertools import chain
from ..errors import InvalidRange


class MemoryDatabase(Database):
    """In memory implementation of the AutoDB interface."""

    def __init__(self) -> None:
        self.class_map: Dict[object, Table] = {}

    def __len__(self):
        return sum((len(table) for table in self.class_map.values()))

    def __repr__(self):
        return "".join([f"{str(cls)}: size={self.class_map[cls].size}" for cls in self.class_map.values()])

    def insert(self, complex_object: object):
        class_type = type(complex_object)
        if class_type not in self.class_map:
            self.class_map.update({class_type: Table()})
        self.class_map[class_type].insert(complex_object)

    def retrieve(self, class_type=None, **kwargs) -> Union[Optional[Generator[object, None, None]], chain]:
        """
        This method retrieves objects from the database depending on the user specified query.
        Note: This method will not throw query errors when performing a search on the entire
        database. Typically this type of behavior should be avoided.
        :param class_type:
        :param return_copies:
        :param kwargs:
        :return:
        """
        if class_type is None:
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
            if class_type not in self.class_map:
                raise IndexError
            return self.class_map[class_type].retrieve(**kwargs)

    def delete(self, class_type=None, **kwargs) -> None:
        if class_type is None:
            for table in self.class_map.values():
                try:
                    table.delete(**kwargs)
                except (IndexError, ValueError) as _:
                    continue
                except InvalidRange:
                    raise InvalidRange(f"An invalid range query was given!")
        else:
            if class_type not in self.class_map:
                raise IndexError
            self.class_map[class_type].delete(**kwargs)
