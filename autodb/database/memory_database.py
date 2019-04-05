from ..table import Table

from typing import List, Dict, Optional, Generator, Union
from itertools import chain


class MemoryDatabase:
    """In memory implementation of the AutoDB interface."""

    def __init__(self) -> None:
        self.class_map: Dict[object, Table] = {}

    def __len__(self):
        return sum((len(table) for table in self.class_map.values()))

    def insert(self, complex_object: object):
        class_type = type(complex_object)
        if class_type not in self.class_map:
            self.class_map.update({class_type: Table()})
        self.class_map[class_type].insert(complex_object)

    def retrieve(self, class_type=None, return_copies=True, **kwargs) -> Union[Optional[Generator[object, None, None]], chain]:
        if class_type is None:
            object_generators = []
            for table in self.class_map.values():
                try:
                    if return_copies:
                        table_results = table.retrieve(**kwargs)
                    else:
                        table_results = table.retrieve_references(**kwargs)
                except IndexError:
                    continue
                if table_results is not None:
                    object_generators.append(table_results)
            if object_generators:
                return chain(*object_generators)
        else:
            if class_type not in self.class_map:
                raise IndexError
            if return_copies:
                return self.class_map[class_type].retrieve(**kwargs)
            else:
                return self.class_map[class_type].retrieve_references(**kwargs)

    def delete(self, class_type=None, **kwargs) -> None:
        if class_type is None:
            for table in self.class_map.values():
                try:
                    table.delete(**kwargs)
                except IndexError:
                    continue
        else:
            if class_type not in self.class_map:
                raise IndexError
            self.class_map[class_type].delete(**kwargs)
