from ..interfaces.database_interface import DatabaseInterface

from typing import List, Dict

class MemoryDatabase(DatabaseInterface):
    """In memory implementation of the AutoDB interface."""

    def __init__(self) -> None:
        self.class_map: Dict[object, Dict] = {}


    def insert(self, complex_object: object) -> bool:
        pass

    def retrieve(self, class_type: object, **kwargs) -> List[object]:
        pass