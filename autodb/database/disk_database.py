from .database import Database
from ..errors import PathError
from ..utils.io_utils import dir_empty


class DiskDatabase(Database):

    def __init__(self, directory: str):
        if not dir_empty(directory):
            raise PathError(f"The directory specified ")
        self.directory = directory

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
