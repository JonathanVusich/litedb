import pytest
from autodb.database import MemoryDatabase
from autodb.errors import InvalidRange


class SimpleRecord:
    def __init__(self, x):
        self.x = x


def test_table_retrieve():
    database = MemoryDatabase()
    database.insert(SimpleRecord(12))
    with pytest.raises(IndexError):
        database.retrieve(MemoryDatabase, x=12)
    with pytest.raises(InvalidRange):
        database.retrieve(SimpleRecord, x=(1, 2, 3))


def test_table_delete():
    database = MemoryDatabase()
    database.insert(SimpleRecord(12))
    with pytest.raises(IndexError):
        database.delete(MemoryDatabase, x=12)
    with pytest.raises(InvalidRange):
        database.delete(SimpleRecord, x=(1, 2, 3))


