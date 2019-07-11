import pytest
from autodb.database import MemoryDatabase
from autodb.errors import InvalidRange


class SimpleRecord:
    def __init__(self, x):
        self.x = x


def test_table_retrieve():
    database = MemoryDatabase()
    database.insert(SimpleRecord(12))
    with pytest.raises(KeyError):
        database.select(MemoryDatabase)
    with pytest.raises(InvalidRange):
        database.select(SimpleRecord).retrieve(x=(1, 2, 3))
    with pytest.raises(ValueError):
        database.select(SimpleRecord).retrieve()


def test_table_delete():
    database = MemoryDatabase()
    database.insert(SimpleRecord(12))
    with pytest.raises(KeyError):
        database.select(MemoryDatabase)
    with pytest.raises(InvalidRange):
        database.select(SimpleRecord).retrieve(x=(1, 2, 3))
    with pytest.raises(ValueError):
        database.select(SimpleRecord).delete()

