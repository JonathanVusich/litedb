import pytest
from autodb.database import MemoryDatabase


class SimpleRecord:
    def __init__(self, x):
        self.x = x


class ComplexRecord:

    def __init__(self, x, y):
        self.x = x
        self.y = y


@pytest.fixture
def database(tmpdir):
    return MemoryDatabase()


@pytest.fixture
def test_objects():
    return [ComplexRecord(x, x) for x in range(1000)]


def test_iteration(database):
    database.insert(SimpleRecord(12))
    database.insert(ComplexRecord(12, 24))
    for item in database:
        assert item == SimpleRecord or item == ComplexRecord


def test_insert(database):
    database.insert(SimpleRecord(12))
    database.insert(ComplexRecord(12, 24))
    assert len(database) == 2


def test_retrieve_valid(database, test_objects):
    for item in test_objects:
        database.insert(item)
    item = list(database.select(ComplexRecord).retrieve(x=500))[0]
    assert item.x == 500


def test_retrieve_invalid(database, test_objects):
    for item in test_objects:
        database.insert(item)
    with pytest.raises(KeyError):
        list(database.select(SimpleRecord).retrieve(x=500))


def test_delete_valid(database, test_objects):
    for item in test_objects:
        database.insert(item)
    database.select(ComplexRecord).delete(x=500)
    items = list(database.select(ComplexRecord).retrieve(x=500))
    assert items == []
    assert len(database) == 999


def test_delete_invalid(database, test_objects):
    for item in test_objects:
        database.insert(item)
    with pytest.raises(KeyError):
        database.select(SimpleRecord).retrieve(x=500)
