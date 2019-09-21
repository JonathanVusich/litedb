import pytest

from autodb.database import DiskDatabase
from .test_database import SimpleRecord


class ComplexRecord:

    def __init__(self, x, y):
        self.x = x
        self.y = y


@pytest.fixture
def database(tmpdir):
    return DiskDatabase(tmpdir.mkdir("database"))


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


def test_batch_insert_valid_objects(test_objects, database):
    database.batch_insert(test_objects)
    assert len(database) == 1000


def test_batch_insert_invalid_objects(database):
    test_objects = []
    for x in range(1000):
        if x % 2 == 0:
            test_objects.append(ComplexRecord(x, x))
        else:
            test_objects.append(SimpleRecord(x))
    with pytest.raises(ValueError):
        database.batch_insert(test_objects)


def test_retrieve_valid(database, test_objects):
    database.batch_insert(test_objects)
    item = list(database.select(ComplexRecord).retrieve(x=500))[0]
    assert item.x == 500


def test_retrieve_invalid(database, test_objects):
    database.batch_insert(test_objects)
    with pytest.raises(KeyError):
        list(database.select(SimpleRecord).retrieve(x=500))


def test_delete_valid(database, test_objects):
    database.batch_insert(test_objects)
    database.select(ComplexRecord).delete(x=500)
    items = list(database.select(ComplexRecord).retrieve(x=500))
    assert items == []
    assert len(database) == 999


def test_delete_invalid(database, test_objects):
    database.batch_insert(test_objects)
    with pytest.raises(KeyError):
        database.select(SimpleRecord).retrieve(x=500)
