import pytest

from litedb.database import DiskDatabase
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
    for item in test_objects:
        database.insert(item)
    assert len(database) == 1000


def test_inserting_bad_objects(database):
    with pytest.raises(TypeError):
        database.insert(list())
    with pytest.raises(TypeError):
        database.insert(set())
    with pytest.raises(TypeError):
        database.insert(dict())
    with pytest.raises(TypeError):
        database.insert(frozenset())
    with pytest.raises(TypeError):
        database.insert("HI")
    with pytest.raises(TypeError):
        database.insert(True)
    with pytest.raises(TypeError):
        database.insert(123)
    with pytest.raises(TypeError):
        database.insert(123.12)
    with pytest.raises(TypeError):
        database.insert(tuple())
    with pytest.raises(TypeError):
        database.insert(bytes("HI"))
    with pytest.raises(TypeError):
        database.insert(complex(2, 4))
    with pytest.raises(TypeError):
        database.insert(bytearray("1234"))
    with pytest.raises(TypeError):
        database.insert(frozenset())
    with pytest.raises(TypeError):
        database.insert(range(3))


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
