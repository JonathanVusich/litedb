import pickle
from random import shuffle

from pytest import fixture

from litedb import DiskDatabase, MemoryDatabase
from litedb.abc import Database


@fixture
def test_data():
    with open("tests/test_database/fuzz_test", "rb") as file:
        return pickle.load(file)


@fixture
def database_dir(tmpdir) -> str:
    return tmpdir.mkdir("database")


def format_indexes(indexes: list, item: object) -> dict:
    return dict((((index, getattr(item, index)) for index in indexes)))


def test_fuzz_disk(test_data, database_dir: str):
    database = DiskDatabase(database_dir)

    for key, parts in test_data.items():
        database.batch_insert(list(parts))
        database.commit()

    del database

    database = DiskDatabase(database_dir)

    verify_database(test_data, database)

    database.commit()

    del database

    database = DiskDatabase(database_dir)

    assert len(database) == 0


def test_fuzz_memory(test_data):
    database = MemoryDatabase()
    for key, parts in test_data.items():
        for part in parts:
            database.insert(part)

    verify_database(test_data, database)

    assert len(database) == 0
    

def verify_database(test_data, database: Database):
    for key, parts in test_data.items():
        parts = list(parts)
        clazz = parts[0].__class__
        valid_indexes = database.select(clazz).indexes
        shuffle(parts)
        for part in parts:
            current_size = len(database.select(clazz))
            indexes = format_indexes(valid_indexes, part)
            database_parts = list(database.select(clazz).retrieve(**indexes))
            assert len(database_parts) == 1
            database_part = database_parts[0]
            assert database_part == part
            database.select(clazz).delete(**indexes)
            assert current_size == len(database.select(clazz)) + 1
            assert not list(database.select(clazz).retrieve(**indexes))