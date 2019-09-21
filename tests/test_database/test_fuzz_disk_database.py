import cProfile
from pytest import fixture
import os
from pcpartpicker import API
from random import choice, shuffle
import pickle
from autodb import DiskDatabase


@fixture
def test_data():
    test_data = "fuzz_test"
    if not os.path.exists(test_data):
        api = API()
        data = api.retrieve_all()
        parts = {}
        values = list(data.values())
        for i in range(5000):
            test_parts: list = choice(values)
            while not test_parts:
                test_parts = choice(values)
            part = choice(test_parts)
            part_key = type(part).__name__
            if part_key in parts:
                parts[part_key].add(part)
            else:
                parts[part_key] = {part}
        with open(test_data, "wb") as file:
            pickle.dump(parts, file)
            return parts
    else:
        with open(test_data, "rb") as file:
            return pickle.load(file)


@fixture
def database(tmpdir) -> DiskDatabase:
    return DiskDatabase(tmpdir.mkdir("database"))


def format_indexes(indexes: list, item: object) -> dict:
    return dict((((index, getattr(item, index)) for index in indexes)))


def test_fuzz(test_data, database: DiskDatabase):
    profiler = cProfile.Profile()
    profiler.enable()
    for key, parts in test_data.items():
        database.batch_insert(list(parts))
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
    profiler.disable()
    profiler.dump_stats("test2.prof")