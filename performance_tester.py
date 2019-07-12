import time
from dataclasses import asdict
from decimal import Decimal

from pcpartpicker.parts import Memory
from tinydb import TinyDB, Query

from autodb import DiskDatabase, MemoryDatabase


def main():
    parts = []
    mem_database = MemoryDatabase()
    database = DiskDatabase("C:/Users/apian/Desktop/autodb/")
    for key in database:
        table = database.select(key).retrieve_all()
        for part in (list(table)):
            parts.append(part)
    print(f"Total memory parts to test with: {len(parts)}")
    tinydb_database = TinyDB("C:/Users/apian/Desktop/tinydb/db.json")
    print("Inserting into TinyDB...")
    parts = parts * 10
    start = time.perf_counter()
    parts = [dict((key, value) for key, value in asdict(part).items() if
                  isinstance(value, (int, str, float, Decimal, bool, dict, list, set, frozenset, tuple, bytes))) for
             part in parts]
    tinydb_database.insert_multiple(parts)
    print(f"Total time: {time.perf_counter() - start} secs")

    part_lists = []
    autodb = DiskDatabase("C:/Users/apian/Desktop/testdb/")
    for key in database:
        table = database.select(key).retrieve_all()
        part_lists.append(list(table))
    part_lists = part_lists * 10
    start = time.perf_counter()
    print("Inserting into AutoDB...")
    for parts in part_lists:
        autodb.batch_insert(parts)
    print(f"Total time: {time.perf_counter() - start} secs")

    start = time.perf_counter()
    print("Searching TinyDB for GeIL memory...")
    geil = Query()
    search_results = tinydb_database.search(geil.brand == "GeIL")
    print(f"Total time: {time.perf_counter() - start} secs")

    start = time.perf_counter()
    print("Searching AutoDB for GeIL memory...")
    results = list(autodb.select(Memory).retrieve(brand="GeIL"))
    print(f"Total time: {time.perf_counter() - start} secs")


if __name__ == "__main__":
    main()
