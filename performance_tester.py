import time
from autodb.index import Index
from autodb.table import Table
from tests.test_utils.objects import ObjectClassVars

def main():
    item = ObjectClassVars()
    table = Table(item)
    start = time.perf_counter()
    for i in range(1000000):
        item = ObjectClassVars()
        table.insert(item)
    print(time.perf_counter() - start)


if __name__ == "__main__":
    main()