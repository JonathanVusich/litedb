import time
from autodb.index import Index


def test_index_performance():
    index = Index(int)
    start = time.perf_counter()
    for i in range(10000):
        index.add(i, i)
    time_to_complete = time.perf_counter() - start
    assert time_to_complete < .1

    start = time.perf_counter()
    for i in range(10000):
        value = index.retrieve(i)
    time_to_complete = time.perf_counter() - start
    assert time_to_complete < 0.04

    start = time.perf_counter()
    for i in range(1000):
        values = index.retrieve_range(None, i)
        assert isinstance(values, frozenset)
        assert len(values) == i + 1
    time_to_complete = time.perf_counter() - start
    assert time_to_complete < 0.2
