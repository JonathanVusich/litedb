import pytest
from autodb.table import Table
from ..test_utils.objects import ObjectNoClassVars, ObjectClassVars, ObjectUnderscoreVars


def test_table_init():
    item = ObjectClassVars()
    table = Table(item)
    assert table.table_type == type(item)
    assert len(table.unused_indexes) == 0
    assert len(table) == 1
    assert len(table.index_map) == 5


def test_table_insert():
    item = ObjectClassVars()
    table = Table(item)
    table.insert(item)
    assert table.table_type == type(item)
    assert len(table.unused_indexes) == 0
    assert len(table) == 2
    assert len(table.index_map) == 5


def test_table_bad_insert():
    item = ObjectClassVars()
    table = Table(item)
    item = ObjectNoClassVars()
    with pytest.raises(ValueError):
        table.insert(item)
