import pytest

from autodb.errors import InvalidRange
from autodb.index.index import Index
from autodb.index.persistent_index import PersistentIndex
from autodb.utils.serialization import dump_object
from ..test_table.table_test_objects import StandardTableObject, BadObject


@pytest.fixture()
def table_dir(tmpdir):
    return tmpdir.mkdir("table")


@pytest.fixture()
def index_manager(table_dir, tmpdir):
    index_dir = table_dir.join("index")
    return PersistentIndex(str(index_dir))


@pytest.fixture(scope="session")
def index():
    return Index(str)


def test_no_index_file_init(table_dir, index_manager):
    assert index_manager.index_path == str(table_dir.join("index"))
    assert index_manager.index_blacklist == set()
    assert index_manager.index_map == {}


def test_prior_index_file_init(table_dir, tmpdir, index):
    index_map = {"test": index}
    index_blacklist = {"_bad"}
    index_dir = tmpdir.mkdir("table", "index")
    dump_object(index_dir.join("map"), index_map)
    dump_object(index_dir.join("blacklist"), index_blacklist)
    manager = PersistentIndex(index_dir)
    assert manager.index_blacklist == {"_bad"}
    assert manager.index_map == {"test": index}


def test_persist_load(table_dir, tmpdir, index):
    index_dir = tmpdir.mkdir("table", "index")
    manager = PersistentIndex(index_dir)
    manager.index_blacklist = {"_bad"}
    manager.index_map = {"test": index}
    manager.persist()
    new_manager = PersistentIndex(index_dir)
    assert new_manager.index_blacklist == {"_bad"}
    assert new_manager.index_map == {"test": index}


def test_index_item(index_manager, table_dir):
    sto = StandardTableObject(12, -12)
    index_manager.index_item(sto, 12)
    assert index_manager.index_blacklist == set()
    index1 = Index(int)
    index1.add(12, 12)
    index2 = Index(int)
    index2.add(-12, 12)
    assert index_manager.index_map == {"x": index1, "y": index2}

    # Clear out the old index
    index_manager = PersistentIndex(str(table_dir.join("index")))

    # test index blacklist
    bo = BadObject(12)
    bo2 = BadObject(13)
    index_manager.index_item(bo, 12)
    assert index_manager.index_blacklist == set()
    assert 'bad_index' in index_manager.index_map
    index_manager.index_item(bo2, 13)
    assert index_manager.index_blacklist == {'bad_index'}
    assert index_manager.index_map == {}
    bo3 = BadObject(14)
    index_manager.index_item(bo3, 14)
    assert index_manager.index_blacklist == {'bad_index'}
    assert index_manager.index_map == {}


def test_unindex_item(index_manager):
    sto = StandardTableObject(12, -12)
    index_manager.index_item(sto, 12)
    index_manager.unindex_item(sto, 12)
    assert index_manager.index_blacklist == set()
    index1 = Index(int)
    index2 = Index(int)
    assert index_manager.index_map == {"x": index1, "y": index2}


def test_table_retrieve_well_formed_queries(index_manager):
    for i in range(10):
        index_manager.index_item(StandardTableObject(i, -i), i)

    # test x queries

    indexes = index_manager.retrieve(x=9)
    assert len(indexes) == 1
    assert 9 in indexes

    assert not index_manager.retrieve(x=10)

    indexes = index_manager.retrieve(x=(1, 3))
    assert len(indexes) == 3
    assert indexes == {1, 2, 3}

    # test y queries

    indexes = index_manager.retrieve(y=-9)
    assert len(indexes) == 1
    assert indexes == {9}

    assert not index_manager.retrieve(y=-10)

    indexes = index_manager.retrieve(y=(-3, -1))
    assert len(indexes) == 3
    assert indexes == {1, 2, 3}

    # Check multiple queries
    items = index_manager.retrieve(x=9, y=-9)
    assert len(items) == 1
    assert items == {9}

    assert not index_manager.retrieve(x=9, y=9)


def test_table_retrieve_bad_queries(index_manager):

    for i in range(10):
        index_manager.index_item(StandardTableObject(i, -i), i)

    with pytest.raises(IndexError):
        index_manager.retrieve(z=12)

    with pytest.raises(InvalidRange):
        index_manager.retrieve(x=(1, 3, 5))

    with pytest.raises(ValueError):
        index_manager.retrieve(x=b"test")

    with pytest.raises(ValueError):
        index_manager.retrieve(x=(b"test", b"test2"))

    with pytest.raises(ValueError):
        index_manager.retrieve(x=(1, b"test"))

