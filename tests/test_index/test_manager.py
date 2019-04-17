import pytest
from autodb.index.manager import IndexManager
from autodb.index.index import Index
import pickle
from ..test_table.table_test_objects import StandardTableObject


@pytest.fixture()
def table_dir(tmpdir):
    return tmpdir.mkdir("table")


@pytest.fixture()
def index_manager(table_dir, tmpdir):
    index_dir = table_dir.join("index")
    return IndexManager(str(index_dir))


@pytest.fixture(scope="session")
def index():
    return Index(str)


def test_no_index_file_init(table_dir, index_manager):
    assert index_manager.index_path == str(table_dir.join("index"))
    assert index_manager.index_blacklist == set()
    assert index_manager.index_map == {}


def test_prior_index_file_init(table_dir, index):
    index_info = {"map": {"test": index}, "blacklist": {"_bad"}}
    with open(table_dir.join("index"), "wb") as f:
        pickle.dump(index_info, f)
    manager = IndexManager(str(table_dir.join("index")))
    assert manager.index_blacklist == {"_bad"}
    assert manager.index_map == {"test": index}


def test_persist_load(table_dir, index):
    manager = IndexManager(str(table_dir.join("index")))
    manager.index_blacklist = {"_bad"}
    manager.index_map = {"test": index}
    manager.index_attributes = {"good": {1}}
    manager.persist()
    new_manager = IndexManager(str(table_dir.join("index")))
    assert new_manager.index_blacklist == {"_bad"}
    assert new_manager.index_map == {"test": index}


def test_index_item(index_manager):
    sto = StandardTableObject(12, -12)
    index_manager.index_item(sto, 12)
    assert index_manager.index_blacklist == set()
    index1 = Index(int)
    index1.add(12, 12)
    index2 = Index(int)
    index2.add(-12, 12)
    assert index_manager.index_map == {"x": index1, "y": index2}


def test_unindex_item(index_manager):
    sto = StandardTableObject(12, -12)
    index_manager.index_item(sto, 12)
    index_manager.unindex_item(sto, 12)
    assert index_manager.index_blacklist == set()
    index1 = Index(int)
    index2 = Index(int)
    assert index_manager.index_map == {"x": index1, "y": index2}

