import os
import tempfile
import pytest

from autodb.errors import DatabaseNotFound
from autodb.utils.path import load_tables, is_shard, is_index, is_table, is_info, \
    valid_table_contents


def test_load_database_well_formed():
    with tempfile.TemporaryDirectory() as tempdir:
        table_dir = os.path.join(tempdir, "0x1234567890123456")
        os.mkdir(table_dir)
        index_path = os.path.join(table_dir, "index")
        with open(index_path, "wb"):
            pass
        info_path = os.path.join(table_dir, "info")
        with open(info_path, "wb"):
            pass
        shard0_path = os.path.join(table_dir, "shard0")
        with open(shard0_path, "wb"):
            pass
        tables = list(load_tables(tempdir))
        assert len(tables) == 1
        table = tables[0]
        assert table.path == table_dir


def test_load_database_with_junk_files():
    with tempfile.TemporaryDirectory() as tempdir:
        table_dir = os.path.join(tempdir, "0x1234567890123456")
        os.mkdir(table_dir)

        # Add junk table directories
        os.mkdir(os.path.join(tempdir, "table1234d"))
        os.mkdir(os.path.join(tempdir, "table2df21"))

        index_path = os.path.join(table_dir, "index")
        with open(index_path, "wb"):
            pass

        info_path = os.path.join(table_dir, "info")
        with open(info_path, "wb"):
            pass

        # Add junk index file
        with open(os.path.join(table_dir, "index12"), "wb"):
            pass

        shard0_path = os.path.join(table_dir, "shard0")
        with open(shard0_path, "wb"):
            pass

        # Add junk shards
        with open(os.path.join(table_dir, "shardd1"), "wb"):
            pass
        with open(os.path.join(table_dir, "shard2g"), "wb"):
            pass

        tables = list(load_tables(tempdir))
        assert len(tables) == 1
        table = tables[0]
        assert table.path == table_dir


def test_load_database_no_database():
    with tempfile.TemporaryDirectory() as tempdir:
        table_dir = os.path.join(tempdir, "0x1234567890123456")
        os.mkdir(table_dir)
        shard0_path = os.path.join(table_dir, "shard0")
        with open(shard0_path, "wb"):
            pass
        with pytest.raises(DatabaseNotFound):
            list(load_tables(tempdir))
