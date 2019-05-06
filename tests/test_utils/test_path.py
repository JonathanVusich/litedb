import os
from autodb.utils.path import is_info, is_index, is_shard, is_table, valid_table_contents
import tempfile


def test_is_table():
    assert is_table("0xasdf23fwdg22fwde")


def test_is_shard():
    assert is_shard("shard01")
    assert not is_shard("shard_01")
    assert is_shard("shard12345")
    assert not is_shard("shard")


def test_is_index():
    assert is_index("index")
    assert not is_index("index1")


def test_is_info():
    assert is_info("info")
    assert not is_info("info1")


def test_is_valid_table_directory():
    with tempfile.TemporaryDirectory() as tempdir:
        with open(os.path.join(tempdir, "index"), "wb"):
            pass
        with open(os.path.join(tempdir, "info"), "wb"):
            pass
        with open(os.path.join(tempdir, "shard0"), "wb"):
            pass

        assert valid_table_contents(tempdir)

        # remove index file
        os.remove(os.path.join(tempdir, "index"))

        assert not valid_table_contents(tempdir)

        # Add index file back and remove shard file
        with open(os.path.join(tempdir, "index"), "wb"):
            pass
        os.remove(os.path.join(tempdir, "shard0"))

        assert not valid_table_contents(tempdir)
