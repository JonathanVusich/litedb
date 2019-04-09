import pickle
from typing import List, Dict

from ..index import Index


def load_table_index(index_path: str) -> Dict[str, Index]:
    """
    This function returns a deserialized index map and the filenames of
    the existing table shards.
    :param index_path:
    :return:
    """
    return pickle.load(index_path, pickle.HIGHEST_PROTOCOL)


def load_shard(shard_path: str) -> List[bytes]:
    """
    This function returns a list of pickled Python objects
    :param shard_path:
    :return:
    """
    return pickle.load(shard_path, pickle.HIGHEST_PROTOCOL)


def load_table_info(info_path: str) -> dict:
    """
    This function returns a dictionary that contains saved state
    for a persistant table.
    :param info_path:
    :return:
    """
    return pickle.load(info_path, pickle.HIGHEST_PROTOCOL)
