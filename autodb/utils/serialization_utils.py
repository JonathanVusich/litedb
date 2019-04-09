import pickle
from typing import List, Dict

from ..index import Index


def serialize(item: object) -> bytes:
    return pickle.dumps(item, pickle.HIGHEST_PROTOCOL)


def deserialize(raw_data: bytes) -> object:
    return pickle.loads(raw_data)


def load_table_index(index_path: str) -> Dict[str, Index]:
    """
    This function returns a deserialized index map and the filenames of
    the existing table shards.
    :param index_path:
    :return:
    """
    return pickle.load(index_path)


def load_shard(shard_path: str) -> List[bytes]:
    """
    This function returns a list of pickled Python objects
    :param shard_path:
    :return:
    """
    with open(shard_path, "rb") as file:
        return pickle.load(file)


def dump_shard(shard_path: str, shard: List[bytes]) -> None:
    """
    This function saves a shard to disk.
    :param shard:
    :param shard_path:
    :return:
    """
    with open(shard_path, "wb") as file:
        pickle.dump(shard, file, pickle.HIGHEST_PROTOCOL)


def load_table_info(info_path: str) -> dict:
    """
    This function returns a dictionary that contains saved state
    for a persistant table.
    :param info_path:
    :return:
    """
    return pickle.load(info_path)
