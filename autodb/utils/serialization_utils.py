import pickle
import os
from typing import List, Dict, Optional
from .checksum_utils import checksum

from ..index import Index


def serialize(item: object) -> bytes:
    return pickle.dumps(item, pickle.HIGHEST_PROTOCOL)


def deserialize(raw_data: bytes) -> object:
    return pickle.loads(raw_data)


def load_table_index(index_path: str) -> Optional[Dict[str, Index]]:
    """
    This function returns a deserialized index map and index blacklist.
    :param index_path:
    :return:
    """
    if os.path.exists(index_path):
        with open(index_path, "rb") as f:
            return pickle.load(f)


def dump_table_index(index_path: str, index_info: dict) -> None:
    """
    This function dumps a dictionary of index info to the given index path.
    :param index_path:
    :param index_info:
    :return:
    """
    with open(index_path, "wb") as f:
        pickle.dump(index_info, f)


def load_shard(shard_path: str) -> List[bytes]:
    """
    This function returns a list of pickled Python objects
    :param shard_path:
    :return:
    """
    with open(shard_path, "rb") as file:
        file.read(4)
        return pickle.loads(file.read())


def dump_shard(shard_path: str, shard: List[bytes]) -> None:
    """
    This function saves a shard to disk.
    :param shard:
    :param shard_path:
    :return:
    """
    pickled_shard = pickle.dumps(shard, pickle.HIGHEST_PROTOCOL)
    with open(shard_path, "wb") as file:
        file.write(bytes(checksum(pickled_shard)))
        file.write(pickled_shard)


def get_checksum(shard_path: str) -> int:
    with open(shard_path, "rb") as file:
        chksum = int(file.read(4))
    return chksum


def load_table_info(info_path: str) -> dict:
    """
    This function returns a dictionary that contains saved state
    for a persistant table.
    :param info_path:
    :return:
    """
    return pickle.load(info_path)
