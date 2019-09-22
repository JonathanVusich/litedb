import os
import pickle
import sys
from io import BytesIO
from typing import Optional

from ..errors import PathError


def serialize(item: object) -> bytes:
    """Serializes the given object using pickle."""
    return pickle.dumps(item, pickle.HIGHEST_PROTOCOL)


def deserialize(raw_data: bytes):
    """Deserializes bytes to object using pickle."""
    return pickle.loads(raw_data)


def load_object(path: str) -> object:
    """Load python object from disk using pickle. Used to load indexes and other attributes."""
    if not os.path.exists(path) or not os.path.isfile(path):
        return
    with open(path, "rb") as file:
        return pickle.load(file)


def dump_object(path: str, item: object) -> None:
    """Persists object to disk using pickle. Used to save indexes and other attributes."""
    if not os.path.exists(os.path.dirname(path)):
        os.mkdir(os.path.dirname(path))
    with open(path, "wb") as file:
        pickle.dump(item, file, pickle.HIGHEST_PROTOCOL)


def load_shard(path: str) -> Optional[BytesIO]:
    """
    This function returns a serialized Python object.
    :param path:
    :return:
    """
    if not os.path.exists(path) or not os.path.isfile(path):
        return
    with open(path, "rb") as file:
        file.seek(0)
        bytes_io = BytesIO(file.read())
        bytes_io.seek(0)
        return bytes_io


def dump_shard(path: str, item: BytesIO) -> None:
    """
    This function saves an object with a checksum to disk.
    :param item:
    :param path:
    :return:
    """
    if not os.path.exists(os.path.dirname(path)):
        os.mkdir(os.path.dirname(path))
    with open(path, "wb") as file:
        file.seek(0)
        file.write(item.read())
        file.truncate()


def get_checksum(path: str) -> int:
    """
    This is a utility function that returns the checksum
    for a given serialized object.
    :param path:
    :return:
    """
    if not os.path.exists(path) or not os.path.isfile(path):
        raise PathError
    with open(path, "rb") as file:
        chksum_bytes = file.read(4)
    return int.from_bytes(chksum_bytes, sys.byteorder)
