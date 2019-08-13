import os
import pickle
import sys
from io import BytesIO
from typing import Optional

from ..errors import PathError


def serialize(item: object) -> bytes:
    return pickle.dumps(item, pickle.HIGHEST_PROTOCOL)


def deserialize(raw_data: bytes):
    return pickle.loads(raw_data)


def load(path: str) -> Optional[bytes]:
    """
    This function returns a deserialized Python object.
    :param path:
    :return:
    """
    if not os.path.exists(path) or not os.path.isfile(path):
        return
    with open(path, "rb") as file:
        file.read(4)  # Discard the checksum
        return file.read()


def dump(path: str, item: BytesIO) -> None:
    """
    This function saves an object to disk with a checksum.
    :param item:
    :param path:
    :return:
    """
    with open(path, "wb") as file:
        file.write(item.read())


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
