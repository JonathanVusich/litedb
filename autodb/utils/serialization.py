import os
import pickle

from .checksum import checksum
from ..errors import PathError


def serialize(item: object) -> bytes:
    return pickle.dumps(item, pickle.HIGHEST_PROTOCOL)


def deserialize(raw_data: bytes):
    return pickle.loads(raw_data)


def load(path: str):
    """
    This function returns a deserialized Python object.
    :param path:
    :return:
    """
    if not os.path.exists(path) or not os.path.isfile(path):
        return
    with open(path, "rb") as file:
        file.read(4)  # Discard the checksum
        pickle_data = file.read()
        return pickle.loads(pickle_data)


def dump(path: str, item: object) -> None:
    """
    This function saves an object to disk with a checksum.
    :param item:
    :param path:
    :return:
    """
    pickled_data = pickle.dumps(item, pickle.HIGHEST_PROTOCOL)
    chksum = checksum(pickled_data)
    try:
        if chksum == get_checksum(path):
            return
    except PathError:
        pass
    if not os.path.exists(os.path.dirname(path)):
        os.mkdir(os.path.dirname(path))
    with open(path, "wb") as file:
        file.write(chksum)
        file.write(pickled_data)


def get_checksum(path: str) -> bytes:
    """
    This is a utility function that returns the checksum
    for a given serialized object.
    :param path:
    :return:
    """
    if not os.path.exists(path) or not os.path.isfile(path):
        raise PathError
    with open(path, "rb") as file:
        chksum = file.read(4)
    return chksum
