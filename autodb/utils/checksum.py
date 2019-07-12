from hashlib import blake2b
import pickle


def checksum(item: object) -> bytes:
    """Returns a blake2b hash of a given object."""
    pickled_item = pickle.dumps(item, pickle.HIGHEST_PROTOCOL)
    return blake2b(pickled_item).digest()
