import pickle
from hashlib import blake2b
from zlib import adler32


def checksum(item: object) -> bytes:
    """Returns a checksum of the given object."""
    return blake2b(pickle.dumps(item, pickle.HIGHEST_PROTOCOL)).digest()


def chksum(byte_string: bytes) -> int:
    """Returns a fast checksum of the given byte string."""
    return adler32(byte_string)
