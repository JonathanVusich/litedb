from zlib import adler32
from hashlib import blake2b
import pickle


def checksum(item: object) -> bytes:
    return blake2b(pickle.dumps(item, pickle.HIGHEST_PROTOCOL)).digest()


def chksum(byte_string: bytes) -> int:
    return adler32(byte_string)
