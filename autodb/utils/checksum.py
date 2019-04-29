from zlib import adler32
import pickle


def checksum(item: object) -> bytes:
    pickled_item = pickle.dumps(item, pickle.HIGHEST_PROTOCOL)
    return (adler32(pickled_item) & 0xffffffff).to_bytes(4, byteorder='big')
