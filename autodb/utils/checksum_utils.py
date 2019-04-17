from zlib import adler32


def checksum(item: bytes) -> int:
    return adler32(item) & 0xffffffff
