from zlib import adler32


def checksum(item: bytes) -> bytes:
    return (adler32(item) & 0xffffffff).to_bytes(4, byteorder='big')
