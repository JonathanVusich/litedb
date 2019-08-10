from zlib import adler32


def checksum(byte_string: bytes) -> int:
    """Returns an adler32 checksum of the given byte string."""
    return adler32(byte_string)
