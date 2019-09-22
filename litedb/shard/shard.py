import sys
from io import BytesIO
from typing import List, Optional

from ..utils.checksum import chksum
from ..utils.serialization import serialize, deserialize


class Shard:
    # Constants for None so they don't constantly have to be recalculated
    none_constant = b'\x80\x03N.'
    none_hash = chksum(none_constant)

    def __init__(self, shard_size: int = 512) -> None:
        self.max_size: int = shard_size
        self.checksum = 0
        self.binary_blobs: List[Optional[bytes]] = [None] * self.max_size

    def __getitem__(self, key: int) -> object:
        object_bytes = self.binary_blobs[key]
        return object_bytes if object_bytes is None else deserialize(object_bytes)

    def __setitem__(self, key: int, value: object) -> None:
        if value is None:  # We are removing this object from the database...
            # Null out the field in this shard and update the checksum
            self.binary_blobs[key] = self.none_constant
            self.checksum = self.checksum ^ self.none_hash
        else:
            # Convert the object to bytes, add it to the shard and update the checksum
            self.binary_blobs[key] = serialize(value)
            self.checksum = self.checksum ^ chksum(self.binary_blobs[key])

    @classmethod
    def from_bytes(cls, bytes: BytesIO, size: int = 512):
        """Converts the given BytesIO object into a Shard instance."""

        # Seek back to the beginning of this byte buffer
        bytes.seek(0)

        # Initialize the shard
        shard = cls(size)

        # Get the checksum
        num = int.from_bytes(bytes.read(4), byteorder=sys.byteorder, signed=False)
        shard.checksum = num

        # Alias inner arrays for faster lookups
        blobs = shard.binary_blobs

        for i in range(size):
            size_bytes = bytes.read(8)
            if size_bytes == b"":
                break
            object_size: int = int.from_bytes(size_bytes, byteorder=sys.byteorder, signed=False)
            object_bytes = bytes.read(object_size)
            blobs[i] = object_bytes

        return shard

    def to_bytes(self) -> BytesIO:
        """Converts this shard into a BytesIO instance that can then be written to disk."""

        # Create bytebuffer
        byte_buffer = BytesIO()

        # Convert checksum to bytes and write to buffer
        checksum = self.checksum.to_bytes(4, byteorder=sys.byteorder, signed=False)
        byte_buffer.write(checksum)

        # Convert stored objects to bytes and write to buffer
        for blob in self.binary_blobs:
            if blob is not None:
                object_size = len(blob)
                object_size = object_size.to_bytes(8, byteorder=sys.byteorder, signed=False)
                byte_buffer.write(object_size)
                byte_buffer.write(blob)

        # Seek back to the beginning of the byte buffer and return
        byte_buffer.seek(0)
        return byte_buffer
