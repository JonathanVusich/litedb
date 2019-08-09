import itertools
from io import BytesIO
from typing import List, Optional
from zlib import compress, decompress

from numpy import int64

from ..utils.serialization import serialize, deserialize


class Shard:

    def __init__(self, shard_size: int = 512) -> None:
        self.max_size: int = shard_size
        self.checksum: int64 = int64(0)
        self.binary_blobs: List[Optional[bytes]] = [None * self.max_size]
        self.items: List[Optional[object]] = [None * self.max_size]

    def __getitem__(self, key: int) -> object:
        if self.items[key] is None:
            object_bytes = self.binary_blobs[key]
            if object_bytes is None:
                raise RuntimeError
            deserialized_item = deserialize(decompress(object_bytes))
            self.items[key] = deserialized_item
            return deserialized_item
        return self.items[key]

    def __setitem__(self, key: int, value: object) -> None:
        self.items[key] = value
        # TODO: Replace this call with a byte constant for None
        self.binary_blobs[key] = compress(serialize(value))

    @classmethod
    def from_filesystem(cls, bytes: BytesIO, preferred_size: int):
        """This method loads a shard from a BytesIO instance, which will typically
        be a File object. The preferred size is simply meant as an upper bound on how
        large the shard will size itself to. If the shard's size is above the preferred
        size when loaded from the filesystem, it will simply trim itself as items get
        deleted until it is at the preferred size."""

        # Initialize the shard
        shard = cls(preferred_size)

        # Get the checksum
        shard.checksum = int64(bytes.read(8))

        # Alias inner arrays for faster lookups
        blobs = shard.binary_blobs
        items = shard.items

        for i in range(preferred_size):
            object_size: int64 = int64(bytes.read(8))
            if object_size == b"":
                break
            object_bytes = bytes.read(object_size.astype(int))
            blobs[i] = object_bytes
            items[i] = deserialize(decompress(object_bytes))

        return shard

    def to_bytes(self) -> bytes:
        byte_buffer = BytesIO()
        byte_buffer.write(bytes(self.checksum.data))
        for blob in self.binary_blobs:
            if blob is not None:
                object_size = int64(len(blob))
                byte_buffer.write(object_size)
                byte_buffer.write(blob)
        return byte_buffer.read()


