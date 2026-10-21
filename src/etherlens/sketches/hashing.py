"""Hashing helpers for sketch data structures."""
import hashlib
import struct


def h64(item: str, salt: int = 0) -> int:
    """First 8 bytes of blake2b(item, digest_size=8, salt=salt) as unsigned int."""
    salt_bytes = salt.to_bytes(16, "little")
    digest = hashlib.blake2b(item.encode(), digest_size=8, salt=salt_bytes).digest()
    return struct.unpack(">Q", digest)[0]


def h_pair(item: str) -> tuple[int, int]:
    """Two 64-bit halves of a 16-byte blake2b digest; used for double hashing."""
    digest = hashlib.blake2b(item.encode(), digest_size=16).digest()
    h1 = struct.unpack(">Q", digest[:8])[0]
    h2 = struct.unpack(">Q", digest[8:])[0]
    return h1, h2
