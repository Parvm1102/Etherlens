"""Bloom filter — notes §6.8.

Notation used here vs. notes:
  This code    Notes
  m            m  (number of bits)
  k            k  (number of hash functions)
  n_expected   n  (expected number of insertions)
  fp_target    p  (target false-positive rate)
"""
import math

from etherlens.sketches.hashing import h_pair


class BloomFilter:
    """Bloom filter for approximate set membership.

    n_expected: anticipated number of elements
    fp_target: desired false-positive probability
    """

    def __init__(self, n_expected: int, fp_target: float = 0.01):
        self.n_expected = n_expected
        self.fp_target = fp_target
        # m = ceil(-n * ln(p) / (ln 2)^2)
        ln2 = math.log(2)
        self.m = math.ceil(-n_expected * math.log(fp_target) / (ln2 ** 2))
        # k = max(1, round((m/n) * ln 2))
        self.k = max(1, round((self.m / n_expected) * ln2))
        self._bits = bytearray((self.m + 7) // 8)
        self._n = 0

    def _bit_positions(self, x: str):
        h1, h2 = h_pair(x)
        for i in range(self.k):
            yield (h1 + i * h2) % self.m

    def _set(self, pos: int):
        self._bits[pos >> 3] |= 1 << (pos & 7)

    def _get(self, pos: int) -> bool:
        return bool(self._bits[pos >> 3] & (1 << (pos & 7)))

    def add(self, x: str):
        for pos in self._bit_positions(x):
            self._set(pos)
        self._n += 1

    def __contains__(self, x: str) -> bool:
        return all(self._get(pos) for pos in self._bit_positions(x))

    def theoretical_fp(self, n_inserted: int) -> float:
        return (1 - math.exp(-self.k * n_inserted / self.m)) ** self.k
