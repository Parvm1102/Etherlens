"""HyperLogLog — notes §6.7."""
import math

from etherlens.sketches.hashing import h64


class HyperLogLog:
    """HyperLogLog cardinality estimator.

    p: number of index bits; m = 2^p registers.
    standard_error ≈ 1.04 / sqrt(m)
    """

    def __init__(self, p: int = 14):
        self.p = p
        self.m = 1 << p
        self.M = bytearray(self.m)  # registers

    @property
    def standard_error(self) -> float:
        return 1.04 / math.sqrt(self.m)

    def add(self, x: str):
        h = h64(x)
        j = h >> (64 - self.p)
        w = h & ((1 << (64 - self.p)) - 1)
        # rho = position of leftmost 1-bit in w (within 64-p bits), counted from left
        # equals (64 - p) - w.bit_length() + 1
        rho = (64 - self.p) - w.bit_length() + 1
        if rho > self.M[j]:
            self.M[j] = rho

    def estimate(self) -> float:
        alpha_m = 0.7213 / (1 + 1.079 / self.m)
        Z = sum(2.0 ** (-v) for v in self.M)
        E = alpha_m * self.m * self.m / Z

        # Small-range correction: linear counting
        V = self.M.count(0)
        if E <= 2.5 * self.m and V > 0:
            E = self.m * math.log(self.m / V)

        return E
