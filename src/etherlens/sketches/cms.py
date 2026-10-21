"""Count-Min Sketch — notes §6.6."""
import math

from etherlens.sketches.hashing import h_pair


class CountMinSketch:
    """Count-Min Sketch for frequency estimation.

    eps: error bound as fraction of total count N
    delta: failure probability (estimate exceeds true + eps*N with prob <= delta)
    w = ceil(e / eps), d = ceil(ln(1/delta))
    """

    def __init__(self, eps: float = 0.001, delta: float = 0.01):
        self.eps = eps
        self.delta = delta
        self.w = math.ceil(math.e / eps)
        self.d = math.ceil(math.log(1.0 / delta))
        self.table = [[0] * self.w for _ in range(self.d)]
        self.N = 0
        self._candidates: dict[str, int] = {}

    @property
    def memory_bytes(self) -> int:
        return self.w * self.d * 4

    @property
    def error_bound(self) -> float:
        return self.eps * self.N

    def _buckets(self, x: str):
        h1, h2 = h_pair(x)
        for i in range(self.d):
            yield i, (h1 + i * h2) % self.w

    def add(self, x: str, c: int = 1):
        self.N += c
        for i, col in self._buckets(x):
            self.table[i][col] += c
        est = self.estimate(x)
        self._candidates[x] = est
        if len(self._candidates) > 10_000:
            # Prune to 5,000 largest
            top = sorted(self._candidates.items(), key=lambda kv: kv[1], reverse=True)[:5_000]
            self._candidates = dict(top)

    def estimate(self, x: str) -> int:
        return min(self.table[i][col] for i, col in self._buckets(x))

    def top_k(self, k: int) -> list[tuple[str, int]]:
        return sorted(self._candidates.items(), key=lambda kv: kv[1], reverse=True)[:k]
