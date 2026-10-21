"""Reservoir sampling — notes §6.5, Algorithm R."""
import random


class Reservoir:
    """Algorithm R reservoir sampler.

    s: sample size
    seed: RNG seed for reproducibility
    """

    def __init__(self, s: int, seed: int = 0):
        self.s = s
        self._rng = random.Random(seed)
        self._sample: list = []
        self._i = 0  # 1-indexed count of items seen

    def add(self, item):
        self._i += 1
        if self._i <= self.s:
            self._sample.append(item)
        else:
            j = self._rng.randint(1, self._i)
            if j <= self.s:
                self._sample[j - 1] = item

    def sample(self) -> list:
        return list(self._sample)
