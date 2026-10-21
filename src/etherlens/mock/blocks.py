"""MockChain — yields §6.4 stream records. No pyspark imports."""
import datetime
import random
import time

from etherlens.mock.entities import build_world


class MockChain:
    """Yields stream records conforming to §6.4 schema.

    Every 25th block is a burst (5× tx).
    2% of tx come from flagged addresses (bots).
    A spammer sends 30% of tx during a 10-block episode every 100 blocks.
    """

    def __init__(self, seed: int, flagged_addresses: list[str], interval_s: int = 12):
        self.seed = seed
        self.flagged_addresses = list(flagged_addresses)
        self.interval_s = interval_s
        self._world = build_world(seed)
        self._rng = random.Random(seed + 999)
        self._spammer = "0x" + self._rng.randbytes(20).hex()

    def _make_tx(self, rng, block_number, block_ts_str, sender=None):
        world = self._world
        if sender is None:
            sender = rng.choices(world["users"], weights=world["user_weights"])[0]
        roll = rng.random()
        if roll < 0.45:
            to = world["hub"] if rng.random() < 0.1 else rng.choices(world["users"])[0]
            gas_limit = 21000
        elif roll < 0.997:
            to = rng.choices(world["contracts"], weights=world["contract_weights"])[0]
            gas_limit = rng.randint(30_000, 300_000)
        else:
            to = None
            gas_limit = rng.randint(500_000, 3_000_000)

        gas_price_wei = rng.randint(8_000_000_000, 40_000_000_000)
        nonce = rng.randint(0, 999_999)
        tx_type = 2 if rng.random() < 0.9 else 0
        value_wei = str(int(rng.uniform(0, 10) * 1e18))

        return {
            "hash": "0x" + rng.randbytes(32).hex(),
            "block_number": block_number,
            "block_timestamp": block_ts_str,
            "from_address": sender.lower(),
            "to_address": to.lower() if to else None,
            "value_wei": value_wei,
            "gas_limit": gas_limit,
            "gas_price_wei": gas_price_wei,
            "nonce": nonce,
            "tx_type": tx_type,
        }

    def blocks(self):
        """Yield (block_number, list_of_tx_dicts) indefinitely."""
        rng = self._rng
        block_number = 21_500_000
        now = datetime.datetime.now(datetime.timezone.utc)
        block_time = now

        while True:
            ts_str = block_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            base_tx = 150

            # Burst every 25th block
            is_burst = (block_number % 25 == 0)
            n_tx = base_tx * 5 if is_burst else base_tx

            # Spammer episode: 30% of tx during 10-block window every 100 blocks
            in_spammer_episode = ((block_number % 100) < 10)

            txs = []
            for _ in range(n_tx):
                r = rng.random()
                if r < 0.02 and self.flagged_addresses:
                    sender = rng.choice(self.flagged_addresses)
                elif in_spammer_episode and rng.random() < 0.30:
                    sender = self._spammer
                else:
                    sender = None
                txs.append(self._make_tx(rng, block_number, ts_str, sender))

            yield block_number, txs

            block_number += 1
            block_time += datetime.timedelta(seconds=self.interval_s)

    def stream(self, sleep: bool = True):
        """Yield individual tx dicts, sleeping interval_s between blocks."""
        for block_number, txs in self.blocks():
            for tx in txs:
                yield tx
            if sleep:
                time.sleep(self.interval_s)
