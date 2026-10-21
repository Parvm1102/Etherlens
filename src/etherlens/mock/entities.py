"""Mock world entities — deterministic synthetic Ethereum world from a seed."""
import math
import random


def _addr(rng, prefix=None):
    """Generate a random Ethereum address."""
    if prefix is not None:
        return prefix + rng.randbytes(18).hex()
    return "0x" + rng.randbytes(20).hex()


def build_world(seed: int) -> dict:
    """Build the deterministic synthetic world from seed.

    Returns a dict with all entity addresses and metadata needed by the
    batch generator and stream mock.
    """
    rng = random.Random(seed)

    # 20,000 normal users — lognormal activity weights
    n_users = 20_000
    users = [_addr(rng) for _ in range(n_users)]
    user_weights = [math.exp(rng.gauss(0, 1.0)) for _ in range(n_users)]
    total_w = sum(user_weights)
    user_weights = [w / total_w for w in user_weights]

    # 300 contracts — lognormal popularity weights
    n_contracts = 300
    contracts = [_addr(rng) for _ in range(n_contracts)]
    contract_weights = [math.exp(rng.gauss(0, 1.0)) for _ in range(n_contracts)]
    total_cw = sum(contract_weights)
    contract_weights = [w / total_cw for w in contract_weights]

    # Hub — exchange deposit address (~8% of tx as plain transfers)
    hub = _addr(rng)

    # 8 bots — each sends ~1% daily traffic, fixed targets, gas_used=65000, nonce>=200000
    n_bots = 8
    bots = [_addr(rng) for _ in range(n_bots)]
    bot_targets = [contracts[i] for i in range(3)]  # 3 fixed contract targets
    bot_starting_nonces = [rng.randint(200_000, 300_000) for _ in range(n_bots)]

    # Wash ring: A→B→C→D→E→F→A (~20 hops/day, ~50 ETH)
    wash_ring = [_addr(rng) for _ in range(6)]

    # 15 whales — 1-5 tx/day, 500-20000 ETH
    n_whales = 15
    whales = [_addr(rng) for _ in range(n_whales)]

    # Per-user starting nonces (0-500)
    user_nonces = {addr: rng.randint(0, 500) for addr in users}
    # Bot nonces
    bot_nonces = {bots[i]: bot_starting_nonces[i] for i in range(n_bots)}
    # Wash ring nonces
    ring_nonces = {addr: rng.randint(0, 500) for addr in wash_ring}
    # Whale nonces
    whale_nonces = {addr: rng.randint(0, 100) for addr in whales}

    return {
        "seed": seed,
        "users": users,
        "user_weights": user_weights,
        "contracts": contracts,
        "contract_weights": contract_weights,
        "hub": hub,
        "bots": bots,
        "bot_targets": bot_targets,
        "bot_starting_nonces": bot_starting_nonces,
        "wash_ring": wash_ring,
        "whales": whales,
        "user_nonces": user_nonces,
        "bot_nonces": bot_nonces,
        "ring_nonces": ring_nonces,
        "whale_nonces": whale_nonces,
    }
