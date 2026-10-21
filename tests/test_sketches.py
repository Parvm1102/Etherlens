"""Tests for sketch libraries — Phase 2."""
import random


def _zipf_stream(n, alpha=1.2, seed=0):
    """Generate n items from a Zipf distribution over 1000 keys."""
    rng = random.Random(seed)
    k = 1000
    weights = [1.0 / (i ** alpha) for i in range(1, k + 1)]
    total = sum(weights)
    weights = [w / total for w in weights]
    keys = [f"key_{i}" for i in range(1, k + 1)]
    return [rng.choices(keys, weights=weights)[0] for _ in range(n)]


def test_cms_no_underestimate():
    """CMS never underestimates true count."""
    from etherlens.sketches.cms import CountMinSketch

    stream = _zipf_stream(50_000)
    true_counts: dict[str, int] = {}
    for item in stream:
        true_counts[item] = true_counts.get(item, 0) + 1

    cms = CountMinSketch(eps=0.001, delta=0.01)
    for item in stream:
        cms.add(item)

    for item, true in true_counts.items():
        est = cms.estimate(item)
        assert est >= true, f"{item}: estimate {est} < true {true}"


def test_cms_error_fraction():
    """Fraction of items with error > eps*N is <= 2*delta."""
    from etherlens.sketches.cms import CountMinSketch

    stream = _zipf_stream(50_000)
    true_counts: dict[str, int] = {}
    for item in stream:
        true_counts[item] = true_counts.get(item, 0) + 1

    eps, delta = 0.001, 0.01
    cms = CountMinSketch(eps=eps, delta=delta)
    for item in stream:
        cms.add(item)

    threshold = eps * cms.N
    over = sum(1 for item, true in true_counts.items() if cms.estimate(item) - true > threshold)
    frac = over / len(true_counts)
    assert frac <= 2 * delta, f"Error fraction {frac:.4f} > 2*delta={2*delta}"


def test_hll_large():
    """200k distinct items → relative error < 3%."""
    from etherlens.sketches.hll import HyperLogLog

    hll = HyperLogLog(p=14)
    n = 200_000
    for i in range(n):
        hll.add(f"addr_{i}")

    est = hll.estimate()
    rel_err = abs(est - n) / n
    assert rel_err < 0.03, f"Relative error {rel_err:.4f} >= 3% for n={n}"


def test_hll_small():
    """100 distinct items → error < 5% (linear counting regime)."""
    from etherlens.sketches.hll import HyperLogLog

    hll = HyperLogLog(p=14)
    n = 100
    for i in range(n):
        hll.add(f"item_{i}")

    est = hll.estimate()
    rel_err = abs(est - n) / n
    assert rel_err < 0.05, f"Relative error {rel_err:.4f} >= 5% for n={n}"


def test_bloom_no_false_negatives():
    """Zero false negatives on 10k inserted items."""
    from etherlens.sketches.bloom import BloomFilter

    bf = BloomFilter(n_expected=10_000, fp_target=0.01)
    items = [f"item_{i}" for i in range(10_000)]
    for x in items:
        bf.add(x)
    for x in items:
        assert x in bf, f"False negative for {x}"


def test_bloom_false_positive_rate():
    """Empirical FP rate on 100k non-members within [0.5×, 2×] of theoretical + 0.002 slack."""
    from etherlens.sketches.bloom import BloomFilter

    n = 10_000
    bf = BloomFilter(n_expected=n, fp_target=0.01)
    for i in range(n):
        bf.add(f"item_{i}")

    non_members = [f"other_{i}" for i in range(100_000)]
    fp_count = sum(1 for x in non_members if x in bf)
    empirical = fp_count / len(non_members)
    theoretical = bf.theoretical_fp(n)

    slack = 0.002
    assert empirical <= theoretical * 2 + slack, (
        f"FP rate {empirical:.4f} > 2× theoretical {theoretical:.4f} + slack"
    )
    assert empirical >= theoretical * 0.5 - slack, (
        f"FP rate {empirical:.4f} < 0.5× theoretical {theoretical:.4f} - slack"
    )


def test_reservoir_uniform():
    """Every item selected with frequency 0.10 ± 0.015 over 20k trials."""
    from etherlens.sketches.reservoir import Reservoir

    s = 10
    n = 100
    trials = 20_000
    counts = [0] * n

    for trial in range(trials):
        res = Reservoir(s=s, seed=trial)
        for item in range(n):
            res.add(item)
        for item in res.sample():
            counts[item] += 1

    for i, count in enumerate(counts):
        freq = count / trials
        assert abs(freq - 0.10) <= 0.015, (
            f"Item {i}: frequency {freq:.4f} not in 0.10 ± 0.015"
        )
