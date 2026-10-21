"""Tests for Merkle tree — Phase 2.6."""
import pytest

from etherlens.chain.merkle import build_tree, verify_row, to_json


GOLDEN_ROWS = ["a", "b", "c"]
GOLDEN_ROOT = "0x5842148bc6ebeb52af882a317c765fccd3ae80589b21a9b8cbf21abb630e46a7"


def test_golden_vector():
    """["a","b","c"] must produce the exact root validated against the Solidity contract."""
    tree = build_tree(GOLDEN_ROWS)
    assert tree.root == GOLDEN_ROOT, f"Got {tree.root}"


@pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 7, 8, 33, 100])
def test_all_proofs_verify(n):
    """Every proof for every leaf verifies against the root."""
    rows = [f"row_{i}" for i in range(n)]
    tree = build_tree(rows)
    root = tree.root
    for i, row in enumerate(rows):
        proof = tree.proof(i)
        assert verify_row(row, proof, root), f"Proof failed for leaf {i} of {n}"


@pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 7, 8, 33, 100])
def test_tampered_row_fails(n):
    """Any single-character change in a row must cause verification to fail."""
    rows = [f"row_{i}" for i in range(n)]
    tree = build_tree(rows)
    root = tree.root
    for i in range(min(n, 5)):  # check first 5 leaves to keep test fast
        proof = tree.proof(i)
        tampered = rows[i] + "X"
        assert not verify_row(tampered, proof, root), (
            f"Tampered row {i} of {n} should not verify"
        )


def test_root_deterministic():
    """Same rows always produce the same root."""
    rows = ["alpha", "beta", "gamma", "delta"]
    assert build_tree(rows).root == build_tree(rows).root


def test_single_leaf():
    """One leaf: root equals the leaf hash."""
    tree = build_tree(["only"])
    assert tree.root == tree.leaves[0]
    assert verify_row("only", tree.proof(0), tree.root)


def test_empty_raises():
    """Empty list raises ValueError."""
    with pytest.raises(ValueError):
        build_tree([])


def test_to_json_structure():
    """to_json returns the expected dict shape."""
    rows = ["x", "y", "z"]
    data = to_json("2025-01-01", rows)
    assert data["dt"] == "2025-01-01"
    assert data["leaf_count"] == 3
    assert data["root"] == build_tree(rows).root
    assert len(data["proofs"]) == 3
    assert len(data["rows"]) == 3
    # All proofs must verify
    for i, row in enumerate(rows):
        assert verify_row(row, data["proofs"][i], data["root"])
