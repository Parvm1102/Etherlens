"""Pytest configuration and fixtures."""
import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: slow integration tests")
    config.addinivalue_line("markers", "chain: tests requiring a running anvil chain")
