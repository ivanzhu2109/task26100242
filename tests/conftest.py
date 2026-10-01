import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False,
                     help="run slow performance tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        return
    import pytest
    skip = pytest.mark.skip(reason="need --runslow to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip)
