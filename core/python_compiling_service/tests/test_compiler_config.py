import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.config import (
    DEFAULT_TYPE_SIZE_BYTES,
    FIRST_USAGE_HEURISTIC_BONUS,
    MIN_VALID_CUT_LINE,
)


def test_config_constants():
    assert FIRST_USAGE_HEURISTIC_BONUS < 0
    assert MIN_VALID_CUT_LINE == 3
    assert DEFAULT_TYPE_SIZE_BYTES["DataFrame"] > DEFAULT_TYPE_SIZE_BYTES["unknown"]
