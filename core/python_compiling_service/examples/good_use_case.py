#!/usr/bin/env python3
"""Recommended end-to-end example for compiling a two-input pandas UDF."""

import os
import sys


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(os.path.dirname(CURRENT_DIR), "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from compiler import RECOMMENDED_AUTO_CUT_UDF, compile_udf


def main() -> int:
    result = compile_udf(RECOMMENDED_AUTO_CUT_UDF)
    ranked_lines = [cut["line_number"] for cut in result.ranked_cuts]
    selected_lines = [cut["line_number"] for cut in result.cuts_used]

    print("=== Recommended UDF compile use case ===")
    print(f"num_args: {result.num_args}")
    print(f"baseline_mode: {result.baseline_mode}")
    print(f"ranked_cuts: {ranked_lines}")
    print(f"cuts_used: {selected_lines}")
    print(f"process_tables: {sorted(result.process_tables.keys())}")
    print("\nGenerated operator class:\n")
    print(result.operator_class)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
