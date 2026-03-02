#!/usr/bin/env python3
"""Manual demo/debug entrypoint for the UDF compiler."""

import argparse
import os
import sys
import traceback


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(os.path.dirname(CURRENT_DIR), "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from compiler import SSA, VariableDependencyGraph, compile_udf_legacy as compile_udf


AUTO_CODE = """
from pytexera import *
import pandas as pd
import numpy as np

def compare_texts_vectorized(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    s1_full = df1['text'].fillna("").str.strip().str.lower()
    n = min(len(s1_full), len(df2))
    s1 = s1_full.head(n)
    s2 = df2['text'].head(n).fillna("").str.strip().str.lower()
    exact_mask = s1.eq(s2)
    partial_mask = s1.str.contains(s2, regex=False) | s2.str.contains(s1, regex=False)
    match_type = np.select([exact_mask, partial_mask], ['exact', 'partial'], default='none')
    return pd.DataFrame({'df1_text': s1, 'df2_text': s2, 'match_type': match_type})
"""


BASELINE_CODE = """#baseline
import pandas as pd
import numpy as np

def compare_texts_vectorized(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    s1_full = df1['text'].fillna("").str.strip().str.lower()
    n = min(len(s1_full), len(df2))
    s1 = s1_full.head(n)
    s2 = df2['text'].head(n).fillna("").str.strip().str.lower()
    exact_mask = s1.eq(s2)
    partial_mask = s1.str.contains(s2, regex=False) | s2.str.contains(s1, regex=False)
    match_type = np.select([exact_mask, partial_mask], ['exact', 'partial'], default='none')
    return pd.DataFrame({'df1_text': s1, 'df2_text': s2, 'match_type': match_type})
"""


def run_auto_demo(code: str) -> None:
    print("Original code:")
    print(code)
    print("\nSSA format:")
    ssa_code = SSA(code)
    print(ssa_code)

    print("\n" + "=" * 50)
    print("Variable Dependency Graph Analysis:")
    print("=" * 50)
    graph = VariableDependencyGraph(ssa_code)
    print(graph.visualize())

    print("\n" + "=" * 50)
    print("Compile Result (auto cut):")
    print("=" * 50)
    result = compile_udf(code)
    print(f"Number of arguments: {result['num_args']}")
    print(f"Available ranked cuts: {len(result['ranked_cuts'])}")
    print(result["operator_class"])


def run_baseline_demo(code: str) -> None:
    print("\n" + "=" * 50)
    print("Baseline Compilation:")
    print("=" * 50)
    result = compile_udf(code)
    print(f"Baseline mode: {result.get('baseline_mode', False)}")
    print(f"Number of arguments: {result['num_args']}")
    print(result["operator_class"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Run manual UDF compiler demos.")
    parser.add_argument(
        "--mode",
        choices=["auto", "baseline", "both"],
        default="both",
        help="Which demo to run",
    )
    args = parser.parse_args()

    try:
        if args.mode in ("auto", "both"):
            run_auto_demo(AUTO_CODE)
        if args.mode in ("baseline", "both"):
            run_baseline_demo(BASELINE_CODE)
        return 0
    except Exception as exc:
        print(f"Demo failed: {exc}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
