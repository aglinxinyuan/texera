import hashlib
import json
import os
import re
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler import compile_udf_legacy as compile

INPUT_DIR = Path(__file__).parent / "inputs"
GOLDEN_DIR = Path(__file__).parent / "golden"

CASES = [
    ("baseline_two_inputs", INPUT_DIR / "baseline_two_inputs.py", None),
    ("auto_two_inputs", INPUT_DIR / "auto_two_inputs.py", None),
    ("auto_three_inputs", INPUT_DIR / "auto_three_inputs.py", None),
]


def _snapshot(result: dict) -> dict:
    operator_class = _normalize_operator_class(result["operator_class"])
    return {
        "num_args": result.get("num_args"),
        "import_statements": result.get("import_statements"),
        "ranked_cut_lines": [c["line_number"] for c in result.get("ranked_cuts", [])],
        "cuts_used_lines": [c["line_number"] for c in result.get("cuts_used", [])],
        "filtered_cut_lines": [c["line_number"] for c in result.get("filtered_cuts", [])],
        "operator_class_sha256": hashlib.sha256(operator_class.encode("utf-8")).hexdigest(),
        "operator_class": operator_class,
    }


def _normalize_operator_class(operator_class: str) -> str:
    normalized_lines = []
    pattern = re.compile(r"^(\s*def process_table_\d+\()(.+?)(\):\s*)$")
    for line in operator_class.splitlines():
        match = pattern.match(line)
        if not match:
            normalized_lines.append(line)
            continue

        args = [a.strip() for a in match.group(2).split(",") if a.strip()]
        if args and args[0] == "self":
            args = ["self"] + sorted(args[1:])
        else:
            args = sorted(args)

        normalized_lines.append(f"{match.group(1)}{', '.join(args)}{match.group(3)}")

    normalized = "\n".join(normalized_lines)
    if operator_class.endswith("\n"):
        normalized += "\n"
    return normalized


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_compile_golden_snapshots():
    update = os.getenv("UPDATE_GOLDEN") == "1"

    for case_name, code_path, line_number in CASES:
        code = code_path.read_text(encoding="utf-8")
        result = compile(code, line_number=line_number)
        actual = _snapshot(result)

        golden_path = GOLDEN_DIR / f"{case_name}.json"
        if update or not golden_path.exists():
            _write_json(golden_path, actual)
            continue

        expected = json.loads(golden_path.read_text(encoding="utf-8"))
        assert actual == expected, f"Golden mismatch for case: {case_name}"
