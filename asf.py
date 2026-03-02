#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---- Helpers to format/validate the branch protection payload ----


def format_required_status_checks(obj: Any) -> Optional[Dict[str, Any]]:
    """
    Accepts the value under github.protected_branches.<branch>.required_status_checks
    and normalizes it to the GitHub API shape expected by Infra (strict + checks[]).
    Mirrors the intent of Infra's formatProtectedBranchRequiredStatusChecks().
    """
    if isinstance(obj, dict):
        contexts = obj.get("contexts", [])
        checks = obj.get("checks", [])

        if contexts:
            if not isinstance(contexts, list) or not all(
                isinstance(x, str) for x in contexts
            ):
                raise AssertionError("`contexts` must be a list of strings")
            # Convert old-style string contexts -> list of dicts with {context, app_id}
            checks = [{"context": ctx, "app_id": -1} for ctx in contexts]
        else:
            if checks:
                if not isinstance(checks, list):
                    raise AssertionError("`checks` must be a list")
                for chk in checks:
                    if not isinstance(chk, dict):
                        raise AssertionError("`checks` must be a list of objects")
                    if not isinstance(chk.get("context"), str):
                        raise AssertionError("`checks[*].context` must be a string")
                    if "app_id" in chk and not isinstance(chk["app_id"], int):
                        raise AssertionError(
                            "`checks[*].app_id` must be an int if present"
                        )

        return {"strict": bool(obj.get("strict", False)), "checks": checks or []}

    # Explicit None is allowed (means “no required checks”)
    if obj is None:
        return None

    # Anything else is invalid → treat as None
    return None


def extract_required_contexts(parsed: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Returns a dict: {branch: [required context strings ...]} for quick inspection.
    Works with both old 'contexts' list and new 'checks' list.
    """
    out: Dict[str, List[str]] = {}
    gh = (parsed or {}).get("github") or {}
    pb = gh.get("protected_branches") or {}
    if not isinstance(pb, dict):
        return out

    for branch, settings in pb.items():
        if not isinstance(settings, dict):
            continue
        rsc = settings.get("required_status_checks", {})
        if rsc is None:
            out[branch] = []
            continue
        contexts = []
        if isinstance(rsc, dict):
            if isinstance(rsc.get("contexts"), list):
                # old style: list of strings
                contexts = [str(x) for x in rsc["contexts"]]
            elif isinstance(rsc.get("checks"), list):
                # new style: list of dicts
                for chk in rsc["checks"]:
                    if isinstance(chk, dict) and isinstance(chk.get("context"), str):
                        contexts.append(chk["context"])
        out[branch] = contexts
    return out


# ---- YAML parsing with good error messages ----


def load_yaml(path: Path) -> Dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        sys.exit(f"ERROR: file not found: {path}")

    try:
        data = yaml.safe_load(text) or {}
        if not isinstance(data, dict):
            raise yaml.YAMLError("Top-level YAML must be a mapping (dictionary).")
        return data
    except yaml.YAMLError as exc:
        # Pinpoint line/column and show context
        if hasattr(exc, "problem_mark") and exc.problem_mark:
            mark = exc.problem_mark
            line_no = mark.line + 1
            col_no = mark.column + 1
            print(
                f"YAML parse error at line {line_no}, column {col_no}:", file=sys.stderr
            )
        print(exc, file=sys.stderr)

        # Print a few lines around the error for fast debugging
        lines = text.splitlines()
        idx = (
            getattr(exc, "problem_mark", None).line
            if hasattr(exc, "problem_mark") and exc.problem_mark
            else 0
        )
        start = max(0, idx - 3)
        end = min(len(lines), idx + 4)
        width = len(str(end))
        for i in range(start, end):
            pointer = ">>" if i == idx else "  "
            print(f"{pointer} {str(i+1).rjust(width)} | {lines[i]}", file=sys.stderr)
        sys.exit(2)


def main(argv: List[str]) -> None:
    path = Path(argv[1] if len(argv) > 1 else ".asf.yaml")
    data = load_yaml(path)
    print(f"Loaded {path} OK.")

    # Optional: normalize each branch's required_status_checks and report
    gh = data.get("github") or {}
    pb = gh.get("protected_branches") or {}
    if isinstance(pb, dict):
        print("\n[github.protected_branches] required_status_checks summary:")
        for branch, settings in pb.items():
            if not isinstance(settings, dict):
                continue
            rsc_raw = settings.get("required_status_checks")
            rsc = format_required_status_checks(rsc_raw)
            strict = (rsc or {}).get("strict", False) if rsc is not None else False
            checks = (rsc or {}).get("checks", []) if rsc is not None else []
            contexts = [c["context"] for c in checks] if checks else []
            print(f"  - {branch}: strict={strict}, contexts={contexts}")

    # Quick convenience output (old/new styles unified)
    ctxs = extract_required_contexts(data)
    if ctxs:
        print("\n[contexts by branch]")
        for br, lst in ctxs.items():
            print(f"  {br}:")
            for c in lst:
                print(f"    - {c}")


if __name__ == "__main__":
    main(sys.argv)
