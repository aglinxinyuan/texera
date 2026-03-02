import re
from typing import Optional

def infer_line_number_from_code(code: str) -> Optional[int]:
    lines = code.split("\n")
    if not lines:
        return None

    first_line = lines[0].strip()
    if not first_line.startswith("#") or len(first_line) <= 1:
        return None

    comment_content = first_line[1:].strip()
    if comment_content.isdigit():
        return int(comment_content)
    return None


def get_base_variable_name(var_name: str) -> str:
    match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)(\d*)$", var_name)
    if match:
        return match.group(1)
    return var_name
