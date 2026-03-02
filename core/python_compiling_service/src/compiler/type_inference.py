import ast
import logging

import astor

logger = logging.getLogger(__name__)


def infer_types_from_code(code: str) -> dict:
    """
    Infer argument and variable types from the function code.

    Args:
        code (str): Function code string

    Returns:
        dict: Dictionary mapping variable names to their inferred types
    """
    try:
        tree = ast.parse(code)

        func_def = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_def = node
                break

        if not func_def:
            return {}

        type_info = {}

        for arg in func_def.args.args:
            if arg.annotation:
                type_str = astor.to_source(arg.annotation).strip()
                inferred_type = _parse_type_annotation(type_str)
                type_info[arg.arg] = inferred_type

        for stmt in func_def.body:
            if isinstance(stmt, ast.Assign):
                rhs_type = _infer_type_from_expression(stmt.value, type_info)

                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        type_info[target.id] = rhs_type
                    elif isinstance(target, ast.Tuple):
                        if isinstance(stmt.value, ast.Tuple):
                            for i, elt in enumerate(target.elts):
                                if isinstance(elt, ast.Name) and i < len(stmt.value.elts):
                                    elt_type = _infer_type_from_expression(stmt.value.elts[i], type_info)
                                    type_info[elt.id] = elt_type

        return type_info

    except Exception as e:
        logger.warning("Type inference failed: %s", e)
        return {}


def _parse_type_annotation(type_str: str) -> str:
    type_str = type_str.lower()

    if "dataframe" in type_str or "pd.dataframe" in type_str:
        return "DataFrame"
    elif "series" in type_str or "pd.series" in type_str:
        return "Series"
    elif "int" in type_str:
        return "int"
    elif "float" in type_str:
        return "float"
    elif "str" in type_str or "string" in type_str:
        return "str"
    elif "bool" in type_str:
        return "bool"
    elif "list" in type_str:
        return "list"
    elif "dict" in type_str:
        return "dict"
    elif "tuple" in type_str:
        return "tuple"
    elif "set" in type_str:
        return "set"

    return "unknown"


def _infer_type_from_expression(expr, type_info: dict) -> str:
    if isinstance(expr, ast.Call):
        if isinstance(expr.func, ast.Name):
            func_name = expr.func.id
            if func_name in ["pd.DataFrame", "DataFrame"]:
                return "DataFrame"
            elif func_name in ["pd.Series", "Series"]:
                return "Series"
        elif isinstance(expr.func, ast.Attribute):
            if isinstance(expr.func.value, ast.Name):
                var_name = expr.func.value.id
                if var_name in type_info and type_info[var_name] == "DataFrame":
                    return "Series"

    elif isinstance(expr, ast.Subscript):
        if isinstance(expr.value, ast.Name):
            var_name = expr.value.id
            if var_name in type_info and type_info[var_name] == "DataFrame":
                return "Series"

    elif isinstance(expr, ast.Compare):
        return "bool"

    elif isinstance(expr, ast.BinOp):
        left_type = _infer_type_from_expression(expr.left, type_info)
        right_type = _infer_type_from_expression(expr.right, type_info)

        if left_type in ["int", "float", "numeric"] and right_type in ["int", "float", "numeric"]:
            if left_type == "float" or right_type == "float":
                return "float"
            return "int"

        if left_type == "str" and right_type == "str":
            return "str"

        return "unknown"

    elif isinstance(expr, ast.Constant):
        if isinstance(expr.value, str):
            return "str"
        elif isinstance(expr.value, (int, float)):
            return "numeric"
        elif isinstance(expr.value, bool):
            return "bool"

    elif isinstance(expr, ast.Name):
        return type_info.get(expr.id, "unknown")

    elif isinstance(expr, ast.Attribute):
        if isinstance(expr.value, ast.Name):
            var_name = expr.value.id
            if var_name in type_info:
                base_type = type_info[var_name]
                if base_type == "DataFrame":
                    if expr.attr in ["columns", "index", "shape"]:
                        return "list"
                    elif expr.attr in ["dtypes", "info"]:
                        return "dict"

    return "unknown"
