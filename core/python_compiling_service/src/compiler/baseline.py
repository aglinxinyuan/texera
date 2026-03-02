import ast

import astor

from .pipeline import split_imports_and_function_code


def compile_baseline_mode(code: str) -> dict:
    # Remove the "#baseline" line
    lines = code.split("\n")[1:]
    import_statements, function_code_str = split_imports_and_function_code(lines)

    try:
        tree = ast.parse(function_code_str)
        func_def = tree.body[0]
        if not isinstance(func_def, ast.FunctionDef):
            raise ValueError("Input must be a function definition")

        args_with_types = []
        for arg in func_def.args.args:
            if arg.annotation:
                type_annotation = astor.to_source(arg.annotation).strip()
                args_with_types.append(f"{arg.arg}: {type_annotation}")
            else:
                args_with_types.append(arg.arg)

        body_lines = []
        for stmt in func_def.body:
            if isinstance(stmt, ast.Return):
                return_code = astor.to_source(stmt).strip()
                if return_code == "return" or return_code == "return None":
                    body_lines.append("        yield None")
                else:
                    yield_code = return_code.replace("return", "yield", 1)
                    body_lines.append(f"        {yield_code}")
            else:
                stmt_code = astor.to_source(stmt)
                stmt_lines = stmt_code.split("\n")
                for stmt_line in stmt_lines:
                    if stmt_line.strip():
                        body_lines.append(f"        {stmt_line.strip()}")

        if not any("yield" in line for line in body_lines):
            body_lines.append("        yield None")

        args_str = ", ".join(["self"] + args_with_types)
        method_body = "\n".join(body_lines)

        return_type = ""
        if func_def.returns:
            return_type = f" -> {astor.to_source(func_def.returns).strip()}"

        process_tables_method = f"    def process_tables({args_str}){return_type}:\n{method_body}"

        operator_class = "from pytexera import *\n"
        if import_statements:
            clean_imports = [imp.lstrip() for imp in import_statements]
            operator_class += "\n".join(clean_imports) + "\n"

        operator_class += "\nclass Operator(UDFGeneralOperator):\n"
        operator_class += process_tables_method + "\n"

        return {
            "ranked_cuts": [],
            "ssa_code": function_code_str,
            "converted_code": function_code_str,
            "process_tables": {"process_tables": process_tables_method},
            "operator_class": operator_class,
            "num_args": len(func_def.args.args),
            "cuts_used": [],
            "filtered_cuts": [],
            "import_statements": import_statements,
            "cleaned_code": function_code_str,
            "port_assignments": [],
            "baseline_mode": True,
        }
    except Exception as e:
        raise ValueError(f"Failed to compile in baseline mode: {e}")
