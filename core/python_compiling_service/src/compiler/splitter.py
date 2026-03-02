import ast
import logging
import re

import astor

from .config import MIN_VALID_CUT_LINE

logger = logging.getLogger(__name__)


def apply_loop_transformation_to_process_table(process_table_code: str, table_name: str) -> str:
    """
    Apply loop transformation to a process table method if it contains loop patterns.

    Args:
        process_table_code (str): The process table method code
        table_name (str): Name of the process table (e.g., 'process_table_0')

    Returns:
        str: Transformed process table code or original if no transformation needed
    """
    try:
        # Extract the function body to check for loop patterns
        # Find the method body by locating the first ':' and extracting everything after it
        colon_pos = process_table_code.find(":")
        if colon_pos == -1:
            return process_table_code  # No method signature found

        method_body = process_table_code[colon_pos + 1 :]
        lines = method_body.split("\n")

        # Filter out empty lines and lines that are clearly part of method signature
        body_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped and not stripped.endswith("):") and not stripped.startswith("def "):
                body_lines.append(line)

        if not body_lines:
            return process_table_code  # No meaningful body found

        # Fix indentation for the temporary function
        fixed_lines = []

        # Find the base indentation (minimum non-zero indentation)
        base_indent = None
        for line in body_lines:
            if line.strip():
                current_indent = len(line) - len(line.lstrip())
                if current_indent > 0 and (base_indent is None or current_indent < base_indent):
                    base_indent = current_indent

        if base_indent is None:
            base_indent = 8  # Default if no indentation found

        # Normalize indentation for function body
        for line in body_lines:
            if line.strip():
                current_indent = len(line) - len(line.lstrip())
                relative_indent = max(0, current_indent - base_indent)
                fixed_lines.append("    " + " " * relative_indent + line.lstrip())
            else:
                fixed_lines.append("")

        temp_func_code = "def temp_func():\n" + "\n".join(fixed_lines)

        # Apply loop transformation to the temporary function
        try:
            # Try different import paths since this might be called from different contexts
            try:
                from loop_transformer import LoopTransformer
            except ImportError:
                from src.loop_transformer import LoopTransformer

            transformer = LoopTransformer()
            transformed_temp = transformer.transform_function(temp_func_code)

            # Check if transformation actually occurred (if code changed)
            if transformed_temp.strip() == temp_func_code.strip():
                # No transformation needed, return original
                return process_table_code

            logger.info("Applying loop transformation to %s", table_name)

        except ImportError as e:
            # Fallback if loop_transformer is not available
            logger.warning("LoopTransformer not available (%s), using original code", e)
            return process_table_code

        # Extract the transformed body and reformat back to process table method
        transformed_tree = ast.parse(transformed_temp)
        if transformed_tree.body and isinstance(transformed_tree.body[0], ast.FunctionDef):
            transformed_func = transformed_tree.body[0]

            # Get the original method signature
            method_signature = process_table_code[: process_table_code.find(":") + 1]

            # Reconstruct the transformed method body with proper indentation
            transformed_body_lines = []
            for stmt in transformed_func.body:
                stmt_code = astor.to_source(stmt)
                # Handle multi-line statements properly while preserving relative indentation
                lines = stmt_code.split("\n")

                # Find the base indentation from the first non-empty line
                base_indent = 0
                for line in lines:
                    if line.strip():
                        base_indent = len(line) - len(line.lstrip())
                        break

                # Add each line with proper method body indentation (8 spaces) + relative indent
                for line in lines:
                    if line.strip():  # Skip empty lines
                        current_indent = len(line) - len(line.lstrip())
                        relative_indent = max(0, current_indent - base_indent)
                        transformed_body_lines.append(f"        {' ' * relative_indent}{line.strip()}")

            transformed_process_table = method_signature + "\n" + "\n".join(transformed_body_lines)

            logger.info("Loop transformation applied successfully to %s", table_name)
            return transformed_process_table

    except Exception as e:
        logger.warning("Loop transformation failed for %s: %s. Using original code.", table_name, e)

    return process_table_code


def generate_process_tables_and_split(
    converted_code: str, ranked_cuts: list, original_code: str, type_info: dict = None
) -> dict:
    """
    Generate process tables for each argument, where each process table contains
    a part of the converted code split by N-1 cuts into N parts.
    Wraps all process tables in an Operator class.

    Args:
        converted_code (str): The converted code with self.<var> format
        ranked_cuts (list): List of ranked cut points
        original_code (str): The original code
        type_info (dict): Dictionary mapping variable names to their types

    Returns:
        dict: Contains process tables with code parts wrapped in Operator class
    """

    def analyze_used_arguments(code_lines, all_args):
        """Analyze which function arguments are actually used in the given code lines."""
        used_args = set()

        # Join all lines and look for argument usage
        full_code = " ".join(code_lines)

        for arg in all_args:
            # Look for the argument name in the code (with word boundaries to avoid partial matches)
            # Use word boundaries to ensure we match the exact argument name
            # and not parts of other variable names
            pattern = r"\b" + re.escape(arg) + r"\b"
            if re.search(pattern, full_code):
                used_args.add(arg)

        # Preserve function argument order for deterministic method signatures.
        return [arg for arg in all_args if arg in used_args]

    def get_type_annotation(var_name: str) -> str:
        """Get the type annotation for a variable."""
        if not type_info:
            return ""

        var_type = type_info.get(var_name, "unknown")
        if var_type == "DataFrame":
            return ": pd.DataFrame"
        elif var_type == "Series":
            return ": pd.Series"
        elif var_type == "int":
            return ": int"
        elif var_type == "float":
            return ": float"
        elif var_type == "str":
            return ": str"
        elif var_type == "bool":
            return ": bool"
        elif var_type == "list":
            return ": list"
        elif var_type == "dict":
            return ": dict"
        elif var_type == "tuple":
            return ": tuple"
        elif var_type == "set":
            return ": set"
        else:
            return ""

    def convert_return_to_yield(code_lines):
        """Convert return statements to yield statements and add yield None if no return."""
        processed_lines = []
        has_return = False

        for line in code_lines:
            stripped_line = line.strip()
            if stripped_line.startswith("return"):
                # Convert return to yield
                if stripped_line == "return" or stripped_line == "return None":
                    processed_lines.append(line.replace("return", "yield None"))
                else:
                    # Extract the return value and convert to yield
                    processed_lines.append(line.replace("return", "yield"))
                has_return = True
            else:
                processed_lines.append(line)

        # If no return statement found, add yield None at the end
        if not has_return:
            # Preserve the indentation of the last line or use default indentation
            if processed_lines:
                last_line = processed_lines[-1]
                # Find the indentation of the last line
                indentation = len(last_line) - len(last_line.lstrip())
                processed_lines.append(" " * indentation + "yield None")
            else:
                processed_lines.append("        yield None")

        return processed_lines

    def add_type_annotations_to_lines(code_lines):
        """Add type annotations to local variable assignments."""
        if not type_info:
            return code_lines

        processed_lines = []
        for line in code_lines:
            # Look for assignment patterns like "self.var = ..."
            assignment_pattern = r"^(\s*)(self\.[a-zA-Z_][a-zA-Z0-9_]*)\s*="
            match = re.match(assignment_pattern, line)

            if match:
                indent, var_name = match.groups()
                # Extract the base variable name (remove 'self.')
                base_var_name = var_name.replace("self.", "")
                var_type = type_info.get(base_var_name, "unknown")

                if var_type != "unknown":
                    type_annotation = get_type_annotation(base_var_name)
                    # Add type annotation to the variable
                    annotated_line = f"{indent}{var_name}{type_annotation} ="
                    # Add the rest of the line after the assignment
                    rest_of_line = line[match.end() :]
                    annotated_line += rest_of_line
                    processed_lines.append(annotated_line)
                else:
                    processed_lines.append(line)
            else:
                processed_lines.append(line)

        return processed_lines

    # Parse the function to get input arguments
    tree = ast.parse(converted_code)
    func_def = tree.body[0]
    if not isinstance(func_def, ast.FunctionDef):
        raise ValueError("Input must be a function definition")

    input_args = [arg.arg for arg in func_def.args.args]
    num_args = len(input_args)

    # Extract function body lines from converted code (which already has self. prefixes)
    converted_body_lines = []

    for stmt in func_def.body:
        if isinstance(stmt, ast.Return):
            return_line = astor.to_source(stmt).strip()
            # Include return statement in body lines
            converted_body_lines.append(return_line)
        else:
            # Preserve indentation for multi-line statements
            stmt_code = astor.to_source(stmt)
            # Split into lines and add each line separately to preserve indentation
            lines = stmt_code.split("\n")
            for line in lines:
                if line.strip():  # Skip empty lines
                    converted_body_lines.append(line.rstrip())  # Remove trailing whitespace but keep leading

    # Filter out invalid cuts: only allow cuts where 2 < line_number < len(body_lines) + 1
    valid_cut_min = MIN_VALID_CUT_LINE
    valid_cut_max = len(converted_body_lines)
    filtered_cuts = [cut for cut in ranked_cuts if valid_cut_min <= cut["line_number"] <= valid_cut_max]
    ranked_cuts = filtered_cuts

    # Generate process tables with code parts
    process_tables = {}

    if num_args <= 1:
        # For 0 or 1 arguments, create process tables with the full code
        for i, arg in enumerate(input_args, 0):
            arg_type = get_type_annotation(arg)

            if converted_body_lines:
                # Create temporary process table for loop transformation
                temp_lines = add_type_annotations_to_lines(converted_body_lines)
                temp_process_table_body = "\n".join(f"        {line}" for line in temp_lines)

                # Analyze which arguments are actually used
                used_args = analyze_used_arguments(temp_lines, input_args)

                # Create method signature with type annotations
                if used_args:
                    args_with_types = []
                    for used_arg in used_args:
                        used_arg_type = get_type_annotation(used_arg)
                        args_with_types.append(f"{used_arg}{used_arg_type}")
                    args_str = ", ".join(["self"] + args_with_types)
                else:
                    args_str = "self"

                temp_process_table_code = f"    def process_table_{i}({args_str}):\n{temp_process_table_body}"

                # Apply loop transformation FIRST (before converting returns to yields)
                table_name = f"process_table_{i}"
                transformed_process_table_code = apply_loop_transformation_to_process_table(
                    temp_process_table_code, table_name
                )

                # Then convert any remaining return statements to yield statements
                # Extract body lines from the transformed code and apply return-to-yield conversion
                transformed_lines = transformed_process_table_code.split("\n")
                body_start = next(i for i, line in enumerate(transformed_lines) if ":" in line) + 1
                body_lines = [
                    line[8:] if line.startswith("        ") else line.strip()
                    for line in transformed_lines[body_start:]
                    if line.strip()
                ]

                if body_lines:
                    processed_lines = convert_return_to_yield(body_lines)
                    process_table_body = "\n".join(f"        {line}" for line in processed_lines)
                    transformed_process_table_code = f"    def process_table_{i}({args_str}):\n{process_table_body}"
            else:
                process_table_code = f"    def process_table_{i}(self, {arg}{arg_type}):\n        yield None"
                transformed_process_table_code = process_table_code

            process_tables[f"process_table_{i}"] = transformed_process_table_code
    else:
        # Use the best N-1 valid cuts to split the code into N parts
        best_cuts = filtered_cuts[: num_args - 1]
        sorted_cuts = sorted(best_cuts, key=lambda x: x["line_number"])

        # Split the code into parts
        start_line = 0
        for i in range(num_args):
            if i < len(sorted_cuts):
                # Use the cut point to determine the end of this part
                # To cut before line N, use converted_body_lines[0:N-2] for the first part
                cut_line = sorted_cuts[i]["line_number"] - 2
                part_lines = converted_body_lines[start_line:cut_line]
                start_line = cut_line
            else:
                # This is the last part (from last cut to end)
                part_lines = converted_body_lines[start_line:]

            # Create process table with this part of the code
            arg_name = input_args[i]

            if part_lines:
                # Add type annotations to local variables first
                temp_lines = add_type_annotations_to_lines(part_lines)
                temp_process_table_body = "\n".join(f"        {line}" for line in temp_lines)

                # Analyze which arguments are actually used in this process table's body
                used_args = analyze_used_arguments(temp_lines, input_args)

                # Create method signature with type annotations
                if used_args:
                    args_with_types = []
                    for arg in used_args:
                        arg_type = get_type_annotation(arg)
                        args_with_types.append(f"{arg}{arg_type}")
                    args_str = ", ".join(["self"] + args_with_types)
                else:
                    args_str = "self"

                temp_process_table_code = f"    def process_table_{i}({args_str}):\n{temp_process_table_body}"

                # Apply loop transformation FIRST (before converting returns to yields)
                table_name = f"process_table_{i}"
                transformed_process_table_code = apply_loop_transformation_to_process_table(
                    temp_process_table_code, table_name
                )

                # Then convert any remaining return statements to yield statements
                # Extract body lines from the transformed code and apply return-to-yield conversion
                transformed_lines = transformed_process_table_code.split("\n")
                body_start = next(i for i, line in enumerate(transformed_lines) if ":" in line) + 1
                body_lines = [
                    line[8:] if line.startswith("        ") else line.strip()
                    for line in transformed_lines[body_start:]
                    if line.strip()
                ]

                if body_lines:
                    processed_lines = convert_return_to_yield(body_lines)
                    process_table_body = "\n".join(f"        {line}" for line in processed_lines)
                    transformed_process_table_code = f"    def process_table_{i}({args_str}):\n{process_table_body}"
            else:
                arg_type = get_type_annotation(arg_name)
                temp_process_table_code = (
                    f"    def process_table_{i}(self, {arg_name}{arg_type}):\n        yield None"
                )

                # Apply loop transformation even for empty process tables
                table_name = f"process_table_{i}"
                transformed_process_table_code = apply_loop_transformation_to_process_table(
                    temp_process_table_code, table_name
                )

            process_tables[f"process_table_{i}"] = transformed_process_table_code

    # Wrap all process tables in an Operator class
    operator_class_code = "from pytexera import *\nclass Operator(UDFGeneralOperator):\n"
    for _, table_code in process_tables.items():
        # The process table code already has proper indentation, just add it as is
        operator_class_code += table_code + "\n"

    return {
        "process_tables": process_tables,
        "operator_class": operator_class_code,
        "num_args": num_args,
        "cuts_used": best_cuts if num_args > 1 else [],
        "filtered_cuts": filtered_cuts,
    }
