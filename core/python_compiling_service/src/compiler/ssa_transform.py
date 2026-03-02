import ast
import logging

import astor

logger = logging.getLogger(__name__)


def convert_ssa_to_self(ssa_code, cleaned_function_code, type_info=None):
    # Parse the function to get input arguments (use cleaned code)
    tree = ast.parse(cleaned_function_code)
    func_def = tree.body[0]
    if isinstance(func_def, ast.FunctionDef):
        input_args = [arg.arg for arg in func_def.args.args]
    else:
        input_args = []

    # Parse the SSA code to extract local variables from it
    ssa_tree = ast.parse(ssa_code)
    ssa_func_def = ssa_tree.body[0]

    # Extract local variables from the SSA function body
    local_vars = set()
    for stmt in ssa_func_def.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    local_vars.add(target.id)
                elif isinstance(target, ast.Tuple):
                    for elt in target.elts:
                        if isinstance(elt, ast.Name):
                            local_vars.add(elt.id)
        elif isinstance(stmt, ast.AugAssign):
            if isinstance(stmt.target, ast.Name):
                local_vars.add(stmt.target.id)

    logger.debug("Local variables extracted from SSA code: %s", sorted(local_vars))
    logger.debug("Input arguments: %s", input_args)
    if type_info:
        logger.debug("Type information: %s", type_info)

    # Create AST transformer to add self. prefix to local variables
    class SelfPrefixTransformer(ast.NodeTransformer):
        def __init__(self, local_vars, input_args, type_info=None):
            self.local_vars = local_vars
            self.input_args = input_args
            self.type_info = type_info or {}
            self.transformed_vars = set()  # Track which variables were transformed

        def visit_Name(self, node):
            var_name = node.id

            # Skip if it's already prefixed with self.
            if var_name.startswith("self."):
                return node

            # Skip if it's a function argument
            if var_name in self.input_args:
                return node

            # Skip if it's a built-in name
            if var_name in __builtins__:
                return node

            # Skip if it's a keyword
            keywords = {"True", "False", "None", "self", "yield", "return", "import", "from", "as"}
            if var_name in keywords:
                return node

            # Add self. prefix to local variables in both Load and Store contexts
            if var_name in self.local_vars:
                self.transformed_vars.add(var_name)
                var_type = self.type_info.get(var_name, "unknown")
                logger.debug(
                    "Transforming variable '%s' (type: %s) to 'self.%s' (%s context)",
                    var_name,
                    var_type,
                    var_name,
                    type(node.ctx).__name__,
                )
                return ast.Attribute(value=ast.Name(id="self", ctx=ast.Load()), attr=var_name, ctx=node.ctx)

            logger.debug("Variable '%s' not in local_vars, context: %s", var_name, type(node.ctx).__name__)
            return node

    # Apply the transformer to the SSA code
    try:
        transformer = SelfPrefixTransformer(local_vars, input_args, type_info)
        modified_tree = transformer.visit(ssa_tree)

        logger.debug("Variables actually transformed: %s", sorted(transformer.transformed_vars))

        # Convert back to source code
        converted_code = astor.to_source(modified_tree)
        return converted_code

    except Exception as e:
        # If AST transformation fails, fall back to the original SSA code
        logger.warning("AST transformation failed: %s. Using original SSA code.", e)
        return ssa_code
