import ast

import astor


def SSA(code_snippet: str) -> str:
    """
    Convert a function definition to Static Single Assignment (SSA) format.

    Args:
        code_snippet (str): A string containing a function definition (already cleaned)

    Returns:
        str: The function converted to SSA format
    """
    try:
        # Parse the code into an AST (code is already cleaned)
        tree = ast.parse(code_snippet)

        # Transform the AST to SSA format
        ssa_transformer = SSATransformer()
        ssa_tree = ssa_transformer.visit(tree)

        # Flatten lists in function body (for tuple assignments)
        for node in ast.walk(ssa_tree):
            if isinstance(node, ast.FunctionDef):
                new_body = []
                for stmt in node.body:
                    if isinstance(stmt, list):
                        new_body.extend(stmt)
                    else:
                        new_body.append(stmt)
                node.body = new_body

        # Convert back to source code using astor
        return astor.to_source(ssa_tree)

    except Exception as e:
        raise ValueError(f"Failed to convert code to SSA format: {e}")


class SSATransformer(ast.NodeTransformer):
    """AST transformer to convert code to SSA format."""

    def __init__(self):
        self.variable_counter = {}
        self.scope_stack = []

    def visit_FunctionDef(self, node):
        """Visit function definition and process its body."""
        # Initialize variable counter for this function
        self.variable_counter = {}
        self.scope_stack.append(set())

        # Process function arguments (they start with version 0)
        for arg in node.args.args:
            self.variable_counter[arg.arg] = 0
            self.scope_stack[-1].add(arg.arg)

        # Process the function body
        node.body = [self.visit(stmt) for stmt in node.body]

        self.scope_stack.pop()
        return node

    def visit_Assign(self, node):
        """Visit assignment statements and rename variables."""
        # Handle tuple assignments
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Tuple):
            return self._handle_tuple_assignment(node)

        # Handle normal assignments
        node.value = self.visit(node.value)
        new_targets = []
        for target in node.targets:
            if isinstance(target, ast.Name):
                var_name = target.id
                if var_name not in self.variable_counter:
                    self.variable_counter[var_name] = 0
                else:
                    self.variable_counter[var_name] += 1
                new_target = ast.Name(
                    id=f"{var_name}{self.variable_counter[var_name] if self.variable_counter[var_name] > 0 else ''}",
                    ctx=target.ctx,
                )
                new_targets.append(new_target)
                if self.scope_stack:
                    self.scope_stack[-1].add(var_name)
            else:
                new_targets.append(self.visit(target))
        node.targets = new_targets
        return node

    def visit_AugAssign(self, node):
        """Visit augmented assignment statements (+=, -=, &=, etc.) and convert to SSA."""
        # Convert augmented assignment to regular assignment
        # e.g., x += 1 becomes x = x + 1

        # First, visit the value to get the latest versions
        node.value = self.visit(node.value)

        if isinstance(node.target, ast.Name):
            var_name = node.target.id

            # Get the latest version of the variable for the right side
            latest_version = self.variable_counter.get(var_name, 0)
            if latest_version > 0:
                current_var = ast.Name(id=f"{var_name}{latest_version}", ctx=ast.Load())
            else:
                current_var = ast.Name(id=var_name, ctx=ast.Load())

            # Create the binary operation
            bin_op = ast.BinOp(left=current_var, op=node.op, right=node.value)

            # Increment the variable counter
            if var_name not in self.variable_counter:
                self.variable_counter[var_name] = 0
            else:
                self.variable_counter[var_name] += 1

            # Create the new assignment
            new_target = ast.Name(
                id=f"{var_name}{self.variable_counter[var_name] if self.variable_counter[var_name] > 0 else ''}",
                ctx=ast.Store(),
            )

            if self.scope_stack:
                self.scope_stack[-1].add(var_name)

            return ast.Assign(targets=[new_target], value=bin_op)

        return node

    def _handle_tuple_assignment(self, node):
        """Handle tuple assignments like 'a, b = b, a'."""
        targets = node.targets[0].elts
        values = node.value.elts if isinstance(node.value, ast.Tuple) else [node.value]

        # Create temporary variables for all right-hand side values
        temp_assignments = []
        temp_vars = []

        for i, value in enumerate(values):
            temp_name = f"_tmp_{i}"
            temp_vars.append(temp_name)
            temp_assign = ast.Assign(
                targets=[ast.Name(id=temp_name, ctx=ast.Store())],
                value=self.visit(value),
            )
            temp_assignments.append(temp_assign)

        # Create assignments from temporaries to targets
        target_assignments = []
        for i, target in enumerate(targets):
            if isinstance(target, ast.Name):
                var_name = target.id
                if var_name not in self.variable_counter:
                    self.variable_counter[var_name] = 0
                else:
                    self.variable_counter[var_name] += 1
                new_target = ast.Name(
                    id=f"{var_name}{self.variable_counter[var_name] if self.variable_counter[var_name] > 0 else ''}",
                    ctx=ast.Store(),
                )
                if self.scope_stack:
                    self.scope_stack[-1].add(var_name)

                # Assign from corresponding temporary
                if i < len(temp_vars):
                    target_assign = ast.Assign(
                        targets=[new_target],
                        value=ast.Name(id=temp_vars[i], ctx=ast.Load()),
                    )
                    target_assignments.append(target_assign)

        # Return all assignments as a list
        return temp_assignments + target_assignments

    def visit_Name(self, node):
        """Visit name nodes and update references to use the latest version."""
        if isinstance(node.ctx, ast.Load):  # Only rename when reading the variable
            var_name = node.id

            # Check if this variable has been assigned in the current scope
            if self.scope_stack and var_name in self.scope_stack[-1]:
                # Use the latest version of the variable
                latest_version = self.variable_counter.get(var_name, 0)
                if latest_version > 0:
                    return ast.Name(id=f"{var_name}{latest_version}", ctx=node.ctx)
                else:
                    return ast.Name(id=var_name, ctx=node.ctx)

        return node

    def visit_Return(self, node):
        """Visit return statements and update variable references."""
        if node.value:
            node.value = self.visit(node.value)
        return node

    def visit_BinOp(self, node):
        """Visit binary operations and update variable references."""
        node.left = self.visit(node.left)
        node.right = self.visit(node.right)
        return node

    def visit_UnaryOp(self, node):
        """Visit unary operations and update variable references."""
        node.operand = self.visit(node.operand)
        return node

    def visit_Call(self, node):
        """Visit function calls and update variable references."""
        node.func = self.visit(node.func)
        node.args = [self.visit(arg) for arg in node.args]
        node.keywords = [self.visit(keyword) for keyword in node.keywords]
        return node

    def visit_keyword(self, node):
        """Visit keyword arguments and update variable references."""
        node.value = self.visit(node.value)
        return node
