import ast

import astor


def _is_docstring_expr(node: ast.AST) -> bool:
    if not isinstance(node, ast.Expr):
        return False
    value = node.value
    if isinstance(value, ast.Constant):
        return isinstance(value.value, str)
    return isinstance(value, ast.Str)


class _DocstringRemover(ast.NodeTransformer):
    def _strip_first_docstring(self, node):
        self.generic_visit(node)
        if node.body and _is_docstring_expr(node.body[0]):
            node.body = node.body[1:]
        return node

    def visit_FunctionDef(self, node):
        return self._strip_first_docstring(node)

    def visit_AsyncFunctionDef(self, node):
        return self._strip_first_docstring(node)

    def visit_ClassDef(self, node):
        return self._strip_first_docstring(node)

    def visit_Module(self, node):
        return self._strip_first_docstring(node)


class _EmptyBodyFixer(ast.NodeTransformer):
    def _ensure_non_empty_body(self, node):
        self.generic_visit(node)
        if not node.body:
            node.body = [ast.Pass()]
        return node

    def visit_FunctionDef(self, node):
        return self._ensure_non_empty_body(node)

    def visit_AsyncFunctionDef(self, node):
        return self._ensure_non_empty_body(node)

    def visit_ClassDef(self, node):
        return self._ensure_non_empty_body(node)

    def visit_Module(self, node):
        return self._ensure_non_empty_body(node)


def preprocess_code(code_snippet: str) -> str:
    """
    Preprocess code by removing docstrings, comments, and empty lines.
    Ensures that empty function/class/module bodies have a 'pass' statement.
    """
    try:
        tree = ast.parse(code_snippet)
        tree = _DocstringRemover().visit(tree)
        code_wo_docstrings = astor.to_source(tree)
    except Exception:
        code_wo_docstrings = code_snippet

    lines = code_wo_docstrings.split("\n")
    cleaned_lines = []
    for line in lines:
        stripped_line = line.lstrip()
        if not stripped_line:
            continue
        if stripped_line.startswith("#"):
            continue
        if "#" in line:
            comment_pos = line.find("#")
            code_part = line[:comment_pos].rstrip()
            if not code_part.strip():
                continue
            cleaned_lines.append(code_part)
        else:
            cleaned_lines.append(line.rstrip())
    cleaned_code = "\n".join(cleaned_lines)

    try:
        tree = ast.parse(cleaned_code)
        tree = _EmptyBodyFixer().visit(tree)
        return astor.to_source(tree)
    except Exception:
        return cleaned_code
