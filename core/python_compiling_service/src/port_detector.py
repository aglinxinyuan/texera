import ast
from collections import defaultdict

def detect_ports_and_classify_statements(source_code: str):
    """Infer statement-to-port ownership from a simple AST walk."""
    tree = ast.parse(source_code)
    fn_def = tree.body[0]

    # Step 1: Identify input arguments -> ports.
    arg_ports = [arg.arg for arg in fn_def.args.args]
    port_origin = {arg: f"{arg}_port" for arg in arg_ports}
    var_to_port = {}

    # Step 2: Direct unpacking from input (e.g., X_train, y_train = train_set).
    for stmt in fn_def.body:
        if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Name):
            source = stmt.value.id
            if source in port_origin:
                targets = stmt.targets[0]
                if isinstance(targets, ast.Tuple):
                    for elt in targets.elts:
                        if isinstance(elt, ast.Name):
                            var_to_port[elt.id] = port_origin[source]

    # Step 3: Propagate port origin to derived variables.
    def propagate_variable_origins(fn_body):
        for stmt in fn_body:
            lhs_vars = set()
            rhs_ports = set()
            for node in ast.walk(stmt):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            lhs_vars.add(target.id)
                elif isinstance(node, ast.Name):
                    if node.id in var_to_port:
                        rhs_ports.add(var_to_port[node.id])
            for var in lhs_vars:
                if len(rhs_ports) == 1:
                    var_to_port[var] = list(rhs_ports)[0]

    propagate_variable_origins(fn_def.body)

    # Step 4: Classify each statement.
    stmt_to_port = []
    for stmt in fn_def.body:
        stmt_vars = {node.id for node in ast.walk(stmt) if isinstance(node, ast.Name)}
        used_ports = {var_to_port.get(var, None) for var in stmt_vars}
        used_ports.discard(None)

        if len(used_ports) == 1:
            port = list(used_ports)[0]
        elif len(used_ports) > 1:
            port = "shared"
        else:
            port = "global"

        stmt_to_port.append((ast.unparse(stmt).strip(), port))

    return port_origin, stmt_to_port


def build_dependency_graph(function_node):
    import astroid

    dependency_graph = defaultdict(set)
    for stmt in function_node.body:
        if isinstance(stmt, astroid.Assign):
            rhs_vars = {v.name for v in stmt.value.nodes_of_class(astroid.Name)}
            for target in stmt.targets:
                if isinstance(target, astroid.AssignName):
                    dependency_graph[target.name].update(rhs_vars)
        elif isinstance(stmt, (astroid.For, astroid.Expr, astroid.Return)):
            for node in stmt.nodes_of_class(astroid.AssignName):
                rhs_vars = {v.name for v in stmt.nodes_of_class(astroid.Name)}
                dependency_graph[node.name].update(rhs_vars)
    return dependency_graph


def backward_propagate_ports(var_port_map, dependency_graph):
    updated = True
    while updated:
        updated = False
        for var, deps in dependency_graph.items():
            dep_ports = {var_port_map.get(dep) for dep in deps if dep in var_port_map}
            dep_ports.discard(None)
            if len(dep_ports) == 1 and (
                var not in var_port_map or var_port_map[var] != next(iter(dep_ports))
            ):
                var_port_map[var] = next(iter(dep_ports))
                updated = True


def label_statements_by_port(code: str):
    import astroid
    import pandas as pd

    module = astroid.parse(code)
    function_node = next((n for n in module.body if isinstance(n, astroid.FunctionDef)), None)
    if function_node is None:
        raise ValueError("No function definition found.")

    # Step 1: Arguments as ports.
    arg_names = [arg.name for arg in function_node.args.args]
    arg_port_map = {arg: f"{arg}_port" for arg in arg_names}
    var_port_map = dict(arg_port_map)  # variable -> port or "shared"

    statement_ports = []
    for stmt in function_node.body:
        assigned_vars = {node.name for node in stmt.nodes_of_class(astroid.AssignName)}
        loaded_vars = {node.name for node in stmt.nodes_of_class(astroid.Name)}
        loaded_vars -= assigned_vars

        loaded_arg_ports = {arg_port_map.get(var) for var in loaded_vars if var in arg_port_map}
        loaded_arg_ports.discard(None)
        loaded_local_ports = {
            var_port_map.get(var)
            for var in loaded_vars
            if var in var_port_map and var not in arg_port_map
        }
        loaded_local_ports.discard(None)

        # Coloring rule: prioritize argument-origin ports.
        for var in assigned_vars:
            if loaded_arg_ports:
                if len(loaded_arg_ports) == 1:
                    var_port_map[var] = next(iter(loaded_arg_ports))
                else:
                    var_port_map[var] = "shared"
            elif loaded_local_ports:
                if len(loaded_local_ports) == 1:
                    var_port_map[var] = next(iter(loaded_local_ports))
                else:
                    var_port_map[var] = "shared"

        if assigned_vars:
            stmt_ports = {var_port_map.get(var) for var in assigned_vars if var in var_port_map}
            stmt_ports.discard(None)
            if len(stmt_ports) == 1:
                label = next(iter(stmt_ports))
            elif len(stmt_ports) > 1:
                label = "shared"
            else:
                label = "global"
        else:
            all_ports = loaded_arg_ports | loaded_local_ports
            if len(all_ports) == 1:
                label = next(iter(all_ports))
            elif len(all_ports) > 1:
                label = "shared"
            else:
                label = "global"

        statement_ports.append((stmt.lineno, stmt.as_string(), label))

    df = pd.DataFrame(statement_ports, columns=["Line", "Statement", "Port Assignment"])

    # Backward propagation for global assignments.
    global_vars = set()
    stmt_lineno_to_var = {}
    for i, (lineno, _stmt_str, label) in enumerate(statement_ports):
        if label == "global":
            stmt = function_node.body[i]
            assigned_vars = {node.name for node in stmt.nodes_of_class(astroid.AssignName)}
            for var in assigned_vars:
                global_vars.add(var)
                stmt_lineno_to_var[lineno] = var

    var_used_by_ports = {var: set() for var in global_vars}
    for i, stmt in enumerate(function_node.body):
        loaded_vars = {node.name for node in stmt.nodes_of_class(astroid.Name)}
        for var in global_vars:
            if var in loaded_vars:
                port_label = statement_ports[i][2]
                if port_label not in ("global", "shared"):
                    var_used_by_ports[var].add(port_label)

    for var, ports in var_used_by_ports.items():
        if len(ports) == 1:
            port = next(iter(ports))
            for i, (lineno, stmt_str, label) in enumerate(statement_ports):
                if stmt_lineno_to_var.get(lineno) == var and label == "global":
                    statement_ports[i] = (lineno, stmt_str, port)
            var_port_map[var] = port
        elif len(ports) > 1:
            for i, (lineno, stmt_str, label) in enumerate(statement_ports):
                if stmt_lineno_to_var.get(lineno) == var and label == "global":
                    statement_ports[i] = (lineno, stmt_str, "shared")
            var_port_map[var] = "shared"

    return pd.DataFrame(statement_ports, columns=["Line", "Statement", "Port Assignment"])


def _extract_assigned_vars(stmt):
    assigned_vars = set()
    for node in ast.walk(stmt):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            assigned_vars.add(node.id)
    return assigned_vars


def _extract_loaded_vars(stmt):
    loaded_vars = set()
    for node in ast.walk(stmt):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            loaded_vars.add(node.id)
    return loaded_vars


def label_statements_by_port_ast(code: str):
    """
    Lightweight port-labeling variant that only depends on Python's built-in ast module.

    Returns:
        List[Dict]: [{"line_number": int, "statement": str, "port": str}, ...]
    """
    tree = ast.parse(code)
    function_node = next((n for n in tree.body if isinstance(n, ast.FunctionDef)), None)
    if function_node is None:
        raise ValueError("No function definition found.")

    arg_names = [arg.arg for arg in function_node.args.args]
    arg_port_map = {arg: f"{arg}_port" for arg in arg_names}
    var_port_map = dict(arg_port_map)

    statement_records = []
    for stmt in function_node.body:
        assigned_vars = _extract_assigned_vars(stmt)
        loaded_vars = _extract_loaded_vars(stmt) - assigned_vars

        loaded_arg_ports = {arg_port_map.get(var) for var in loaded_vars if var in arg_port_map}
        loaded_arg_ports.discard(None)
        loaded_local_ports = {
            var_port_map.get(var)
            for var in loaded_vars
            if var in var_port_map and var not in arg_port_map
        }
        loaded_local_ports.discard(None)

        # Prioritize argument-origin ports when assigning local variables.
        for var in assigned_vars:
            if loaded_arg_ports:
                var_port_map[var] = next(iter(loaded_arg_ports)) if len(loaded_arg_ports) == 1 else "shared"
            elif loaded_local_ports:
                var_port_map[var] = (
                    next(iter(loaded_local_ports)) if len(loaded_local_ports) == 1 else "shared"
                )

        if assigned_vars:
            stmt_ports = {var_port_map.get(var) for var in assigned_vars if var in var_port_map}
            stmt_ports.discard(None)
            if len(stmt_ports) == 1:
                label = next(iter(stmt_ports))
            elif len(stmt_ports) > 1:
                label = "shared"
            else:
                label = "global"
        else:
            all_ports = loaded_arg_ports | loaded_local_ports
            if len(all_ports) == 1:
                label = next(iter(all_ports))
            elif len(all_ports) > 1:
                label = "shared"
            else:
                label = "global"

        statement_records.append(
            {
                "line_number": getattr(stmt, "lineno", None),
                "statement": ast.unparse(stmt).strip(),
                "port": label,
                "_assigned_vars": assigned_vars,
                "_loaded_vars": loaded_vars,
            }
        )

    # Backward propagation for global assignments.
    global_vars = set()
    for record in statement_records:
        if record["port"] == "global":
            global_vars.update(record["_assigned_vars"])

    var_used_by_ports = {var: set() for var in global_vars}
    for record in statement_records:
        for var in global_vars:
            if var in record["_loaded_vars"] and record["port"] not in ("global", "shared"):
                var_used_by_ports[var].add(record["port"])

    for var, ports in var_used_by_ports.items():
        if len(ports) == 1:
            only_port = next(iter(ports))
            var_port_map[var] = only_port
            for record in statement_records:
                if record["port"] == "global" and var in record["_assigned_vars"]:
                    record["port"] = only_port
        elif len(ports) > 1:
            var_port_map[var] = "shared"
            for record in statement_records:
                if record["port"] == "global" and var in record["_assigned_vars"]:
                    record["port"] = "shared"

    cleaned_records = []
    for record in statement_records:
        cleaned_records.append(
            {
                "line_number": record["line_number"],
                "statement": record["statement"],
                "port": record["port"],
            }
        )

    return cleaned_records
