import ast
from collections import defaultdict

import astor

from .common import get_base_variable_name
from .config import DEFAULT_TYPE_SIZE_BYTES, FIRST_USAGE_HEURISTIC_BONUS
from .cut_strategy import estimate_variable_size, rank_cuts_by_variable_size as rank_cuts_with_strategy
from .graph import (
    are_all_temporal_edges,
    draw_graph_output,
    find_valid_cuts as find_valid_cuts_in_graph,
    generate_dot_graph,
    get_edges_crossing_line,
    get_topological_order as graph_topological_order,
    has_cycle as graph_has_cycle,
    visualize_graph,
    visualize_graph_text,
)
class VariableDependencyGraph:
    """
    A class to analyze variable dependencies in SSA form code.
    
    Each vertex is a tuple (variable, line_number).
    Edges represent dependencies:
    1. If the same variable appears on different lines, there's an edge from lower to higher line number
    2. If X = Y (assignment), then there's an edge from Y to X
    """
    
    def __init__(self, ssa_code_snippet: str):
        """
        Initialize the dependency graph from SSA form code.
        
        Args:
            ssa_code_snippet (str): Code snippet in SSA format
        """
        self.ssa_code = ssa_code_snippet
        self.vertices = set()  # Set of all vertices as (variable, line_number) tuples
        self.edges = defaultdict(set)  # Adjacency list: vertex -> set of dependent vertices
        self.reverse_edges = defaultdict(set)  # Reverse adjacency list: vertex -> set of vertices that depend on it
        self.variable_versions = defaultdict(list)  # variable_name -> list of versions
        self.variable_lines = {}  # variable -> line number where defined
        self.variable_usage_lines = defaultdict(list)  # variable -> list of line numbers where used
        self.variable_types = {}  # variable -> inferred type
        
        self._build_graph()
    
    def _build_graph(self):
        """Build the dependency graph by parsing the SSA code."""
        try:
            tree = ast.parse(self.ssa_code)
            visitor = DependencyVisitor()
            visitor.visit(tree)
            
            # Extract information from the visitor
            self.variable_lines = visitor.variable_lines
            self.variable_usage_lines = visitor.variable_usage_lines
            self.variable_versions = visitor.variable_versions
            
            # Build vertices as (variable, line_number) tuples
            self.vertices = set()
            for variable, line_num in self.variable_lines.items():
                self.vertices.add((variable, line_num))
            
            # Add usage lines as vertices too
            for variable, usage_lines in self.variable_usage_lines.items():
                for line_num in usage_lines:
                    self.vertices.add((variable, line_num))
            
            # Build edges based on the visitor's dependency information
            self._build_edges_from_visitor(visitor)
            
            # Build reverse edges
            for vertex, dependents in self.edges.items():
                for dependent in dependents:
                    self.reverse_edges[dependent].add(vertex)
                    
        except Exception as e:
            raise ValueError(f"Failed to build dependency graph: {e}")
    
    def _build_edges_from_visitor(self, visitor):
        """Build edges based on the visitor's dependency information."""
        # Rule 1: Same variable edges (lower line number to higher line number)
        variable_line_mapping = defaultdict(list)
        for vertex in self.vertices:
            variable, line_num = vertex
            variable_line_mapping[variable].append((line_num, vertex))
        
        for variable, line_vertices in variable_line_mapping.items():
            # Sort by line number
            line_vertices.sort(key=lambda x: x[0])
            
            # Add edges from lower to higher line numbers (only for same variable)
            for i in range(len(line_vertices) - 1):
                current_vertex = line_vertices[i][1]
                next_vertex = line_vertices[i + 1][1]
                self.edges[current_vertex].add(next_vertex)
        
        # Rule 2: Assignment dependencies (only actual assignments, not variable versions)
        for target_var, dependencies in visitor.dependencies.items():
            target_line = visitor.variable_lines.get(target_var)
            if target_line is not None:
                target_vertex = (target_var, target_line)
                
                for dep_var in dependencies:
                    # Find the latest usage line of dep_var before or at the assignment
                    dep_lines = [l for l in visitor.variable_usage_lines.get(dep_var, []) if l <= target_line]
                    if dep_lines:
                        latest_line = max(dep_lines)
                        dep_vertex = (dep_var, latest_line)
                        if dep_var != target_var:
                            self.edges[dep_vertex].add(target_vertex)
        
        # Store type information for size estimation
        self.variable_types = visitor.variable_types
    
    def get_vertices(self):
        """Get all vertices (variable, line_number) in the graph."""
        return list(self.vertices)
    
    def get_edges(self):
        """Get all edges in the graph as a list of tuples ((from_var, from_line), (to_var, to_line))."""
        edges = []
        for from_vertex, to_vertices in self.edges.items():
            for to_vertex in to_vertices:
                edges.append((from_vertex, to_vertex))
        return edges
    
    def get_dependents(self, vertex):
        """Get all vertices that depend on the given vertex."""
        return list(self.edges.get(vertex, set()))
    
    def get_dependencies(self, vertex):
        """Get all vertices that the given vertex depends 
        on."""
        return list(self.reverse_edges.get(vertex, set()))
    
    def get_variable_versions(self, variable_name):
        """Get all versions of a variable."""
        return self.variable_versions.get(variable_name, [])
    
    def get_variable_line(self, variable):
        """Get the line number where a variable is defined."""
        return self.variable_lines.get(variable, None)
    
    def get_variable_usage_lines(self, variable):
        """Get all line numbers where a variable is used."""
        return self.variable_usage_lines.get(variable, [])
    
    def get_vertices_by_variable(self, variable):
        """Get all vertices for a specific variable."""
        return [vertex for vertex in self.vertices if vertex[0] == variable]
    
    def find_valid_cuts(self):
        """
        Find valid cut points in the dependency graph.
        A valid cut must only cut through temporal (blue) edges, not assignment (red) edges.
        
        Returns:
            list: List of valid cut points, each containing line number and the edge being cut
        """
        return find_valid_cuts_in_graph(list(self.vertices), self.edges)
    
    def _get_edges_crossing_line(self, line_num):
        """
        Get all edges that cross the given line number.
        An edge crosses a line if one vertex is before the line and one is at or after the line.
        """
        return get_edges_crossing_line(self.edges, line_num)
    
    def _are_all_temporal_edges(self, edges):
        """
        Check if all edges are temporal (same variable).
        Temporal edges connect the same variable at different lines.
        """
        return are_all_temporal_edges(edges)
    
    def rank_cuts_by_variable_size(self, valid_cuts):
        """
        Rank valid cuts by the size of variables being cut through and argument usage heuristic.
        Smaller variables are preferred for cuts, and cuts at L-1 are favored if an argument
        is first used at line L.
        
        Args:
            valid_cuts (list): List of valid cut points
            
        Returns:
            list: Ranked list of cuts (smallest variable size first, with argument usage bonus)
        """
        return rank_cuts_with_strategy(
            valid_cuts=valid_cuts,
            vertices=self.vertices,
            variable_types=self.variable_types,
            type_size_bytes=DEFAULT_TYPE_SIZE_BYTES,
            first_usage_heuristic_bonus=FIRST_USAGE_HEURISTIC_BONUS,
        )
    
    def _estimate_variable_size(self, var_name):
        """
        Estimate the size of a variable based on type information.
        DataFrames and complex data structures are much larger than simple types.
        
        Args:
            var_name (str): Variable name
            
        Returns:
            int: Estimated size (bytes)
        """
        return estimate_variable_size(self.variable_types, var_name, DEFAULT_TYPE_SIZE_BYTES)
    
    def has_cycle(self):
        """Check if the dependency graph has cycles using DFS."""
        return graph_has_cycle(self.vertices, self.edges)
    
    def get_topological_order(self):
        """Get topological ordering of vertices (if no cycles)."""
        return graph_topological_order(self.vertices, self.edges, self.reverse_edges)
    
    def visualize(self):
        """Return a string representation of the graph."""
        return visualize_graph(self.vertices, self.edges, self.reverse_edges)
    
    def visualize_text(self):
        """Return a visual ASCII representation of the dependency graph."""
        return visualize_graph_text(self.vertices, self.edges, self.reverse_edges, self.variable_versions)
    
    def generate_dot(self, filename="dependency_graph.dot"):
        """Generate a DOT file for Graphviz visualization."""
        return generate_dot_graph(self.vertices, self.edges, self.variable_lines, filename)
    
    def draw_graph(self, output_format="png", filename="dependency_graph"):
        """Draw the graph using Graphviz and save as image."""
        return draw_graph_output(self.vertices, self.edges, self.variable_lines, output_format, filename)
    
    def _get_base_variable_name(self, var_name):
        """Extract the base variable name from a versioned variable name."""
        return get_base_variable_name(var_name)


class DependencyVisitor(ast.NodeVisitor):
    """AST visitor to extract variable dependencies from SSA code."""
    
    def __init__(self):
        self.all_variables = set()
        self.dependencies = defaultdict(set)
        self.variable_versions = defaultdict(list)
        self.variable_lines = {}  # variable -> line number where defined
        self.variable_usage_lines = defaultdict(list)  # variable -> list of line numbers where used
        self.variable_types = {}  # variable -> inferred type
        self.current_assignment_target = None
    
    def visit_FunctionDef(self, node):
        """Visit function definition and process arguments."""
        # Add function arguments as vertices
        for arg in node.args.args:
            self.all_variables.add(arg.arg)
            self.variable_versions[arg.arg].append(arg.arg)
            # Function arguments are defined at the function definition line
            self.variable_lines[arg.arg] = node.lineno
            
            # Extract type annotation if available
            if arg.annotation:
                type_str = astor.to_source(arg.annotation).strip()
                self.variable_types[arg.arg] = self._parse_type_annotation(type_str)
        
        # Process function body
        self.generic_visit(node)
    
    def visit_Assign(self, node):
        """Visit assignment statements to extract dependencies."""
        # First, visit the value to collect all variables used
        used_variables = set()
        self._collect_variables(node.value, used_variables, node.lineno)
        
        # Then process the targets
        for target in node.targets:
            if isinstance(target, ast.Name):
                target_var = target.id
                self.all_variables.add(target_var)
                
                # Record the line where this variable is defined
                self.variable_lines[target_var] = node.lineno
                
                # Extract base variable name and version
                base_name = self._get_base_variable_name(target_var)
                self.variable_versions[base_name].append(target_var)
                
                # Infer type from the assignment
                inferred_type = self._infer_type_from_assignment(node.value)
                if inferred_type:
                    self.variable_types[target_var] = inferred_type
                
                # Add dependencies: target depends on all used variables
                for used_var in used_variables:
                    self.dependencies[target_var].add(used_var)
    
    def _parse_type_annotation(self, type_str):
        """Parse type annotation string to extract type information."""
        type_str = type_str.lower()
        
        # DataFrame types
        if 'dataframe' in type_str or 'pd.dataframe' in type_str:
            return 'DataFrame'
        elif 'series' in type_str or 'pd.series' in type_str:
            return 'Series'
        
        # Basic types
        elif 'int' in type_str:
            return 'int'
        elif 'float' in type_str:
            return 'float'
        elif 'str' in type_str or 'string' in type_str:
            return 'str'
        elif 'bool' in type_str:
            return 'bool'
        elif 'list' in type_str:
            return 'list'
        elif 'dict' in type_str:
            return 'dict'
        elif 'tuple' in type_str:
            return 'tuple'
        elif 'set' in type_str:
            return 'set'
        
        # Default to unknown
        return 'unknown'
    
    def _infer_type_from_assignment(self, value_node):
        """Infer the type of a variable from its assignment."""
        if isinstance(value_node, ast.Call):
            # Function call - check if it's a DataFrame operation
            if isinstance(value_node.func, ast.Attribute):
                # Method call like df1['activity']
                if isinstance(value_node.func.value, ast.Name):
                    var_name = value_node.func.value.id
                    if var_name in self.variable_types and self.variable_types[var_name] == 'DataFrame':
                        return 'Series'  # DataFrame column access returns Series
            elif isinstance(value_node.func, ast.Name):
                func_name = value_node.func.id
                if func_name in ['pd.DataFrame', 'DataFrame']:
                    return 'DataFrame'
                elif func_name in ['pd.Series', 'Series']:
                    return 'Series'
        
        elif isinstance(value_node, ast.Subscript):
            # Subscript operation like df1['activity']
            if isinstance(value_node.value, ast.Name):
                var_name = value_node.value.id
                if var_name in self.variable_types and self.variable_types[var_name] == 'DataFrame':
                    return 'Series'  # DataFrame column access returns Series
        
        elif isinstance(value_node, ast.Compare):
            # Comparison operation - result is boolean
            return 'bool'
        
        elif isinstance(value_node, ast.BinOp):
            # Binary operation - infer from operands
            return 'numeric'  # Usually numeric for arithmetic operations
        
        elif isinstance(value_node, ast.Constant):
            # Constant value
            if isinstance(value_node.value, str):
                return 'str'
            elif isinstance(value_node.value, (int, float)):
                return 'numeric'
            elif isinstance(value_node.value, bool):
                return 'bool'
        
        return 'unknown'
    
    def visit_Name(self, node):
        """Visit name nodes to collect variable references."""
        if isinstance(node.ctx, ast.Load):
            self.all_variables.add(node.id)
            base_name = self._get_base_variable_name(node.id)
            if node.id not in self.variable_versions[base_name]:
                self.variable_versions[base_name].append(node.id)
            
            # Record the line where this variable is used
            self.variable_usage_lines[node.id].append(node.lineno)
    
    def _collect_variables(self, node, variables, line_number):
        """Recursively collect all variable names from an AST node."""
        if isinstance(node, ast.Name):
            variables.add(node.id)
            # Record usage line number
            self.variable_usage_lines[node.id].append(line_number)
        elif isinstance(node, ast.BinOp):
            self._collect_variables(node.left, variables, line_number)
            self._collect_variables(node.right, variables, line_number)
        elif isinstance(node, ast.UnaryOp):
            self._collect_variables(node.operand, variables, line_number)
        elif isinstance(node, ast.Call):
            self._collect_variables(node.func, variables, line_number)
            for arg in node.args:
                self._collect_variables(arg, variables, line_number)
            for keyword in node.keywords:
                self._collect_variables(keyword.value, variables, line_number)
        elif isinstance(node, ast.List):
            for elt in node.elts:
                self._collect_variables(elt, variables, line_number)
        elif isinstance(node, ast.Tuple):
            for elt in node.elts:
                self._collect_variables(elt, variables, line_number)
        elif isinstance(node, ast.Subscript):
            # Handle subscript operations like df1['activity']
            self._collect_variables(node.value, variables, line_number)
            self._collect_variables(node.slice, variables, line_number)
        elif isinstance(node, ast.Attribute):
            # Handle attribute access like df1.columns
            self._collect_variables(node.value, variables, line_number)
        elif isinstance(node, ast.Compare):
            # Handle comparison operations like !=, ==
            self._collect_variables(node.left, variables, line_number)
            for comparator in node.comparators:
                self._collect_variables(comparator, variables, line_number)
        elif isinstance(node, ast.Constant):
            # Handle constants (strings, numbers, etc.)
            pass  # Constants don't contribute to variable dependencies
        elif isinstance(node, ast.Str):
            # Handle string literals (for backward compatibility)
            pass
        elif isinstance(node, ast.Num):
            # Handle numeric literals (for backward compatibility)
            pass
        elif isinstance(node, ast.Index):
            # Handle index operations in subscripts
            self._collect_variables(node.value, variables, line_number)
        elif isinstance(node, ast.Slice):
            # Handle slice operations
            if node.lower:
                self._collect_variables(node.lower, variables, line_number)
            if node.upper:
                self._collect_variables(node.upper, variables, line_number)
            if node.step:
                self._collect_variables(node.step, variables, line_number)
        else:
            # For any other node type, try to visit all children
            for child in ast.iter_child_nodes(node):
                self._collect_variables(child, variables, line_number)
    
    def _get_base_variable_name(self, var_name):
        """Extract the base variable name from a versioned variable name."""
        return get_base_variable_name(var_name)
