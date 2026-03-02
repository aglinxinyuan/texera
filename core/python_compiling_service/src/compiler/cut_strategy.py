from typing import Dict, List, Sequence, Tuple


def estimate_variable_size(
    variable_types: Dict[str, str], var_name: str, type_size_bytes: Dict[str, int]
) -> int:
    var_type = variable_types.get(var_name, "unknown")
    return type_size_bytes.get(var_type, type_size_bytes["unknown"])


def rank_cuts_by_variable_size(
    *,
    valid_cuts: List[dict],
    vertices: Sequence[Tuple[str, int]],
    variable_types: Dict[str, str],
    type_size_bytes: Dict[str, int],
    first_usage_heuristic_bonus: int,
) -> List[dict]:
    ranked_cuts = []

    # Find first usage line for each argument
    argument_first_usage = {}
    for var_name, line_num in vertices:
        if var_name in variable_types:
            if var_name not in argument_first_usage or line_num < argument_first_usage[var_name]:
                argument_first_usage[var_name] = line_num

    for cut in valid_cuts:
        total_size = 0
        cut_variables = set()

        for from_vertex, _to_vertex in cut["crossing_edges"]:
            from_var, _from_line = from_vertex
            cut_variables.add(from_var)
            total_size += estimate_variable_size(variable_types, from_var, type_size_bytes)

        heuristic_bonus = 0
        cut_line = cut["line_number"]
        for _arg_name, first_usage_line in argument_first_usage.items():
            if cut_line == first_usage_line - 1:
                heuristic_bonus = first_usage_heuristic_bonus

        ranked_cut = {
            "line_number": cut["line_number"],
            "crossing_edges": cut["crossing_edges"],
            "description": cut["description"],
            "cut_variables": list(cut_variables),
            "total_variable_size": total_size,
            "average_variable_size": total_size / len(cut_variables) if cut_variables else 0,
            "rank_score": total_size + heuristic_bonus,  # Lower is better
            "heuristic_bonus": heuristic_bonus,
        }
        ranked_cuts.append(ranked_cut)

    ranked_cuts.sort(key=lambda x: x["rank_score"])
    return ranked_cuts
