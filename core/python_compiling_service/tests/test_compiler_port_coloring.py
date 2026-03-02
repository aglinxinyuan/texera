import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler import RECOMMENDED_AUTO_CUT_UDF, compile_udf
from src.compiler.port_coloring import infer_port_assignments


def test_infer_port_assignments_returns_statement_labels():
    assignments = infer_port_assignments(RECOMMENDED_AUTO_CUT_UDF)

    assert assignments
    assert all("line_number" in row for row in assignments)
    assert all("statement" in row for row in assignments)
    assert all("port" in row for row in assignments)


def test_compile_udf_includes_port_assignments_metadata():
    result = compile_udf(RECOMMENDED_AUTO_CUT_UDF)

    assert result.port_assignments
    assert any(row["port"] in ("df_events_port", "df_weights_port", "shared", "global") for row in result.port_assignments)
