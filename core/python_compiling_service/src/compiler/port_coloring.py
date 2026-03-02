import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def infer_port_assignments(function_code: str) -> List[Dict[str, Any]]:
    """
    Infer per-statement port assignments using the experimental coloring module.

    This adapter is intentionally best-effort:
    - If the experimental module is unavailable, it returns [].
    - If inference fails for a given UDF, it returns [].
    """
    try:
        from src.port_detector import label_statements_by_port_ast
    except Exception as exc:  # pragma: no cover - environment-dependent import path
        try:
            from port_detector import label_statements_by_port_ast
        except Exception:
            logger.debug("Port coloring unavailable: %s", exc)
            return []

    try:
        return label_statements_by_port_ast(function_code)
    except Exception as exc:
        logger.debug("Port coloring failed: %s", exc)
        return []
