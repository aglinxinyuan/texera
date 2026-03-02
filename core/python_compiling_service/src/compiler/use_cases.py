"""Canonical sample inputs for integration demos and regression checks."""

RECOMMENDED_AUTO_CUT_UDF = """import pandas as pd

def score_events(df_events: pd.DataFrame, df_weights: pd.DataFrame) -> pd.DataFrame:
    active = df_events[df_events["event"] != "idle"]
    joined = pd.merge(active, df_weights, on="event", how="left")
    joined["score"] = joined["count"] * joined["weight"].fillna(0)
    result = joined[["user_id", "event", "score"]]
    return result
"""


BASELINE_REFERENCE_UDF = """#baseline
import pandas as pd

def score_events_baseline(df_events: pd.DataFrame, df_weights: pd.DataFrame) -> pd.DataFrame:
    joined = pd.merge(df_events, df_weights, on="event", how="left")
    joined["score"] = joined["count"] * joined["weight"].fillna(0)
    return joined[["user_id", "event", "score"]]
"""
