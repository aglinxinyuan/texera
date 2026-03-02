import pandas as pd

def enrich(df1: pd.DataFrame, df2: pd.DataFrame):
    filtered = df1[df1["x"] > 0]
    merged = pd.merge(filtered, df2, on="id", how="inner")
    merged["score"] = merged["x"] * merged["w"]
    projected = merged[["id", "score"]]
    return projected
