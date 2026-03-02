#baseline
import pandas as pd

def enrich(df1: pd.DataFrame, df2: pd.DataFrame):
    merged = pd.merge(df1, df2, on="id", how="inner")
    merged["score"] = merged["x"] * merged["w"]
    return merged[["id", "score"]]
