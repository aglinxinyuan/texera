import pandas as pd

def combine(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame):
    left = pd.merge(df1, df2, on="id", how="inner")
    left["score"] = left["a"] + left["b"]
    final = pd.merge(left, df3, on="id", how="inner")
    return final[["id", "score", "label"]]
