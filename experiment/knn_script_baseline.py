"""Script side of the KNN overhead experiment (WAFFLE paper, Section 6.3).

The exact task the loop workflow's body performs, as a plain Python for
loop: read the transcript CSV once, then n times (train/test split,
TF-IDF, KNN fit, accuracy). Mirrors the candidacy-2024 experiment.

Run:  python experiment/knn_script_baseline.py [n ...]
"""

import sys
import time

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier


def run_once(table: pd.DataFrame) -> float:
    X = table["content"].astype(str)
    y = table["positive"].astype(int)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=0
    )
    vec = TfidfVectorizer()
    Xtr = vec.fit_transform(X_train)
    Xte = vec.transform(X_test)
    knn = KNeighborsClassifier()
    knn.fit(Xtr, y_train)
    return accuracy_score(y_test, knn.predict(Xte))


def main() -> None:
    ns = [int(a) for a in sys.argv[1:]] or [10, 100, 1000, 10000]
    table = pd.read_csv("experiment/transcripts.csv")
    accs = []
    for n in ns:
        t0 = time.perf_counter()
        for _ in range(n):
            accs.append(run_once(table))
        secs = time.perf_counter() - t0
        print(f"SCRIPT_RESULT n={n} seconds={secs:.3f} "
              f"per_iter_ms={1000 * secs / n:.2f} acc={accs[-1]:.3f}")


if __name__ == "__main__":
    main()
