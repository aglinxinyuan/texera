# WAFFLE loop-block demos (for the paper walkthrough)

Two self-contained workflows, one per running example in the paper. Both are the
**same loop block** (Source → Loop Start → body → Loop End) and differ only in the
four one-line primitives — which is exactly the paper's thesis: moving between
iteration patterns edits a line of Python inside the same two operators.

Neither needs an uploaded dataset (the source operator generates its own data), so
they import and run on any Texera instance with the loop operators (apache/main,
commit 80c45be89d or later).

## How to run
1. In the Texera workspace: **top-right menu → Import Workflow** → pick the `.json`.
2. Click **Run**. Loops require materialized execution — the engine sets this
   automatically for Loop Start / Loop End.
3. Click the **Loop End** operator (or the source) → **View Result**.

## Demo 1 — Choosing K for K-means (Example 2.1, count-controlled loop)
`demo1_kmeans_choose_k.json`

| primitive | value | on |
|-----------|-------|-----|
| initialization | `K = 2` | Loop Start |
| output | `table.assign(K=K)` | Loop Start |
| update | `K += 1` | Loop End |
| condition | `K <= 6` | Loop End |

The source makes 15 points in 3 well-separated blobs. Each iteration clusters the
whole table with the current `K` and reports the inertia. `output` folds `K` into the
table so the body can read it (the body has no other view of the loop state).

**Expected result — 5 rows, a clean elbow at the true cluster count (3):**

| K | inertia |
|---|---------|
| 2 | 165.45 |
| 3 | 5.45 |
| 4 | 4.68 |
| 5 | 4.05 |
| 6 | 3.49 |

## Demo 2 — Per-file word counts (Example 2.2, data-driven loop)
`demo2_per_file_counts.json`

| primitive | value | on |
|-----------|-------|-----|
| initialization | `i = 0` | Loop Start |
| output | `table.iloc[[i]]` | Loop Start |
| update | `i += 1` | Loop End |
| condition | `i < len(table)` | Loop End |

The source is a 4-row "list of files". `output` hands the body one file per iteration;
`condition` walks the full list (the whole table travels with the loop state, so
`len(table)` is the file count, not the per-iteration slice). Loop End unions the
per-file results into one table.

**Expected result — 4 rows, one per file:**

| filename | text | word_count |
|----------|------|-----------|
| a.txt | the quick brown fox | 4 |
| b.txt | lorem ipsum dolor sit amet consectetur | 6 |
| c.txt | hello world | 2 |
| d.txt | one two three four five six | 6 |

## Provenance
Both workflows were validated end-to-end on apache/main (commit 80c45be89d) via the
`LoopOverheadExperimentSpec` harness (`demo_kmeans` / `demo_files` modes) — the exact
operator types, primitive expressions, and Python code shipped here are the ones that
ran green there.
