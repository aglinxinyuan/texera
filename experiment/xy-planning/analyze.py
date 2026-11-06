# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Validate the complete paired sweep and report every case, including noisy outcomes."""

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median, quantiles


def analyze(root):
    meta = json.loads((root / "metadata.json").read_text(encoding="utf-8-sig"))
    pairs = defaultdict(dict)
    rows = []
    manifests = []
    for fork in range(meta["forks"]):
        folder = root / f"fork-{fork}"
        sample = list(csv.DictReader((folder / "results.csv").open(encoding="utf-8-sig")))
        assert len(sample) == 32 * 2 * meta["repetitions"] * 2, "incomplete fork"
        log = (folder / "run.log").read_text(encoding="utf-8-sig")
        assert "[success]" in log and "XY sweep results:" in log
        manifests.append(tuple((folder / f).read_bytes() for f in ("operators.csv", "edges.csv")))
        for r in sample:
            assert int(r["fork"]) == fork
            assert r["strategy"] in ("X", "Y")
            key = r["case"], r["pruning"], fork, int(r["repetition"])
            assert r["strategy"] not in pairs[key], "duplicate strategy"
            pairs[key][r["strategy"]] = r
        rows.extend(sample)
    # Manifest case order is shuffled per fork; records must still match exactly.
    for index in (0, 1):
        first = sorted(manifests[0][index].splitlines())
        assert all(sorted(m[index].splitlines()) == first for m in manifests)
    costs = defaultdict(set)
    groups = defaultdict(list)
    for (case, pruning, fork, rep), pair in pairs.items():
        assert set(pair) == {"X", "Y"}
        assert 0 <= rep < meta["repetitions"]
        x, y = pair["X"], pair["Y"]
        assert x["order"] == y["order"]
        assert float(x["cost"]) == float(y["cost"])
        costs[case].add(float(x["cost"]))
        for r in (x, y):
            assert int(r["total_ns"]) > 0
            assert int(r["total_ns"]) == sum(int(r[k]) for k in ("decomposition_ns", "search_ns", "composition_ns"))
            if pruning == "false":
                a, b = int(r["inside_edges"]), int(r["outside_edges"])
                expected = 2 ** (a + b) if r["strategy"] == "X" else 2**a + 2**b
                assert int(r["search_states"]) == expected
        groups[case, pruning].append((fork, x, y))
    assert len(groups) == 64 and len(costs) == 32
    assert all(len(v) == 1 for v in costs.values()), "pruning/fork changed optimum"
    summary = []
    for (case, pruning), samples in sorted(groups.items()):
        assert len(samples) == meta["forks"] * meta["repetitions"]
        fork_ratios = []
        for fork in range(meta["forks"]):
            fork_ratios.append(median(int(x["total_ns"]) / int(y["total_ns"])
                                      for f, x, y in samples if f == fork))
        # A practical stability rule, not a significance test.
        winner = "Y" if min(fork_ratios) > 1.1 else "X" if max(fork_ratios) < 1/1.1 else "close/mixed"
        _, example, _ = samples[0]
        record = dict(case=case, shape=example["shape"], profile=example["profile"],
                      pruning=pruning, inside_edges=example["inside_edges"], outside_edges=example["outside_edges"],
                      operators=example["operators"], links=example["links"], cost=example["cost"],
                      winner=winner, paired_ratio=median(int(x["total_ns"])/int(y["total_ns"]) for _, x, y in samples),
                      min_fork_ratio=min(fork_ratios), max_fork_ratio=max(fork_ratios))
        for strategy, index in (("X", 1), ("Y", 2)):
            data = [p[index] for p in samples]
            times = [int(r["total_ns"]) / 1e6 for r in data]
            qs = quantiles(times, n=4, method="inclusive") if len(times) > 1 else times * 3
            record.update({f"{strategy}_median_ms": median(times), f"{strategy}_q1_ms": qs[0],
                           f"{strategy}_q3_ms": qs[2],
                           f"{strategy}_states": median(int(r["search_states"]) for r in data),
                           f"{strategy}_gc_samples": sum(int(r["gc_count"]) > 0 for r in data)})
        record["Y_composition_ms"] = median(int(y["composition_ns"])/1e6 for _, _, y in samples)
        record["Y_decomposition_ms"] = median(int(y["decomposition_ns"])/1e6 for _, _, y in samples)
        summary.append(record)
    with (root / "summary.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)

    lines = ["# Extended X/Y planning comparison", "",
             f"Revision: `{meta['revision']}`. {len(rows)} measured plans, {len(pairs)} matched pairs, "
             f"{meta['forks']} fresh JVMs, {meta['warmups']} warm-up pairs and {meta['repetitions']} measured pairs per scenario/mode/JVM.", "",
             "All measured plans passed the engine's full-plan validity/cost check. X/Y, pruning on/off and all process runs agree on the estimated minimum for every case. Exhaustive state counts match enumeration. No samples were removed for GC or timing outliers.", "",
             "Each winner below needs at least a 10% paired-median advantage in every fresh JVM. Other results are close/mixed. This is a practical repeatability rule, not a significance test or confidence interval. Raw measurements, GC counters, complete fixtures/weights and environment/source hashes are saved alongside this report.", "",
             "## Results by family", "",
             "| Pruning | Family | X wins | Y wins | Close/mixed |", "|---|---|---:|---:|---:|"]
    for pruning in ("false", "true"):
        for shape in ("chain", "shortcut", "diamond", "fanout"):
            counts = Counter(s["winner"] for s in summary if s["shape"] == shape and s["pruning"] == pruning)
            lines.append(f"| {'on' if pruning == 'true' else 'off'} | {shape} | {counts['X']} | {counts['Y']} | {counts['close/mixed']} |")
    lines += ["", "![Paired planning-time ratios](comparison.png)", "",
              "Points are medians of paired X/Y time ratios. Bars span the median ratio in each fresh JVM, not confidence intervals. Values above 1 favor Y; below 1 favor X. Timers include Y decomposition and composition.", "",
              "## Every case", "", "| Case | Edges in/out | Pruning | X ms | Y ms | X/Y paired ratio | Fork range | X/Y states | Outcome |",
              "|---|---|---|---:|---:|---:|---|---|---|"]
    for s in summary:
        lines.append(f"| {s['case']} | {s['inside_edges']}/{s['outside_edges']} | {'on' if s['pruning']=='true' else 'off'} | {s['X_median_ms']:.3f} | {s['Y_median_ms']:.3f} | {s['paired_ratio']:.2f} | {s['min_fork_ratio']:.2f}–{s['max_fork_ratio']:.2f} | {s['X_states']:.0f}/{s['Y_states']:.0f} | {s['winner']} |")
    lines += ["", "## Interpretation limits", "",
              "These are generated one-block planning graphs, not measured application traces. Uniform estimates are all 1; skewed estimates are seeded draws from {1,2,4,16}. Both use the additive sum-of-region-maxima objective. Results do not measure execution time, loop iteration counts, nested blocks, or the fallback materialized-port-count objective. The Y adapter is research code; neither the production planner selection nor the paper was changed.", "",
              "Pruning changes the work actually done, so edge-assignment counts alone cannot predict the winner. Use search states, complete elapsed time and Y decomposition/composition together. Large unpruned gaps and small pruned differences must be reported separately."]
    if (root / "oracle.json").exists():
        oracle = json.loads((root / "oracle.json").read_text())
        assert len(oracle) == 32
        assert all(costs[r["case"]] == {r["minimum_cost"]} for r in oracle)
        lines += ["", "## Independent check", "",
                  f"A separate Python connected-component/Kahn enumerator checked all {sum(r['assignments'] for r in oracle):,} assignments and confirmed the same minimum for all 32 graph/cost combinations. See oracle.json."]
    (root / "REPORT.md").write_text("\n".join(lines)+"\n", encoding="utf-8", newline="\n")
    plot(root, summary)
    print(f"Verified {len(rows)} plans / {len(pairs)} pairs. Report: {root / 'REPORT.md'}")
    for pruning in ("false", "true"):
        print(f"pruning={pruning}: {dict(Counter(s['winner'] for s in summary if s['pruning']==pruning))}")


def plot(root, summary):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    names = sorted({s["case"] for s in summary})
    fig, axes = plt.subplots(1, 2, figsize=(12, 11), sharey=True, constrained_layout=True)
    colors = {"X": "#a64b20", "Y": "#147d77", "close/mixed": "#687385"}
    for ax, pruning in zip(axes, ("false", "true")):
        ax.axvspan(1/1.1, 1.1, color="#eeeeee")
        ax.axvline(1, color="#444444", linewidth=1)
        for i, name in enumerate(names):
            s = next(s for s in summary if s["case"] == name and s["pruning"] == pruning)
            ax.plot([s['min_fork_ratio'], s['max_fork_ratio']], [i, i], color=colors[s['winner']], linewidth=2)
            ax.scatter(s['paired_ratio'], i, color=colors[s['winner']], s=25, zorder=3)
        ax.set_xscale("log")
        ax.set_xticks([1, 2, 5, 10, 20, 50, 100] if pruning == "false" else [.25, .5, 1, 2, 4])
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:g}×"))
        ax.set_title(f"Pruning {'on' if pruning == 'true' else 'off'}")
        ax.set_xlabel("X time / Y time   (higher favors Y)")
        ax.grid(axis="x", alpha=.2)
        ax.set_yticks(range(len(names)), names, fontsize=8)
    axes[0].invert_yaxis()
    fig.suptitle("Complete planning time across generated workflows", fontsize=15)
    fig.savefig(root / "comparison.png", dpi=180)
    fig.savefig(root / "comparison.pdf")
    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    analyze(parser.parse_args().results)
