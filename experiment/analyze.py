"""Analyze the KNN overhead sweep: linear fit + comparison table."""

import re
import sys

wf = {}
for line in open("experiment/results.log", encoding="utf-8"):
    m = re.search(r"LOOP_EXPERIMENT_RESULT n=(\d+) rep=(\d+) seconds=([\d.]+)", line)
    if m:
        wf.setdefault(int(m.group(1)), []).append(float(m.group(3)))

sc = {}
for line in open("experiment/script_results.log", encoding="utf-8"):
    m = re.search(r"SCRIPT_RESULT n=(\d+) seconds=([\d.]+)", line)
    if m:
        sc[int(m.group(1))] = float(m.group(2))

ns = sorted(wf)
means = {n: sum(v) / len(v) for n, v in wf.items()}

# least-squares fit  t = a*n + b
xs, ys = ns, [means[n] for n in ns]
N = len(xs)
sx, sy = sum(xs), sum(ys)
sxx = sum(x * x for x in xs)
sxy = sum(x * y for x, y in zip(xs, ys))
a = (N * sxy - sx * sy) / (N * sxx - sx * sx)
b = (sy - a * sx) / N
ss_res = sum((y - (a * x + b)) ** 2 for x, y in zip(xs, ys))
ss_tot = sum((y - sy / N) ** 2 for y in ys)
r2 = 1 - ss_res / ss_tot

print(f"fit: t = {a:.2f}*n + {b:.2f}  (R^2 = {r2:.5f})")
print(f"{'n':>6} {'workflow(s)':>12} {'script(s)':>10} {'wf ms/iter':>11} {'ratio':>8}")
for n in ns:
    w = means[n]
    s = sc.get(n, float('nan'))
    print(f"{n:>6} {w:>12.1f} {s:>10.3f} {1000 * w / n:>11.0f} "
          f"{w / s if s == s else float('nan'):>8.0f}x")
print(f"\nprojection: n=1000 -> {a * 1000 + b:.0f}s (~{(a * 1000 + b) / 3600:.1f}h), "
      f"n=10000 -> {(a * 10000 + b) / 3600:.1f}h")
print(f"per-iteration overhead vs script: {1000 * a:.0f}ms vs "
      f"{1000 * sc.get(1000, 3.1) / 1000:.1f}ms")
