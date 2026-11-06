<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Small X/Y planning comparison

Run from this worktree in PowerShell:

```powershell
./experiment/xy-planning/run.ps1
```

The default performs two warm-up pairs and five measured pairs for each of four workflows, with pruning off and on. It first runs the correctness tests. Raw timings, source hashes, revision, environment and the build log go under `target/xy-planning/`. Java17, sbt and Git Bash must be installed. A fresh checkout also needs the usual Texera jOOQ database for compilation; the pilot does not query recorded workflow statistics or run workers.

For a quick smoke run: `./experiment/xy-planning/run.ps1 -Repetitions 1 -Warmups 0`.

On other platforms, use Java17 and the two sbt commands in `run.ps1`; the Scala main accepts `[output.csv] [repetitions] [warmups]`.

## Question

Does decomposing one loop into two searches reduce complete planning time while retaining the minimum estimated cost? The pilot measures the actual PASTA search code with a small research adapter for Y. It does not measure workflow execution time.

## Workflow family

```text
             sealed block
             +------------------------------+
s0 -> ... -> so | LS -> b1 -> ... -> LE | -> sink
 |           +---------|--------------^-----+     ^
 |                     +---- stored --+           |
 +-----------------------------------------------+
```

The two block boundary edges are stored. `b1` has a streaming output on the chain and a blocking output directly to `LE`. The outer bypass is `s0 -> sink`. These two branches create unschedulable region assignments; the example is more than two independent chains.

The four `(block selectable edges, outer selectable edges)` pairs are `(3,2)`, `(3,4)`, `(5,2)` and `(5,4)`. LoopStart/LoopEnd here are graph roles on synthetic physical operators. No loop code or data processing runs. The harness enforces the paper's two boundary cuts directly, so the deployed loop-mode coercion is outside this measurement.

## What runs

- **X:** PASTA `bottomUpSearch(globalSearch=true)` on the whole physical DAG.
- **Y:** extract the block, collapse it into a macro in the outer DAG, invoke the same search separately, map selected cuts back to original edges, then rebuild and allocate the flat region plan using PASTA's normal region construction and cost evaluation.
- **Cost:** fixed positive synthetic operator-time estimates; each region costs its maximum operator estimate, and the plan sums region costs. This is the statistics-based estimator's formula with reproducible supplied estimates. The isolated macro has zero cost to avoid counting the block twice. Resource allocation, storage URI construction, memoization and schedulability checks still use engine code. The port-count estimator is outside this pilot.
- **Pruning:** both strategies use either all three bottom-up optimizations or none. Exhaustive runs establish the reference minimum. Every measured pair must return equal costs and independently pass full-plan schedulability/cost verification.
- **Timing:** outer wall-clock timers include planner construction and cost evaluation. Y also includes validation, decomposition and final flat-plan reconstruction/allocation. Common fixture creation, sbt startup, Scala compilation and independent post-run verification are outside the timed interval. New planner instances give each search a fresh memo table. X/Y order alternates per repetition; warm-ups are excluded. JIT compilation and garbage collection can still affect measured samples.

`search_states` counts states visited inside the searches. Y's additional composition validation is included in `composition_ns` and `total_ns`, but not in that counter. The executable throws on invalid blocks, missing cuts, a cyclic composed plan or a cost mismatch.

## Limits and next measurement

This is a runnable one-block pilot, not a production Y integration or submission-ready benchmark. It uses fixed estimates and four small graph shapes; it does not establish a general speedup, model loop iterations, or measure nested loops. A zero-valued isolated macro is valid for the additive objective only. Compare complete times and estimated costs, not just assignment counts. Repeat in fresh JVMs before interpreting small timing differences; preserve raw samples and report both pruning modes.

The source change adds only estimator injection (the default is unchanged) and a package-visible evaluator for a supplied materialization assignment. Main/prototype checkouts and the paper remain unchanged. All work is local to `codex/xy-planning-pilot`.

## Larger and varied sweep

```powershell
./experiment/xy-planning/run-extended.ps1
```

This runs 16 topologies with two cost profiles: chains, a chain with a blocking shortcut, serial diamonds, and fan-out/merge graphs. Each family has increasing sizes; extra chain/shortcut cases put most selectable edges inside or outside the block. The largest cases have 13 selectable edges (8,192 assignments without pruning). The single-block contract and the real PASTA search/composition path are unchanged.

The `uniform` profile assigns 1 to every operator. The `skewed` profile uses seeded draws from {1,2,4,16}; the weights are identical for both strategies and pruning modes on each graph. All operators and edges, including required cuts and weights, are saved as CSV. These are generated graphs, not application workloads.

Defaults: three fresh JVMs, one warm-up pair and three measured pairs per graph/profile/pruning mode/JVM, for 1,152 measured plans. Scenario and pruning-mode order are shuffled deterministically per JVM; strategy order alternates. Full-plan checks require all four strategy/pruning combinations to have the same minimum cost. All samples stay in the report, including GC-affected samples. Increase `-Repetitions` and `-Warmups` for a longer run.

Results go into a new timestamped directory under `target/xy-planning-extended/`. It contains metadata, per-JVM logs/raw samples/fixtures, `summary.csv`, `REPORT.md`, and PNG/PDF/SVG plots. Python with matplotlib is needed for the report (override `-Python` on other installations). Rebuild a report with `python -I experiment/xy-planning/analyze.py <result-directory>`.

After timing, `oracle.py` independently enumerates the saved graphs using connected components and cycle detection, and checks their minimum costs against every measurement. It does not call the Scala planner. Re-run it with `python -I experiment/xy-planning/oracle.py <result-directory>`; results are saved in `oracle.json`.

The report calls X or Y a repeatable winner only when it has at least a 10% paired-median advantage in every JVM; otherwise it says close/mixed. The plotted bars span per-JVM medians, not confidence intervals. This does not turn synthetic planning measurements into a workflow-runtime or general-speedup claim.
# Compiled workflow comparison

Run `powershell -File experiment/xy-planning/run-compiled.ps1` from this worktree.
The default is 3 fresh JVMs, 20 warm-up pairs and 100 measured pairs per graph,
with production pruning enabled and alternating X/Y order. Output is under
`target/xy-planning-compiled/<timestamp>/`; `analyze-compiled.py` verifies the
saved selected cuts independently and writes `REPORT.md` and `summary.csv`.

`workflows/` holds byte copies of the existing K-means and file-processing GUI
demos and a derived word/character-count branching variant; `provenance.json`
records their sources. Strict compilation preserves actual UDF executors,
schemas, port definitions and partition requirements. The file demo uses
embedded text records rather than native Read File or a control-variable port.
Both demos terminate at LoopEnd. The adapter adds the model's incoming loop
boundary cut; it adds no internal blocking edges. The compiled table UDF in the
K-means demo has a nonblocking output declaration.

This is a planning experiment, not workflow execution: main still dispatches
loops to fully materialized scheduling. The harness directly invokes PASTA's
search, and Y is a research implementation that decomposes and reconstructs the
plan. Both use the same fixed estimates (1 per original operator, 0 for Y's
macro), with cost = sum of region maxima. GUI result-view storage settings are
excluded. Compilation and validation are outside the timer; Y decomposition
and reconstruction are inside it. No data processing or pipeline runtime is
measured, and equal model costs do not imply measured runtime equality.

## Native blocking-port comparison

Run `powershell -File experiment/xy-planning/run-compiled.ps1 -Suite native`.
The three strict compilations use built-in CSV scan, filter, aggregate, sort
and hash-join descriptors. The workflow definitions are in `NativeXYFixtures`;
each run also saves their logical JSON. `native-input.csv` is the local source.
There is no native K-means descriptor in this checkout.

The cases are grouped totals followed by sorting, totals joined back to rows,
and separately computed totals/counts joined and sorted. Every loop has one
entry and exit. Compiler-declared internal blocking ports and dependencies
are preserved, including aggregate local/global stages and hash-join
build/probe stages. Only the model's two external loop-boundary cuts are added.
The local aggregate output is intentionally not marked blocking by the current
compiler; this experiment does not change it.

The output directory is `target/xy-planning-native/<timestamp>/`. Physical IDs
include layer names, so oracle checks cannot merge stages of one logical
operator. All other timing and cost assumptions above still apply. This is
neither a data-processing runtime test nor a production pipeline change.
