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
