#!/usr/bin/env bash
# Workflow side of the KNN overhead experiment. One sbt invocation per
# (n, rep) with a full state purge in between (iceberg tables are keyed by
# wid/eid, which the harness reuses).
set -u
cd "$(dirname "$0")/.."
PYEXE=$(python -c "import sys; print(sys.executable)")
export JAVA_HOME="$HOME/.jdks/jbr-17.0.14"
export PATH="$JAVA_HOME/bin:$PATH"
export UDF_PYTHON_PATH="$PYEXE"
export AMBER_TEST_FILTER=integration-only
export TEXERA_HOME="C:/Users/linxi/IdeaProjects/texera"
export PGPASSWORD=postgres
PSQL="C:/Program Files/PostgreSQL/17/bin/psql.exe"
REPS="${LOOP_EXPERIMENT_REPS:-1}"

purge() {
  "$PSQL" -U postgres -h localhost -d texera_db_for_test_cases \
    -c "DELETE FROM texera_db.workflow_executions WHERE eid=1" \
    -c "DELETE FROM texera_db.workflow_version WHERE vid=1" \
    -c "DELETE FROM texera_db.workflow WHERE wid=1" \
    -c "DELETE FROM texera_db.\"user\" WHERE uid=1" >/dev/null 2>&1
  "$PSQL" -U postgres -h localhost -d texera_iceberg_catalog \
    -c "DELETE FROM iceberg_tables" >/dev/null 2>&1
  rm -rf amber/user-resources/workflow-results/* 2>/dev/null
}

for n in "$@"; do
  for rep in $(seq 1 "$REPS"); do
    purge
    export LOOP_EXPERIMENT_N="$n"
    echo "=== n=$n rep=$rep start $(date +%H:%M:%S) ===" >> experiment/results.log
    sbt "WorkflowExecutionService/testOnly *LoopOverheadExperimentSpec" 2>&1 \
      | grep -E "LOOP_EXPERIMENT_RESULT|TEST FAILED" >> experiment/results.log
  done
done
purge
echo "SWEEP DONE $(date +%H:%M:%S)" >> experiment/results.log
