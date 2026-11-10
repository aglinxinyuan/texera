#!/usr/bin/env bash
# Rerun the If experiment against the deadlock-fixed pyamber, then re-verify ifbase.
set -u
export JAVA_HOME="$HOME/.jdks/jbr-17.0.14"
export PATH="$JAVA_HOME/bin:$PATH"
export UDF_PYTHON_PATH=$(python -c "import sys; print(sys.executable)")
export AMBER_TEST_FILTER=integration-only
export TEXERA_HOME="C:/Users/linxi/IdeaProjects/texera"
export PGPASSWORD=postgres
PSQL="C:/Program Files/PostgreSQL/17/bin/psql.exe"
cd "$TEXERA_HOME"

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

for att in 1 2 3; do
  purge
  rm -f experiment/if_sentinel.log
  LOOP_EXPERIMENT_MODE=if timeout 420 sbt "WorkflowExecutionService/testOnly *LoopOverheadExperimentSpec" \
    > "experiment/if_fixed_a$att.log" 2>&1
  if grep -q "LOOP_EXPERIMENT_IF " "experiment/if_fixed_a$att.log"; then
    echo "FIXED ATTEMPT $att SUCCESS"
    grep "LOOP_EXPERIMENT_IF " "experiment/if_fixed_a$att.log"
    echo "sentinel=[$(tr '\n' ',' < experiment/if_sentinel.log 2>/dev/null)]"
    break
  else
    echo "FIXED ATTEMPT $att no-result sentinel=[$(tr '\n' ',' < experiment/if_sentinel.log 2>/dev/null)]"
  fi
done

purge
rm -f experiment/if_sentinel.log
LOOP_EXPERIMENT_MODE=ifbase timeout 420 sbt "WorkflowExecutionService/testOnly *LoopOverheadExperimentSpec" \
  > experiment/ifbase_fixed.log 2>&1
grep -E "LOOP_EXPERIMENT_IFBASE|Tests:" experiment/ifbase_fixed.log | head -3
echo "ifbase sentinel=[$(tr '\n' ',' < experiment/if_sentinel.log 2>/dev/null)]"
purge
echo "FIXED CAMPAIGN DONE $(date +%H:%M:%S)"
