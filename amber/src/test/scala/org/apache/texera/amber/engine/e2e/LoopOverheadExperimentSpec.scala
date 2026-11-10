package org.apache.texera.amber.engine.e2e

// Experiment harness for the WAFFLE paper (Section 6.3): end-to-end time of
// a KNN task repeated n times as a loop workflow, to compare against the
// same task in a plain Python script. NOT part of the test suite --- run
// explicitly:
//   LOOP_EXPERIMENT_N=100 sbt "WorkflowExecutionService/testOnly *LoopOverheadExperimentSpec"

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.pekko.util.Timeout
import com.twitter.util.{Await, Promise, Return, Throw}
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.workflow.{
  ExecutionMode,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.amber.engine.architecture.coordinator._
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmptyRequest
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  readMaterializedResults,
  registerResultUriPersistence,
  setUpWorkflowExecutionData,
  workflowContext
}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.amber.operator.udf.python.PythonUDFOpDescV2
import org.apache.texera.amber.operator.udf.python.source.PythonUDFSourceOpDescV2
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.workflow.LogicalLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration.DurationInt

// Tagged @IntegrationTest (spawns Python workers) so AMBER_TEST_FILTER=
// integration-only (-n IntegrationTest) discovers it; without the tag the
// filter runs zero tests.
@IntegrationTest
class LoopOverheadExperimentSpec
    extends TestKit(ActorSystem("LoopOverheadExperimentSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private val specId = 1

  private def materializedContext(): WorkflowContext =
    workflowContext(
      specId,
      WorkflowSettings(
        dataTransferBatchSize = 400,
        executionMode = ExecutionMode.MATERIALIZED
      )
    )

  /** Execute and time a workflow; returns elapsed wall-clock seconds
    * (no result fetching, no artificial sleep). */
  private def timeWorkflow(workflow: Workflow): Double = {
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      CoordinatorConfig.default,
      _ => {}
    )
    val completion = Promise[Unit]()
    client.registerCallback[FatalError](evt => completion.updateIfEmpty(Throw(evt.e)))
    client.registerCallback[ExecutionStateUpdate](evt => {
      if (evt.state == COMPLETED) completion.updateIfEmpty(Return(()))
    })
    val t0 = System.nanoTime()
    try {
      Await.result(client.coordinatorInterface.startWorkflow(EmptyRequest(), ()))
      Await.result(completion)
      (System.nanoTime() - t0) / 1e9
    } finally {
      client.shutdown()
    }
  }

  private def knnLoopWorkflow(n: Int): Workflow = {
    // same test-resources location and path pattern as TestOperators' CSVs
    val csvPath = s"${TestOperators.parentDir}/src/test/resources/transcripts.csv"
    val scan = TestOperators.getCsvScanOpDesc(csvPath, header = true)

    val loopStart = new LoopStartOpDesc()
    loopStart.initialization = "i = 0"
    loopStart.output = "table"

    val knn = new PythonUDFOpDescV2()
    knn.workers = 1
    knn.retainInputColumns = false
    knn.outputColumns = List(
      new org.apache.texera.amber.core.tuple.Attribute(
        "accuracy",
        org.apache.texera.amber.core.tuple.AttributeType.DOUBLE
      )
    )
    // LOOP_EXPERIMENT_BODY=trivial swaps the KNN body for a same-schema
    // one-row body with no sklearn import and no compute, isolating the
    // body-specific share of the per-iteration cost. pandas is already
    // loaded by the worker's own import chain, so its import is free.
    val trivialBody =
      sys.env.getOrElse("LOOP_EXPERIMENT_BODY", "knn") == "trivial"
    if (trivialBody) {
      knn.code =
        """
          |from pytexera import *
          |import pandas as pd
          |
          |class ProcessTableOperator(UDFTableOperator):
          |    @overrides
          |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
          |        yield pd.DataFrame({'accuracy': [0.0]})
          |""".stripMargin
    } else
    knn.code =
      """
        |from pytexera import *
        |import pandas as pd
        |from sklearn.model_selection import train_test_split
        |from sklearn.feature_extraction.text import TfidfVectorizer
        |from sklearn.neighbors import KNeighborsClassifier
        |from sklearn.metrics import accuracy_score
        |
        |class ProcessTableOperator(UDFTableOperator):
        |    @overrides
        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        |        X = table['content'].astype(str)
        |        y = table['positive'].astype(int)
        |        X_train, X_test, y_train, y_test = train_test_split(
        |            X, y, test_size=0.2, random_state=0)
        |        vec = TfidfVectorizer()
        |        Xtr = vec.fit_transform(X_train)
        |        Xte = vec.transform(X_test)
        |        knn = KNeighborsClassifier()
        |        knn.fit(Xtr, y_train)
        |        acc = accuracy_score(y_test, knn.predict(Xte))
        |        yield pd.DataFrame({'accuracy': [float(acc)]})
        |""".stripMargin

    val loopEnd = new LoopEndOpDesc()
    loopEnd.condition = s"i < $n"
    loopEnd.update = "i += 1"

    val ops = List(scan, loopStart, knn, loopEnd)
    val links = List(
      LogicalLink(
        scan.operatorIdentifier,
        PortIdentity(),
        loopStart.operatorIdentifier,
        PortIdentity()
      ),
      LogicalLink(
        loopStart.operatorIdentifier,
        PortIdentity(),
        knn.operatorIdentifier,
        PortIdentity()
      ),
      LogicalLink(
        knn.operatorIdentifier,
        PortIdentity(),
        loopEnd.operatorIdentifier,
        PortIdentity()
      )
    )
    buildWorkflow(ops, links, materializedContext())
  }

  "LoopOverheadExperiment" should "run the KNN loop workflow and report time" in {
    assume(sys.env.getOrElse("LOOP_EXPERIMENT_MODE", "knn") == "knn")
    val n = sys.env.getOrElse("LOOP_EXPERIMENT_N", "3").toInt
    val reps = sys.env.getOrElse("LOOP_EXPERIMENT_REPS", "1").toInt
    for (r <- 1 to reps) {
      cleanupWorkflowExecutionData(specId) // self-heal leftovers from crashed runs
      setUpWorkflowExecutionData(specId)
      try {
        val secs = timeWorkflow(knnLoopWorkflow(n))
        val body = sys.env.getOrElse("LOOP_EXPERIMENT_BODY", "knn")
        println(
          f"LOOP_EXPERIMENT_RESULT body=$body n=$n rep=$r seconds=$secs%.3f"
        )
      } finally {
        cleanupWorkflowExecutionData(specId)
      }
    }
  }

  // ---- nested 3x3 validation (paper Section 5.3 / 6.3) --------------------
  // Outer loop (i = 0..2) containing an inner loop (j = 0..2); the body
  // emits exactly one row per execution. Correct semantics: the inner
  // LoopEnd resets its accumulated output once per outer iteration, the
  // outer LoopEnd never resets -> the terminal result has exactly
  // 3 x 3 = 9 rows. A non-resetting inner LoopEnd would yield
  // 3 + 6 + 9 = 18 rows.

  private def nestedLoopWorkflow(
      outerN: Int,
      innerN: Int
  ): (Workflow, LoopEndOpDesc) = {
    val csvPath = s"${TestOperators.parentDir}/src/test/resources/transcripts.csv"
    val scan = TestOperators.getCsvScanOpDesc(csvPath, header = true)

    val outerStart = new LoopStartOpDesc()
    outerStart.initialization = "i = 0"
    outerStart.output = "table"

    val innerStart = new LoopStartOpDesc()
    innerStart.initialization = "j = 0"
    innerStart.output = "table"

    val body = new PythonUDFOpDescV2()
    body.workers = 1
    body.retainInputColumns = false
    body.outputColumns = List(
      new org.apache.texera.amber.core.tuple.Attribute(
        "v",
        org.apache.texera.amber.core.tuple.AttributeType.DOUBLE
      )
    )
    body.code =
      """
        |from pytexera import *
        |import pandas as pd
        |
        |class ProcessTableOperator(UDFTableOperator):
        |    @overrides
        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        |        yield pd.DataFrame({'v': [1.0]})
        |""".stripMargin

    val innerEnd = new LoopEndOpDesc()
    innerEnd.condition = s"j < $innerN"
    innerEnd.update = "j += 1"

    val outerEnd = new LoopEndOpDesc()
    outerEnd.condition = s"i < $outerN"
    outerEnd.update = "i += 1"

    val ops = List(scan, outerStart, innerStart, body, innerEnd, outerEnd)
    val links = List(
      LogicalLink(
        scan.operatorIdentifier,
        PortIdentity(),
        outerStart.operatorIdentifier,
        PortIdentity()
      ),
      LogicalLink(
        outerStart.operatorIdentifier,
        PortIdentity(),
        innerStart.operatorIdentifier,
        PortIdentity()
      ),
      LogicalLink(
        innerStart.operatorIdentifier,
        PortIdentity(),
        body.operatorIdentifier,
        PortIdentity()
      ),
      LogicalLink(
        body.operatorIdentifier,
        PortIdentity(),
        innerEnd.operatorIdentifier,
        PortIdentity()
      ),
      LogicalLink(
        innerEnd.operatorIdentifier,
        PortIdentity(),
        outerEnd.operatorIdentifier,
        PortIdentity()
      )
    )
    (buildWorkflow(ops, links, materializedContext()), outerEnd)
  }

  /** Execute, wait for COMPLETED, and fetch the terminal operator's rows. */
  private def runAndFetch(
      workflow: Workflow,
      terminal: LoopEndOpDesc
  ): (List[Tuple], Double) = {
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      CoordinatorConfig.default,
      _ => {}
    )
    val completion = Promise[Unit]()
    client.registerCallback[FatalError](evt => completion.updateIfEmpty(Throw(evt.e)))
    // main persists result URIs to the DB via this callback; without it the
    // post-completion getResultUriByLogicalPortId lookup finds nothing.
    registerResultUriPersistence(client, workflow.context.executionId)
    client.registerCallback[ExecutionStateUpdate](evt => {
      if (evt.state == COMPLETED) completion.updateIfEmpty(Return(()))
    })
    val t0 = System.nanoTime()
    try {
      Await.result(client.coordinatorInterface.startWorkflow(EmptyRequest(), ()))
      Await.result(completion)
      val elapsed = (System.nanoTime() - t0) / 1e9
      Thread.sleep(1000) // result-commit lag; same workaround as DataProcessingSpec
      val rows = readMaterializedResults(
        workflow.context.executionId,
        List(terminal.operatorIdentifier),
        _.get().toList
      ).getOrElse(terminal.operatorIdentifier, List.empty[Tuple])
      (rows, elapsed)
    } finally {
      client.shutdown()
    }
  }

  "LoopOverheadExperiment" should "run the nested 3x3 loop and yield 9 rows" in {
    assume(sys.env.getOrElse("LOOP_EXPERIMENT_MODE", "knn") == "nested")
    cleanupWorkflowExecutionData(specId)
    setUpWorkflowExecutionData(specId)
    try {
      val (workflow, outerEnd) = nestedLoopWorkflow(3, 3)
      val (rows, secs) = runAndFetch(workflow, outerEnd)
      println(
        f"LOOP_EXPERIMENT_NESTED outer=3 inner=3 rows=${rows.size} seconds=$secs%.3f"
      )
      assert(rows.size == 9)
    } finally {
      cleanupWorkflowExecutionData(specId)
    }
  }

  // ---- If case study (paper Section 5.6 / 6.3) ----------------------------
  // Condition: a tuple-level UDF counts the scan's rows and emits state
  // {'cond': count > 1000} on finish -> False for the 239-row dataset.
  // The If routes data to its False port; each branch is a "model" UDF that
  // sleeps MODEL_SECS and emits one row -- but only if it receives rows.
  // Sentinel file records which branch workers opened and which ran.
  // Diagnostics: taken terminal = 1 row; untaken terminal = 0 rows or NO
  // result URI at all (the latter = branch never deployed). Routing to the
  // False port also proves the condition state arrived before the data,
  // since IfOpExec defaults to the True port.

  private val ModelSecs = 10
  private val sentinelPath =
    sys.env
      .getOrElse("TEXERA_HOME", new java.io.File(".").getCanonicalPath)
      .stripSuffix("/") + "/experiment/if_sentinel.log"

  private def modelUdf(tag: String, alwaysRun: Boolean): PythonUDFOpDescV2 = {
    val m = new PythonUDFOpDescV2()
    m.workers = 1
    m.retainInputColumns = false
    m.outputColumns = List(
      new org.apache.texera.amber.core.tuple.Attribute(
        "branch",
        org.apache.texera.amber.core.tuple.AttributeType.STRING
      )
    )
    val runGuard = if (alwaysRun) "True" else "len(table) > 0"
    m.code =
      s"""
         |from pytexera import *
         |import pandas as pd
         |import time
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    @overrides
         |    def open(self):
         |        with open('$sentinelPath', 'a') as f:
         |            f.write('$tag-open\\n')
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if $runGuard:
         |            with open('$sentinelPath', 'a') as f:
         |                f.write('$tag-run\\n')
         |            time.sleep($ModelSecs)
         |            yield pd.DataFrame({'branch': ['$tag']})
         |""".stripMargin
    m
  }

  private def condUdf(): PythonUDFOpDescV2 = {
    val c = new PythonUDFOpDescV2()
    c.workers = 1
    c.retainInputColumns = true
    c.code =
      """
        |from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    @overrides
        |    def open(self):
        |        self.count = 0
        |
        |    @overrides
        |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        |        self.count += 1
        |        return
        |        yield
        |
        |    def produce_state_on_finish(self, port: int):
        |        return {'cond': self.count > 1000}
        |""".stripMargin
    c
  }

  private def ifWorkflow()
      : (Workflow, PythonUDFOpDescV2, PythonUDFOpDescV2) = {
    val csvPath =
      s"${TestOperators.parentDir}/src/test/resources/transcripts.csv"
    val scan = TestOperators.getCsvScanOpDesc(csvPath, header = true)
    val cond = condUdf()
    val ifOp = new org.apache.texera.amber.operator.ifStatement.IfOpDesc()
    ifOp.conditionName = "cond"
    val modelT = modelUdf("T", alwaysRun = false)
    val modelF = modelUdf("F", alwaysRun = false)

    val ops = List(scan, cond, ifOp, modelT, modelF)
    val links = List(
      LogicalLink(
        scan.operatorIdentifier,
        PortIdentity(),
        cond.operatorIdentifier,
        PortIdentity()
      ),
      // condition state into If port 0
      LogicalLink(
        cond.operatorIdentifier,
        PortIdentity(),
        ifOp.operatorIdentifier,
        PortIdentity()
      ),
      // data directly into If port 1 (dependee: waits for port 0)
      LogicalLink(
        scan.operatorIdentifier,
        PortIdentity(),
        ifOp.operatorIdentifier,
        PortIdentity(1)
      ),
      // False port -> modelF
      LogicalLink(
        ifOp.operatorIdentifier,
        PortIdentity(),
        modelF.operatorIdentifier,
        PortIdentity()
      ),
      // True port -> modelT
      LogicalLink(
        ifOp.operatorIdentifier,
        PortIdentity(1),
        modelT.operatorIdentifier,
        PortIdentity()
      )
    )
    (
      buildWorkflow(ops, links, materializedContext()),
      modelT,
      modelF
    )
  }

  private def bothBranchesWorkflow()
      : (Workflow, PythonUDFOpDescV2, PythonUDFOpDescV2) = {
    val csvPath =
      s"${TestOperators.parentDir}/src/test/resources/transcripts.csv"
    val scan = TestOperators.getCsvScanOpDesc(csvPath, header = true)
    val modelT = modelUdf("BT", alwaysRun = true)
    val modelF = modelUdf("BF", alwaysRun = true)
    val ops = List(scan, modelT, modelF)
    val links = List(
      LogicalLink(
        scan.operatorIdentifier,
        PortIdentity(),
        modelT.operatorIdentifier,
        PortIdentity()
      ),
      LogicalLink(
        scan.operatorIdentifier,
        PortIdentity(),
        modelF.operatorIdentifier,
        PortIdentity()
      )
    )
    (buildWorkflow(ops, links, materializedContext()), modelT, modelF)
  }

  /** Run to completion; return (elapsed, rows per terminal or -1 if the
    * terminal has no result storage at all). */
  private def runAndCount(
      workflow: Workflow,
      terminals: List[PythonUDFOpDescV2]
  ): (Double, Map[String, Int]) = {
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      CoordinatorConfig.default,
      _ => {}
    )
    val completion = Promise[Unit]()
    client.registerCallback[FatalError](evt => completion.updateIfEmpty(Throw(evt.e)))
    registerResultUriPersistence(client, workflow.context.executionId)
    client.registerCallback[ExecutionStateUpdate](evt => {
      if (evt.state == COMPLETED) completion.updateIfEmpty(Return(()))
    })
    val t0 = System.nanoTime()
    try {
      Await.result(client.coordinatorInterface.startWorkflow(EmptyRequest(), ()))
      Await.result(completion)
      val elapsed = (System.nanoTime() - t0) / 1e9
      Thread.sleep(1000)
      // A terminal with no materialized result document is absent from the
      // map -> report -1 (its output storage was never created).
      val present = readMaterializedResults(
        workflow.context.executionId,
        terminals.map(_.operatorIdentifier),
        _.get().toList.size
      )
      val counts = terminals.map { op =>
        op.operatorIdentifier.id -> present.getOrElse(op.operatorIdentifier, -1)
      }.toMap
      (elapsed, counts)
    } finally {
      client.shutdown()
    }
  }

  // One workflow per JVM: the harness reuses wid=1/eid=1, so the If run
  // and the both-branches baseline are separate modes driven by separate
  // sbt invocations (with a purge in between), like the loop sweeps.

  "LoopOverheadExperiment" should "run the If case study" in {
    assume(sys.env.getOrElse("LOOP_EXPERIMENT_MODE", "knn") == "if")
    new java.io.File(sentinelPath).delete()
    cleanupWorkflowExecutionData(specId)
    setUpWorkflowExecutionData(specId)
    try {
      val (wf, t, f) = ifWorkflow()
      val (secs, counts) = runAndCount(wf, List(t, f))
      val named = counts.map { case (k, v) =>
        (if (k == t.operatorIdentifier.id) "T" else "F") -> v
      }
      val sentinel =
        scala.io.Source.fromFile(sentinelPath).getLines().toList.sorted
      println(
        f"LOOP_EXPERIMENT_IF if_seconds=$secs%.3f if_counts=$named " +
          s"sentinel=${sentinel.mkString(",")}"
      )
      // condition is False (239 < 1000): all rows exit the False port
      assert(named("F") == 1)
      assert(named("T") <= 0) // 0 rows, or -1 = storage never created
      assert(sentinel.contains("F-run") && !sentinel.contains("T-run"))
    } finally cleanupWorkflowExecutionData(specId)
  }

  "LoopOverheadExperiment" should "run the both-branches baseline" in {
    assume(sys.env.getOrElse("LOOP_EXPERIMENT_MODE", "knn") == "ifbase")
    cleanupWorkflowExecutionData(specId)
    setUpWorkflowExecutionData(specId)
    try {
      val (wf, t, f) = bothBranchesWorkflow()
      val (secs, counts) = runAndCount(wf, List(t, f))
      val named = counts.map { case (k, v) =>
        (if (k == t.operatorIdentifier.id) "BT" else "BF") -> v
      }
      val sentinel =
        scala.io.Source.fromFile(sentinelPath).getLines().toList.sorted
      println(
        f"LOOP_EXPERIMENT_IFBASE base_seconds=$secs%.3f " +
          s"base_counts=$named sentinel=${sentinel.mkString(",")}"
      )
      assert(named("BT") == 1 && named("BF") == 1)
    } finally cleanupWorkflowExecutionData(specId)
  }

  // ---- Paper-demo workflows (validate the exact JSON demos on main) ----

  private def attr(name: String, t: org.apache.texera.amber.core.tuple.AttributeType) =
    new org.apache.texera.amber.core.tuple.Attribute(name, t)

  private def runAndReadRows(
      workflow: Workflow,
      ops: List[org.apache.texera.amber.operator.LogicalOp]
  ): (Double, Map[String, List[String]]) = {
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      CoordinatorConfig.default,
      _ => {}
    )
    val completion = Promise[Unit]()
    client.registerCallback[FatalError](evt => completion.updateIfEmpty(Throw(evt.e)))
    registerResultUriPersistence(client, workflow.context.executionId)
    client.registerCallback[ExecutionStateUpdate](evt => {
      if (evt.state == COMPLETED) completion.updateIfEmpty(Return(()))
    })
    val t0 = System.nanoTime()
    try {
      Await.result(client.coordinatorInterface.startWorkflow(EmptyRequest(), ()))
      Await.result(completion)
      val elapsed = (System.nanoTime() - t0) / 1e9
      Thread.sleep(1000)
      val present = readMaterializedResults(
        workflow.context.executionId,
        ops.map(_.operatorIdentifier),
        _.get().toList.map(_.toString)
      )
      val rows =
        ops.map(op => op.operatorIdentifier.id -> present.getOrElse(op.operatorIdentifier, Nil)).toMap
      (elapsed, rows)
    } finally client.shutdown()
  }

  import org.apache.texera.amber.core.tuple.AttributeType

  private def demoKmeansWorkflow(): (Workflow, LoopEndOpDesc) = {
    val source = new PythonUDFSourceOpDescV2()
    source.workers = 1
    source.columns = List(attr("x", AttributeType.DOUBLE), attr("y", AttributeType.DOUBLE), attr("K", AttributeType.INTEGER))
    source.code =
      """from pytexera import *
        |import pandas as pd
        |
        |class GenerateOperator(UDFSourceOperator):
        |    @overrides
        |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        |        centers = [(0.0, 0.0), (8.0, 8.0), (0.0, 8.0)]
        |        offs = [(-0.5, -0.5), (0.5, 0.4), (-0.4, 0.5), (0.5, -0.5), (0.0, 0.0)]
        |        rows = []
        |        for cx, cy in centers:
        |            for dx, dy in offs:
        |                rows.append({"x": cx + dx, "y": cy + dy, "K": 0})
        |        yield pd.DataFrame(rows)
        |""".stripMargin

    val loopStart = new LoopStartOpDesc()
    loopStart.initialization = "K = 2"
    loopStart.output = "table.assign(K=K)"

    val body = new PythonUDFOpDescV2()
    body.workers = 1
    body.retainInputColumns = false
    body.outputColumns = List(attr("K", AttributeType.INTEGER), attr("inertia", AttributeType.DOUBLE))
    body.code =
      """from pytexera import *
        |import pandas as pd
        |from sklearn.cluster import KMeans
        |
        |class ProcessTableOperator(UDFTableOperator):
        |    @overrides
        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        |        k = int(table["K"].iloc[0])
        |        X = table[["x", "y"]].to_numpy()
        |        km = KMeans(n_clusters=k, n_init=10, random_state=0).fit(X)
        |        yield pd.DataFrame([{"K": k, "inertia": float(km.inertia_)}])
        |""".stripMargin

    val loopEnd = new LoopEndOpDesc()
    loopEnd.update = "K += 1"
    loopEnd.condition = "K <= 6"

    val ops = List(source, loopStart, body, loopEnd)
    val links = List(
      LogicalLink(source.operatorIdentifier, PortIdentity(), loopStart.operatorIdentifier, PortIdentity()),
      LogicalLink(loopStart.operatorIdentifier, PortIdentity(), body.operatorIdentifier, PortIdentity()),
      LogicalLink(body.operatorIdentifier, PortIdentity(), loopEnd.operatorIdentifier, PortIdentity())
    )
    (buildWorkflow(ops, links, materializedContext()), loopEnd)
  }

  private def demoFilesWorkflow(): (Workflow, LoopEndOpDesc) = {
    val source = new PythonUDFSourceOpDescV2()
    source.workers = 1
    source.columns = List(attr("filename", AttributeType.STRING), attr("text", AttributeType.STRING))
    source.code =
      """from pytexera import *
        |import pandas as pd
        |
        |class GenerateOperator(UDFSourceOperator):
        |    @overrides
        |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        |        files = [
        |            {"filename": "a.txt", "text": "the quick brown fox"},
        |            {"filename": "b.txt", "text": "lorem ipsum dolor sit amet consectetur"},
        |            {"filename": "c.txt", "text": "hello world"},
        |            {"filename": "d.txt", "text": "one two three four five six"},
        |        ]
        |        yield pd.DataFrame(files)
        |""".stripMargin

    val loopStart = new LoopStartOpDesc()
    loopStart.initialization = "i = 0"
    loopStart.output = "table.iloc[[i]]"

    val body = new PythonUDFOpDescV2()
    body.workers = 1
    body.retainInputColumns = true
    body.outputColumns = List(attr("word_count", AttributeType.INTEGER))
    body.code =
      """from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    @overrides
        |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        |        tuple_["word_count"] = len(str(tuple_["text"]).split())
        |        yield tuple_
        |""".stripMargin

    val loopEnd = new LoopEndOpDesc()
    loopEnd.update = "i += 1"
    loopEnd.condition = "i < len(table)"

    val ops = List(source, loopStart, body, loopEnd)
    val links = List(
      LogicalLink(source.operatorIdentifier, PortIdentity(), loopStart.operatorIdentifier, PortIdentity()),
      LogicalLink(loopStart.operatorIdentifier, PortIdentity(), body.operatorIdentifier, PortIdentity()),
      LogicalLink(body.operatorIdentifier, PortIdentity(), loopEnd.operatorIdentifier, PortIdentity())
    )
    (buildWorkflow(ops, links, materializedContext()), loopEnd)
  }

  "LoopOverheadExperiment" should "run the K-means choose-K demo" in {
    assume(sys.env.getOrElse("LOOP_EXPERIMENT_MODE", "knn") == "demo_kmeans")
    cleanupWorkflowExecutionData(specId)
    setUpWorkflowExecutionData(specId)
    try {
      val (wf, loopEnd) = demoKmeansWorkflow()
      val (secs, rows) = runAndReadRows(wf, List(loopEnd))
      val out = rows(loopEnd.operatorIdentifier.id)
      println(f"LOOP_EXPERIMENT_DEMO_KMEANS seconds=$secs%.3f nrows=${out.size}")
      out.foreach(r => println(s"  ROW $r"))
      assert(out.size == 5) // K = 2,3,4,5,6
    } finally cleanupWorkflowExecutionData(specId)
  }

  "LoopOverheadExperiment" should "run the per-file counts demo" in {
    assume(sys.env.getOrElse("LOOP_EXPERIMENT_MODE", "knn") == "demo_files")
    cleanupWorkflowExecutionData(specId)
    setUpWorkflowExecutionData(specId)
    try {
      val (wf, loopEnd) = demoFilesWorkflow()
      val (secs, rows) = runAndReadRows(wf, List(loopEnd))
      val out = rows(loopEnd.operatorIdentifier.id)
      println(f"LOOP_EXPERIMENT_DEMO_FILES seconds=$secs%.3f nrows=${out.size}")
      out.foreach(r => println(s"  ROW $r"))
      assert(out.size == 4) // one row per file
    } finally cleanupWorkflowExecutionData(specId)
  }
}
