/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.engine.architecture.controller.execution

import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.worker.statistics.{
  PortTupleMetricsMapping,
  TupleMetrics
}
import org.apache.texera.amber.engine.common.executionruntimestate.{
  OperatorMetrics,
  OperatorStatistics
}
import org.scalatest.flatspec.AnyFlatSpec

class ExecutionUtilsSpec extends AnyFlatSpec {

  // Sentinel labels used as the generic T for ExecutionUtils.aggregateStates.
  private val Completed = "completed"
  private val Terminated = "terminated"
  private val Running = "running"
  private val Uninitialized = "uninitialized"
  private val Paused = "paused"
  private val Ready = "ready"

  private def aggregate(states: String*): WorkflowAggregatedState =
    ExecutionUtils.aggregateStates(
      states,
      Completed,
      Terminated,
      Running,
      Uninitialized,
      Paused,
      Ready
    )

  "ExecutionUtils.aggregateStates" should "return UNINITIALIZED for an empty input" in {
    assert(aggregate() == WorkflowAggregatedState.UNINITIALIZED)
  }

  it should "return COMPLETED when every state is the completed sentinel" in {
    assert(aggregate(Completed, Completed) == WorkflowAggregatedState.COMPLETED)
  }

  it should "return COMPLETED when every state is the terminated sentinel" in {
    assert(aggregate(Terminated, Terminated) == WorkflowAggregatedState.COMPLETED)
  }

  it should "return RUNNING when any state is the running sentinel" in {
    assert(aggregate(Completed, Running, Paused) == WorkflowAggregatedState.RUNNING)
  }

  it should "return UNINITIALIZED when remaining non-completed states are all uninitialized" in {
    assert(
      aggregate(Completed, Uninitialized, Uninitialized) ==
        WorkflowAggregatedState.UNINITIALIZED
    )
  }

  it should "return PAUSED when remaining non-completed states are all paused" in {
    assert(aggregate(Completed, Paused, Paused) == WorkflowAggregatedState.PAUSED)
  }

  it should "return RUNNING when remaining non-completed states are all ready" in {
    // Note: an all-ready aggregate maps to RUNNING by current contract.
    assert(aggregate(Completed, Ready, Ready) == WorkflowAggregatedState.RUNNING)
  }

  it should "return UNKNOWN when remaining non-completed states are mixed" in {
    assert(aggregate(Completed, Paused, Ready) == WorkflowAggregatedState.UNKNOWN)
  }

  // -- aggregatePortMetrics -----------------------------------------------

  "ExecutionUtils.aggregatePortMetrics" should "return empty when given no mappings" in {
    assert(ExecutionUtils.aggregatePortMetrics(Iterable.empty).isEmpty)
  }

  it should "preserve a single mapping" in {
    val mapping = PortTupleMetricsMapping(PortIdentity(0), TupleMetrics(3, 30))
    assert(ExecutionUtils.aggregatePortMetrics(List(mapping)) == Seq(mapping))
  }

  it should "sum count and size across mappings on the same port" in {
    val portId = PortIdentity(0)
    val a = PortTupleMetricsMapping(portId, TupleMetrics(3, 30))
    val b = PortTupleMetricsMapping(portId, TupleMetrics(5, 50))
    val result = ExecutionUtils.aggregatePortMetrics(List(a, b))
    assert(result == Seq(PortTupleMetricsMapping(portId, TupleMetrics(8, 80))))
  }

  it should "group mappings by port id when ports differ" in {
    val a = PortTupleMetricsMapping(PortIdentity(0), TupleMetrics(1, 10))
    val b = PortTupleMetricsMapping(PortIdentity(1), TupleMetrics(2, 20))
    val result = ExecutionUtils.aggregatePortMetrics(List(a, b)).toSet
    assert(result == Set(a, b))
  }

  // -- aggregateMetrics ---------------------------------------------------

  private def metricsWith(
      state: WorkflowAggregatedState,
      input: Seq[PortTupleMetricsMapping] = Seq.empty,
      output: Seq[PortTupleMetricsMapping] = Seq.empty,
      numWorkers: Int = 0,
      dataTime: Long = 0,
      controlTime: Long = 0,
      idleTime: Long = 0
  ): OperatorMetrics =
    OperatorMetrics(
      state,
      OperatorStatistics(input, output, numWorkers, dataTime, controlTime, idleTime)
    )

  "ExecutionUtils.aggregateMetrics" should "return UNINITIALIZED defaults when given no metrics" in {
    val result = ExecutionUtils.aggregateMetrics(Iterable.empty)
    assert(result.operatorState == WorkflowAggregatedState.UNINITIALIZED)
    assert(result.operatorStatistics.inputMetrics.isEmpty)
    assert(result.operatorStatistics.outputMetrics.isEmpty)
    assert(result.operatorStatistics.numWorkers == 0)
    assert(result.operatorStatistics.dataProcessingTime == 0)
    assert(result.operatorStatistics.controlProcessingTime == 0)
    assert(result.operatorStatistics.idleTime == 0)
  }

  it should "sum scalar statistics and merge per-port metrics across operators" in {
    val portIn = PortIdentity(0)
    val portOut = PortIdentity(0)
    val left = metricsWith(
      WorkflowAggregatedState.RUNNING,
      input = Seq(PortTupleMetricsMapping(portIn, TupleMetrics(2, 20))),
      output = Seq(PortTupleMetricsMapping(portOut, TupleMetrics(1, 10))),
      numWorkers = 1,
      dataTime = 100,
      controlTime = 5,
      idleTime = 1
    )
    val right = metricsWith(
      WorkflowAggregatedState.RUNNING,
      input = Seq(PortTupleMetricsMapping(portIn, TupleMetrics(3, 30))),
      output = Seq(PortTupleMetricsMapping(portOut, TupleMetrics(4, 40))),
      numWorkers = 2,
      dataTime = 200,
      controlTime = 10,
      idleTime = 2
    )

    val result = ExecutionUtils.aggregateMetrics(List(left, right))

    assert(result.operatorState == WorkflowAggregatedState.RUNNING)
    assert(
      result.operatorStatistics.inputMetrics ==
        Seq(PortTupleMetricsMapping(portIn, TupleMetrics(5, 50)))
    )
    assert(
      result.operatorStatistics.outputMetrics ==
        Seq(PortTupleMetricsMapping(portOut, TupleMetrics(5, 50)))
    )
    assert(result.operatorStatistics.numWorkers == 3)
    assert(result.operatorStatistics.dataProcessingTime == 300)
    assert(result.operatorStatistics.controlProcessingTime == 15)
    assert(result.operatorStatistics.idleTime == 3)
  }

  it should "filter out internal ports when aggregating port metrics" in {
    val publicPort = PortIdentity(0)
    val internalPort = PortIdentity(1, internal = true)
    val metrics = metricsWith(
      WorkflowAggregatedState.RUNNING,
      input = Seq(
        PortTupleMetricsMapping(publicPort, TupleMetrics(1, 10)),
        PortTupleMetricsMapping(internalPort, TupleMetrics(99, 990))
      ),
      output = Seq(PortTupleMetricsMapping(internalPort, TupleMetrics(7, 70)))
    )

    val result = ExecutionUtils.aggregateMetrics(List(metrics))

    assert(
      result.operatorStatistics.inputMetrics ==
        Seq(PortTupleMetricsMapping(publicPort, TupleMetrics(1, 10)))
    )
    assert(result.operatorStatistics.outputMetrics.isEmpty)
  }
}
