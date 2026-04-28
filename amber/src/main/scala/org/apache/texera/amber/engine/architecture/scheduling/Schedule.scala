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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.texera.amber.core.virtualidentity.OperatorIdentity

case class Schedule(
    levelSets: Map[Int, Set[Region]],
    baseLevels: Vector[Int] = Vector.empty,
    executionLevels: Vector[Int] = Vector.empty,
    currentLevelIndex: Int = 0
) extends Iterable[Set[Region]] {
  private val normalizedBaseLevels =
    if (baseLevels.nonEmpty || levelSets.isEmpty) baseLevels else levelSets.keys.toVector.sorted
  private val normalizedExecutionLevels =
    if (executionLevels.nonEmpty || normalizedBaseLevels.isEmpty) executionLevels else normalizedBaseLevels
  private val operatorLevelIndices = levelSets.iterator.flatMap { case (level, regions) =>
    val levelIndex = normalizedBaseLevels.indexOf(level)
    regions.iterator.flatMap(region => region.getOperators.map(_.id.logicalOpId -> levelIndex))
  }.toMap

  val startingLevel: Int = normalizedBaseLevels.headOption.getOrElse(0)

  def getRegions: List[Region] = levelSets.values.flatten.toList

  def getCurrentRegions: Set[Region] =
    normalizedExecutionLevels
      .lift(currentLevelIndex)
      .flatMap(levelSets.get)
      .getOrElse(Set.empty)

  def advance: Schedule =
    copy(
      baseLevels = normalizedBaseLevels,
      executionLevels = normalizedExecutionLevels,
      currentLevelIndex = currentLevelIndex + 1
    )

  def getLevelIndexOfOperator(opId: OperatorIdentity): Option[Int] = operatorLevelIndices.get(opId)

  def rewriteExecutionFrom(levelIndex: Int): Schedule =
    copy(
      baseLevels = normalizedBaseLevels,
      executionLevels = normalizedExecutionLevels.take(currentLevelIndex) ++ normalizedBaseLevels.drop(
        levelIndex
      ),
      currentLevelIndex = currentLevelIndex
    )

  override def iterator: Iterator[Set[Region]] =
    normalizedExecutionLevels.iterator.map(levelSets)
}
