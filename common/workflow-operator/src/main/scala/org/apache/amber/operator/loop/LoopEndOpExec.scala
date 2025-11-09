package org.apache.amber.operator.loop

import org.apache.amber.core.executor.OperatorExecutor
import org.apache.amber.core.tuple.{Tuple, TupleLike}

class LoopEndOpExec extends OperatorExecutor {
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator(tuple)
}