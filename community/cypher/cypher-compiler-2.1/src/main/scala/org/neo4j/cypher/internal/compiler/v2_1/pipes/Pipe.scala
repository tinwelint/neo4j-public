/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1._
import symbols._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.ExecutionResult

trait PipeMonitor {
  def startSetup(queryId: AnyRef, pipe: Pipe)
  def stopSetup(queryId: AnyRef, pipe: Pipe)
  def startStep(queryId: AnyRef, pipe: Pipe)
  def stopStep(queryId: AnyRef, pipe: Pipe)
}

object NoopPipeMonitor extends PipeMonitor {
  def startSetup(queryId: AnyRef, pipe: Pipe) {}
  def stopSetup(queryId: AnyRef, pipe: Pipe) {}
  def startStep(queryId: AnyRef, pipe: Pipe) {}
  def stopStep(queryId: AnyRef, pipe: Pipe) {}
}

/**
 * Pipe is a central part of Cypher. Most pipes are decorators - they
 * wrap another pipe. ParamPipe and NullPipe the only exception to this.
 * Pipes are combined to form an execution plan, and when iterated over,
 * the execute the query.
 */
trait Pipe {
  def monitor: PipeMonitor = NoopPipeMonitor

  def createResults(state: QueryState) : Iterator[ExecutionContext] = {
    val decoratedState = state.decorator.decorate(this, state)
    monitor.startSetup(state.queryId, this)
    val that = this
    val innerResult = internalCreateResults(decoratedState)
    val result = new Iterator[ExecutionContext] {
      def hasNext = innerResult.hasNext
      def next() = {
        monitor.startStep(state.queryId, that)
        val value = innerResult.next()
        monitor.stopStep(state.queryId, that)
        value
      }
    }
    monitor.stopSetup(state.queryId, this)
    state.decorator.decorate(this, result)
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext]

  def symbols: SymbolTable

  def executionPlanDescription: PlanDescription

  /**
   * Please make sure to add a test for this implementation @ PipeLazynessTest
   */
  def isLazy: Boolean = true

  def readsFromDatabase: Boolean = true

  def sources: Seq[Pipe] = Seq.empty

  /*
  Runs the predicate on all the inner Pipe until no pipes are left, or one returns true.
   */
  def exists(pred: Pipe => Boolean): Boolean
}

case class NullPipe(symbols: SymbolTable = SymbolTable(),
                    executionPlanDescription:PlanDescription = NullPlanDescription) extends Pipe {
  def internalCreateResults(state: QueryState) =
    Iterator(state.initialContext getOrElse ExecutionContext.empty)

  def exists(pred: Pipe => Boolean) = pred(this)
}

abstract class PipeWithSource(source: Pipe) extends Pipe {
  override def createResults(state: QueryState): Iterator[ExecutionContext] = {
    val sourceResult = source.createResults(state)

    val decoratedState = state.decorator.decorate(this, state)
    val result = internalCreateResults(sourceResult, decoratedState)
    state.decorator.decorate(this, result)
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    throw new ThisShouldNotHappenError("Andres", "This method should never be called on PipeWithSource")

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext]

  override val sources: Seq[Pipe] = Seq(source)

  def exists(pred: Pipe => Boolean) = pred(this) || source.exists(pred)
}
