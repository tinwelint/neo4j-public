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
package org.neo4j.cypher.internal.compiler.v2_2.planDescription

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{VarLengthExpandPipeForStringTypes, ExpandPipeForStringTypes, SingleRowPipe, PipeMonitor}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.graphdb.Direction

class RenderPlanDescriptionDetailsTest extends CypherFunSuite {

  val pipe = SingleRowPipe()(mock[PipeMonitor])

  test("single node is represented nicely") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n"))

    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-------------+-------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers | Other |
        |+----------+---------------+------+--------+-------------+-------+
        ||     NAME |             1 |   42 |     33 |           n |       |
        |+----------+---------------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("extra identifiers are not a problem") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("a", "b", "c"))

    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-------------+-------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers | Other |
        |+----------+---------------+------+--------+-------------+-------+
        ||     NAME |             1 |   42 |     33 |     a, b, c |       |
        |+----------+---------------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("super many identifiers stretches the column") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("a", "b", "c", "d", "e", "f"))

    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+------------------+-------+
        || Operator | EstimatedRows | Rows | DbHits |      Identifiers | Other |
        |+----------+---------------+------+--------+------------------+-------+
        ||     NAME |             1 |   42 |     33 | a, b, c, d, e, f |       |
        |+----------+---------------+------+--------+------------------+-------+
        |""".stripMargin)
  }

  test("execution plan without profiler stats uses question marks") {
    val arguments = Seq()

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n"))

    renderDetails(plan) should equal(
      """+----------+---------------+-------------+-------+
        || Operator | EstimatedRows | Identifiers | Other |
        |+----------+---------------+-------------+-------+
        ||     NAME |             1 |           n |       |
        |+----------+---------------+-------------+-------+
        |""".stripMargin)
  }

  test("two plans with the same name get unique-ified names") {
    val args1 = Seq(Rows(42), DbHits(33))
    val args2 = Seq(Rows(2), DbHits(633), Index("Label", "Prop"))

    val plan1 = PlanDescriptionImpl(pipe, "NAME", NoChildren, args1, Set("a"))
    val plan2 = PlanDescriptionImpl(pipe, "NAME", SingleChild(plan1), args2, Set("b"))

    renderDetails(plan2) should equal(
      """+----------+---------------+------+--------+-------------+--------------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers |        Other |
        |+----------+---------------+------+--------+-------------+--------------+
        ||  NAME(0) |             1 |    2 |    633 |           b | :Label(Prop) |
        ||  NAME(1) |             1 |   42 |     33 |           a |              |
        |+----------+---------------+------+--------+-------------+--------------+
        |""".stripMargin)
  }


  test("Expand contains information about its relations") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))
    val expandPipe = ExpandPipeForStringTypes(pipe, "from", "rel", "to", Direction.INCOMING, Seq.empty)(Some(1L))(mock[PipeMonitor])

    renderDetails(expandPipe.planDescription) should equal(
      """+----------+---------------+-------------+---------------------+
        || Operator | EstimatedRows | Identifiers |               Other |
        |+----------+---------------+-------------+---------------------+
        ||   Expand |             1 |     rel, to | (from)<-[:rel]-(to) |
        |+----------+---------------+-------------+---------------------+
        |""".stripMargin)
  }

  test("Var length expand contains information about its relations") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))
    val expandPipe = VarLengthExpandPipeForStringTypes(pipe, "from", "rel", "to", Direction.INCOMING, Direction.OUTGOING, Seq.empty, 0, None)(Some(1L))(mock[PipeMonitor])

    renderDetails(expandPipe.planDescription) should equal(
      """+-------------------+---------------+-------------+----------------------+
        ||          Operator | EstimatedRows | Identifiers |                Other |
        |+-------------------+---------------+-------------+----------------------+
        || Var length expand |             1 |     rel, to | (from)-[:rel*]->(to) |
        |+-------------------+---------------+-------------+----------------------+
        |""".stripMargin)
  }

  test("do not show unnamed identifiers") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      ExpandExpression("  UNNAMED123", "R", "  UNNAMED24", Direction.OUTGOING)
    )

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n", "  UNNAMED123", "  UNNAMED2", "  UNNAMED24"))
    renderDetails(plan) should equal(
      """|+----------+---------------+------+--------+-------------+-------------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers |       Other |
        |+----------+---------------+------+--------+-------------+-------------+
        ||     NAME |             1 |   42 |     33 |           n | ()-[:R]->() |
        |+----------+---------------+------+--------+-------------+-------------+
        |""".stripMargin)
  }
}
