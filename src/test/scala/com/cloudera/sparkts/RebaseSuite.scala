/**
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.sparkts

import scala.Double.NaN

import breeze.linalg._

import com.cloudera.sparkts.DateTimeIndex._
import com.cloudera.sparkts.TimeSeriesUtils._
import java.time._
import org.scalatest.{FunSuite, ShouldMatchers}

class RebaseSuite extends FunSuite with ShouldMatchers {
  private type Rebaser = Vector[Double] => Vector[Double]
  private val Z = ZoneId.of("Z")

  test("iterateWithUniformFrequency single value") {
    val baseDT = ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, Z)
    val dts = Array(baseDT)
    val values = Array(1.0)
    val iter = iterateWithUniformFrequency(dts.zip(values).iterator, new DayFrequency(1), 47.0)
    iter.toArray should be (Array((baseDT, 1.0)))
  }

  test("iterateWithUniformFrequency no gaps") {
    val baseDT = ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, Z)
    val dts = Array(baseDT, baseDT.plusDays(1), baseDT.plusDays(2), baseDT.plusDays(3))
    val values = Array(1.0, 2.0, 3.0, 4.0)
    val iter = iterateWithUniformFrequency(dts.zip(values).iterator, new DayFrequency(1))
    iter.toArray should be (Array((baseDT, 1.0), (baseDT.plusDays(1), 2.0),
      (baseDT.plusDays(2), 3.0), (baseDT.plusDays(3), 4.0)))
  }

  test("iterateWithUniformFrequency multiple gaps") {
    val baseDT = ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, Z)
    val dts = Array(baseDT, baseDT.plusDays(2), baseDT.plusDays(5))
    val values = Array(1.0, 2.0, 3.0)
    val iter = iterateWithUniformFrequency(dts.zip(values).iterator, new DayFrequency(1), 47.0)
    iter.toArray should be (Array((baseDT, 1.0), (baseDT.plusDays(1), 47.0),
      (baseDT.plusDays(2), 2.0), (baseDT.plusDays(3), 47.0), (baseDT.plusDays(4), 47.0),
      (baseDT.plusDays(5), 3.0)))
  }

  test("uniform source same range") {
    val vec = new DenseVector((0 until 10).map(_.toDouble).toArray)
    val source = uniform(ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, Z),
      vec.length,
      1.businessDays)
    val target = source

    assertRebaser(rebaser(source, target, NaN), vec, vec.toArray)
    assertRebaser(rebaserGeneric(source, target, NaN), vec, vec.toArray)
  }

  test("uniform source, target fits in source") {
    val vec = new DenseVector((0 until 10).map(_.toDouble).toArray)
    val source = uniform(ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, Z),
      vec.length,
      1.businessDays)
    val target = uniform(ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, Z),
      5,
      1.businessDays)

    assertRebaser(rebaser(source, target, NaN), vec, Array(1.0, 2.0, 3.0, 4.0, 5.0))
    assertRebaser(rebaserGeneric(source, target, NaN), vec, Array(1.0, 2.0, 3.0, 4.0, 5.0))
  }

  test("uniform source, target overlaps source ") {
    val vec = new DenseVector((0 until 10).map(_.toDouble).toArray)
    val source = uniform(ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, Z),
      vec.length,
      new DayFrequency(1))
    val targetBefore = uniform(ZonedDateTime.of(2015, 4, 4, 0, 0, 0, 0, Z),
      8,
      new DayFrequency(1))
    val targetAfter = uniform(ZonedDateTime.of(2015, 4, 11, 0, 0, 0, 0, Z),
      8,
      new DayFrequency(1))

    assertRebaser(rebaser(source, targetBefore, NaN), vec,
      Array(NaN, NaN, NaN, NaN, 0.0, 1.0, 2.0, 3.0))
    assertRebaser(rebaser(source, targetAfter, NaN), vec,
      Array(3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, NaN))

    assertRebaser(rebaserGeneric(source, targetBefore, NaN), vec,
      Array(NaN, NaN, NaN, NaN, 0.0, 1.0, 2.0, 3.0))
    assertRebaser(rebaserGeneric(source, targetAfter, NaN), vec,
      Array(3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, NaN))
  }

  test("uniform source, source fits in target") {
    val vec = new DenseVector((0 until 4).map(_.toDouble).toArray)
    val source = uniform(ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, Z),
      vec.length,
      1.businessDays)
    val target = uniform(ZonedDateTime.of(2015, 4, 7, 0, 0, 0, 0, Z),
      8,
      1.businessDays)

    assertRebaser(rebaser(source, target, NaN), vec,
      Array(NaN, 0.0, 1.0, 2.0, 3.0, NaN, NaN, NaN))
    assertRebaser(rebaserGeneric(source, target, NaN), vec,
      Array(NaN, 0.0, 1.0, 2.0, 3.0, NaN, NaN, NaN))
  }

  test("irregular & uniform, same range") {
    val vec = new DenseVector((4 until 10).map(_.toDouble).toArray)
    val source = irregular((4 until 10).map(d =>
      ZonedDateTime.of(2015, 4, d, 0, 0, 0, 0, Z)).toArray)
    vec.size should be (source.size)
    val target = uniform(ZonedDateTime.of(2015, 4, 4, 0, 0, 0, 0, Z),
      vec.length,
      new DayFrequency(1))

    assertRebaser(rebaser(source, target, NaN), vec, vec.toArray)
    assertRebaser(rebaserGeneric(source, target, NaN), vec, vec.toArray)

    assertRebaser(rebaser(target, source, NaN), vec, vec.toArray)
    assertRebaser(rebaserGeneric(target, source, NaN), vec, vec.toArray)
  }

  test("irregular source, hole gets filled default value") {
    val dt = ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, Z)
    val source = irregular(Array(dt, dt.plusDays(1), dt.plusDays(3)))
    val target = uniform(dt, 4, new DayFrequency(1))
    val vec = new DenseVector(Array(1.0, 2.0, 3.0))

    assertRebaser(rebaser(source, target, 47.0), vec, Array(1.0, 2.0, 47.0, 3.0))
    assertRebaser(rebaserGeneric(source, target, 47.0), vec, Array(1.0, 2.0, 47.0, 3.0))
  }

  test("irregular source, target fits in source") {
    val dt = ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, Z)
    val source = irregular(Array(dt, dt.plusDays(1), dt.plusDays(3)))
    val target = uniform(dt.plusDays(1), 2, new DayFrequency(1))
    val vec = new DenseVector(Array(1.0, 2.0, 3.0))

    assertRebaser(rebaser(source, target, 47.0), vec, Array(2.0, 47.0))
    assertRebaser(rebaserGeneric(source, target, 47.0), vec, Array(2.0, 47.0))
  }

  test("irregular source, target overlaps source ") {
    val dt = ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, Z)
    val source = irregular(Array(dt, dt.plusDays(1), dt.plusDays(3)))
    val targetBefore = uniform(ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, Z),
      4,
      new DayFrequency(1))
    val vec = new DenseVector(Array(1.0, 2.0, 3.0))

    assertRebaser(rebaser(source, targetBefore, 47.0), vec,
      Array(47.0, 47.0, 1.0, 2.0))
    assertRebaser(rebaserGeneric(source, targetBefore, 47.0), vec,
      Array(47.0, 47.0, 1.0, 2.0))

    val targetAfter = uniform(ZonedDateTime.of(2015, 4, 11, 0, 0, 0, 0, Z),
      5,
      new DayFrequency(1))

    assertRebaser(rebaser(source, targetAfter, 47.0), vec,
      Array(2.0, 47.0, 3.0, 47.0, 47.0))
    assertRebaser(rebaserGeneric(source, targetAfter, 47.0), vec,
      Array(2.0, 47.0, 3.0, 47.0, 47.0))
  }

  test("irregular source, source fits in target") {
    val dt = ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, Z)
    val source = irregular(Array(dt, dt.plusDays(1), dt.plusDays(3)))
    val target = uniform(dt.minusDays(2), 7, new DayFrequency(1))
    val vec = new DenseVector(Array(1.0, 2.0, 3.0))

    assertRebaser(rebaser(source, target, 47.0), vec,
      Array(47.0, 47.0, 1.0, 2.0, 47.0, 3.0, 47.0))
    assertRebaser(rebaserGeneric(source, target, 47.0), vec,
      Array(47.0, 47.0, 1.0, 2.0, 47.0, 3.0, 47.0))
  }

  test("irregular source, irregular target") {
    // Triples of source index, target index, expected output
    // Assumes that at time i, value of source series is i
    val cases = Array(
      (Array(1, 2, 3), Array(1, 2, 3), Array(1, 2, 3)),
      (Array(1, 2, 3), Array(1, 2), Array(1, 2)),
      (Array(1, 2), Array(1, 2, 3), Array(1, 2, -1)),
      (Array(2, 3), Array(1, 2, 3), Array(-1, 2, 3)),
      (Array(1, 2), Array(2, 3), Array(2, -1)),
      (Array(1, 2, 3), Array(1, 3), Array(1, 3)),
      (Array(1, 2, 3, 4), Array(1, 3), Array(1, 3)),
      (Array(1, 2, 3, 4), Array(1, 4), Array(1, 4)),
      (Array(1, 2, 3, 4), Array(2, 4), Array(2, 4)),
      (Array(1, 2, 3, 4), Array(2, 3), Array(2, 3)),
      (Array(1, 2, 3, 4), Array(1, 3, 4), Array(1, 3, 4))
    )

    cases.foreach { case (source, target, expected) =>
      val sourceIndex = irregular(source.
        map(x => ZonedDateTime.of(2015, 4, x, 0, 0, 0, 0, ZoneId.systemDefault())))
      val targetIndex = irregular(target.
        map(x => ZonedDateTime.of(2015, 4, x, 0, 0, 0, 0, ZoneId.systemDefault())))
      val vec = new DenseVector[Double](source.map(_.toDouble))
      val expectedVec = new DenseVector[Double](expected.map(_.toDouble))
      rebase(sourceIndex, targetIndex, vec, -1) should be (expectedVec)
      rebaserGeneric(sourceIndex, targetIndex, -1)(vec) should be (expectedVec)
    }
  }

  private def assertRebaser(
      rebaser: Rebaser,
      vec: Vector[Double],
      expected: Array[Double]): Unit = {
    val rebased = rebaser(vec)
    assertArraysEqualWithNaN(rebased.toArray, expected)
  }

  private def assertArraysEqualWithNaN(arr1: Array[Double], arr2: Array[Double]): Unit = {
    assert(arr1.zip(arr2).forall { case (d1, d2) =>
      d1 == d2 || (d1.isNaN && d2.isNaN)
    }, s"${arr1.mkString(",")} != ${arr2.mkString(",")}")
  }
}
