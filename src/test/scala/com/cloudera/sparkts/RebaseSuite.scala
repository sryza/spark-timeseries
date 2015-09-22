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

import com.github.nscala_time.time.Imports._

import org.scalatest.{FunSuite, ShouldMatchers}

class RebaseSuite extends FunSuite with ShouldMatchers {
  test("iterateWithUniformFrequency single value") {
    val baseDT = new DateTime("2015-4-8")
    val dts = Array(baseDT)
    val values = Array(1.0)
    val iter = iterateWithUniformFrequency(dts.zip(values).iterator, 1.days, 47.0)
    iter.toArray should be (Array((baseDT, 1.0)))
  }

  test("iterateWithUniformFrequency no gaps") {
    val baseDT = new DateTime("2015-4-8")
    val dts = Array(baseDT, baseDT + 1.days, baseDT + 2.days, baseDT + 3.days)
    val values = Array(1.0, 2.0, 3.0, 4.0)
    val iter = iterateWithUniformFrequency(dts.zip(values).iterator, 1.days)
    iter.toArray should be (Array((baseDT, 1.0), (baseDT + 1.days, 2.0), (baseDT + 2.days, 3.0),
      (baseDT + 3.days, 4.0)))
  }

  test("iterateWithUniformFrequency multiple gaps") {
    val baseDT = new DateTime("2015-4-8")
    val dts = Array(baseDT, baseDT + 2.days, baseDT + 5.days)
    val values = Array(1.0, 2.0, 3.0)
    val iter = iterateWithUniformFrequency(dts.zip(values).iterator, 1.days, 47.0)
    iter.toArray should be (Array((baseDT, 1.0), (baseDT + 1.days, 47.0), (baseDT + 2.days, 2.0),
      (baseDT + 3.days, 47.0), (baseDT + 4.days, 47.0), (baseDT + 5.days, 3.0)))
  }

  test("uniform source same range") {
    val vec = new DenseVector((0 until 10).map(_.toDouble).toArray)
    val source = uniform(new DateTime("2015-4-8"), vec.length, 1.days)
    val target = source
    val rebased = rebase(source, target, vec, NaN)
    rebased.length should be (vec.length)
    rebased should be (vec)
  }

  test("uniform source, target fits in source") {
    val vec = new DenseVector((0 until 10).map(_.toDouble).toArray)
    val source = uniform(new DateTime("2015-4-8"), vec.length, 1.days)
    val target = uniform(new DateTime("2015-4-9"), 5, 1.days)
    val rebased = rebase(source, target, vec, NaN)
    rebased should be (new DenseVector(Array(1.0, 2.0, 3.0, 4.0, 5.0)))
  }

  test("uniform source, target overlaps source ") {
    val vec = new DenseVector((0 until 10).map(_.toDouble).toArray)
    val source = uniform(new DateTime("2015-4-8"), vec.length, 1.days)
    val targetBefore = uniform(new DateTime("2015-4-4"), 8, 1.days)
    val targetAfter = uniform(new DateTime("2015-4-11"), 8, 1.days)
    val rebasedBefore = rebase(source, targetBefore, vec, NaN)
    val rebasedAfter = rebase(source, targetAfter, vec, NaN)
    assertArraysEqualWithNaN(
      rebasedBefore.valuesIterator.toArray,
      Array(NaN, NaN, NaN, NaN, 0.0, 1.0, 2.0, 3.0))
    assertArraysEqualWithNaN(
      rebasedAfter.valuesIterator.toArray,
      Array(3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, NaN))
  }

  test("uniform source, source fits in target") {
    val vec = new DenseVector((0 until 4).map(_.toDouble).toArray)
    val source = uniform(new DateTime("2015-4-8"), vec.length, 1.days)
    val target = uniform(new DateTime("2015-4-7"), 8, 1.days)
    val rebased = rebase(source, target, vec, NaN)
    assertArraysEqualWithNaN(
      rebased.valuesIterator.toArray,
      Array(NaN, 0.0, 1.0, 2.0, 3.0, NaN, NaN, NaN))
  }

  test("irregular source same range") {
    val vec = new DenseVector((4 until 10).map(_.toDouble).toArray)
    val source = irregular((4 until 10).map(d => new DateTime(s"2015-4-$d")).toArray)
    vec.size should be (source.size)
    val target = uniform(new DateTime("2015-4-4"), vec.length, 1.days)
    val rebased = rebase(source, target, vec, NaN)
    rebased should be (vec)
  }

  test("irregular source, hole gets filled default value") {
    val dt = new DateTime("2015-4-10")
    val source = irregular(Array(dt, dt + 1.days, dt + 3.days))
    val target = uniform(dt, 4, 1.days)
    val vec = new DenseVector(Array(1.0, 2.0, 3.0))
    val rebased = rebase(source, target, vec, 47.0)
    rebased.toArray should be (Array(1.0, 2.0, 47.0, 3.0))
  }

  test("irregular source, target fits in source") {
    val dt = new DateTime("2015-4-10")
    val source = irregular(Array(dt, dt + 1.days, dt + 3.days))
    val target = uniform(dt + 1.days, 2, 1.days)
    val vec = new DenseVector(Array(1.0, 2.0, 3.0))
    val rebased = rebase(source, target, vec, 47.0)
    rebased.toArray should be (Array(2.0, 47.0))
  }

  test("irregular source, target overlaps source ") {
    val dt = new DateTime("2015-4-10")
    val source = irregular(Array(dt, dt + 1.days, dt + 3.days))
    val targetBefore = uniform(new DateTime("2015-4-8"), 4, 1.days)
    val vec = new DenseVector(Array(1.0, 2.0, 3.0))
    val rebasedBefore = rebase(source, targetBefore, vec, 47.0)
    rebasedBefore.toArray should be (Array(47.0, 47.0, 1.0, 2.0))
    val targetAfter = uniform(new DateTime("2015-4-11"), 5, 1.days)
    val rebasedAfter = rebase(source, targetAfter, vec, 47.0)
    rebasedAfter.toArray should be (Array(2.0, 47.0, 3.0, 47.0, 47.0))
  }

  test("irregular source, source fits in target") {
    val dt = new DateTime("2015-4-10")
    val source = irregular(Array(dt, dt + 1.days, dt + 3.days))
    val target = uniform(dt - 2.days, 7, 1.days)
    val vec = new DenseVector(Array(1.0, 2.0, 3.0))
    val rebased = rebase(source, target, vec, 47.0)
    rebased.toArray should be (Array(47.0, 47.0, 1.0, 2.0, 47.0, 3.0, 47.0))
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
      val sourceIndex = irregular(source.map(x => new DateTime(s"2015-04-0$x")))
      val targetIndex = irregular(target.map(x => new DateTime(s"2015-04-0$x")))
      val vec = new DenseVector[Double](source.map(_.toDouble))
      val expectedVec = new DenseVector[Double](expected.map(_.toDouble))
      rebase(sourceIndex, targetIndex, vec, -1) should be (expectedVec)
    }
  }

  private def assertArraysEqualWithNaN(arr1: Array[Double], arr2: Array[Double]): Unit = {
    assert(arr1.zip(arr2).forall { case (d1, d2) =>
      d1 == d2 || (d1.isNaN && d2.isNaN)
    }, s"${arr1.mkString(",")} != ${arr2.mkString(",")}")
  }
}
