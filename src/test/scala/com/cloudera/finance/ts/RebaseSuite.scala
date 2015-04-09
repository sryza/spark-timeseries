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

package com.cloudera.finance.ts

import scala.Double.NaN

import breeze.linalg._

import com.cloudera.finance.ts.DateTimeIndex._

import com.github.nscala_time.time.Imports._

import org.scalatest.{FunSuite, ShouldMatchers}

class RebaseSuite extends FunSuite with ShouldMatchers {
  test("uniform source same range") {
    val vec = new DenseVector((0 until 10).map(_.toDouble).toArray)
    val source = uniform(new DateTime("2015-4-8"), vec.length, 1.days)
    val target = source
    val rebased = UnivariateTimeSeries.rebase(source, target, vec)
    rebased.length should be (vec.length)
    rebased should be (vec)
  }

  test("uniform source, target fits in source") {
    val vec = new DenseVector((0 until 10).map(_.toDouble).toArray)
    val source = uniform(new DateTime("2015-4-8"), vec.length, 1.days)
    val target = uniform(new DateTime("2015-4-9"), 5, 1.days)
    val rebased = UnivariateTimeSeries.rebase(source, target, vec)
    rebased should be (new DenseVector(Array(1.0, 2.0, 3.0, 4.0, 5.0)))
  }

  test("uniform source, target overlaps source ") {
    val vec = new DenseVector((0 until 10).map(_.toDouble).toArray)
    val source = uniform(new DateTime("2015-4-8"), vec.length, 1.days)
    val targetBefore = uniform(new DateTime("2015-4-4"), 8, 1.days)
    val targetAfter = uniform(new DateTime("2015-4-11"), 8, 1.days)
    val rebasedBefore = UnivariateTimeSeries.rebase(source, targetBefore, vec)
    val rebasedAfter = UnivariateTimeSeries.rebase(source, targetAfter, vec)
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
    val rebased = UnivariateTimeSeries.rebase(source, target, vec)
    assertArraysEqualWithNaN(
      rebased.valuesIterator.toArray,
      Array(NaN, 0.0, 1.0, 2.0, 3.0, NaN, NaN, NaN))
  }

  test("irregular source same range") {
    val vec = new DenseVector((0 to 10).map(_.toDouble).toArray)

  }

  test("irregular source, target fits in source") {

  }

  test("irregular source, target overlaps source ") {

  }

  test("irregular source, source fits in target") {

  }

  test("different frequencies") {

  }

  private def assertArraysEqualWithNaN(arr1: Array[Double], arr2: Array[Double]): Unit = {
    assert(arr1.zip(arr2).forall { case (d1, d2) =>
      d1 == d2 || (d1.isNaN && d2.isNaN)
    })
  }
}
