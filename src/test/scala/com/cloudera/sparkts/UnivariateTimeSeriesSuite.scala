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

import com.cloudera.sparkts.UnivariateTimeSeries._

import org.apache.commons.math3.random.MersenneTwister

import org.scalatest.{FunSuite, ShouldMatchers}

class UnivariateTimeSeriesSuite extends FunSuite with ShouldMatchers {
  test("lastNotNaN") {
    lastNotNaN(new DenseVector(Array(1.0, 2.0, 3.0, 4.0))) should be (3)
    lastNotNaN(new DenseVector(Array(1.0, 2.0, NaN, 4.0))) should be (3)
    lastNotNaN(new DenseVector(Array(1.0, 2.0, 3.0, NaN))) should be (2)
  }

  test("autocorr") {
    val rand = new MersenneTwister(5L)
    val iidAutocorr = autocorr(Array.fill(10000)(rand.nextDouble * 5.0), 3)
    iidAutocorr.foreach(math.abs(_) should be < .03)

    val arModel = new ARModel(1.5, Array(.2))
    val arSeries = arModel.sample(10000, rand)
    val arAutocorr = autocorr(arSeries, 3)
    math.abs(.2 - arAutocorr(0)) should be < 0.02
    arAutocorr(1) should be < 0.06
    arAutocorr(2) should be < 0.06
    arAutocorr(1) should be > 0.0
    arAutocorr(2) should be > 0.0
  }
}
