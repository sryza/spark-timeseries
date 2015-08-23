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

import breeze.linalg._

import org.scalatest.{FunSuite, ShouldMatchers}

class HoltSuite extends FunSuite with ShouldMatchers {

  test("components based on FPP example") {
    // source: https://www.otexts.org/fpp/7/2
    // Componentens
    val orig = Array(17.55, 21.86, 23.89, 26.93, 26.89, 28.83, 30.08, 30.95, 30.19, 31.58, 32.58,
      33.48, 39.02, 41.39, 41.6)

    val levelRef = Array(18.41, 21.89, 24.21, 27.05, 27.57, 29.12, 30.38, 31.28, 30.8, 31.72, 32.68,
      33.57, 38.17, 41.12, 41.92)
    val trendRef = Array(3.62, 3.59, 3.33, 3.24, 2.69, 2.46, 2.22, 1.96, 1.47, 1.36, 1.28, 1.2,
      1.88, 2.1, 1.84)
    val smoothedRef = Array(21.86, 22.03, 25.48, 27.54, 30.29, 30.26, 31.58, 32.6, 33.24, 32.27,
      33.08, 33.96, 34.78, 40.06, 43.22)


    val model = new HoltModel(0.8, 0.2)
    val n = orig.length
    // The first of these corresponds to addTimeDependentEffects
    val (fitted, level, trend) = model.getHoltComponents(new DenseVector(orig))

    for(i <- 0 until n) {
      fitted(i) should be (smoothedRef(i) +- 1e-2)
      level(i) should be (levelRef(i) +- 1e-2)
      trend(i) should be (trendRef(i) +- 1e-2)
    }
  }

  test("forecast based on FPP example") {
    // source: https://www.otexts.org/fpp/7/2
    // Componentens
    val orig = Array(17.55, 21.86, 23.89, 26.93, 26.89, 28.83, 30.08, 30.95, 30.19, 31.58, 32.58,
      33.48, 39.02, 41.39, 41.6)

    val forecastRef = Array(21.86, 22.03, 25.48, 27.54, 30.29, 30.26, 31.58, 32.6, 33.24, 32.27,
      33.08, 33.96, 34.78, 40.06, 43.22, 43.76, 45.59, 47.43, 49.27, 51.10)


    val model = new HoltModel(0.8, 0.2)
    val n = orig.length
    val forecast = model.forecast(
      new DenseVector(orig),
      new DenseVector(Array.fill(forecastRef.length)(0.0)))

    for(i <- 0 until n) {
      forecast(i) should be (forecastRef(i) +- 1e-2)
    }
  }

  test("Fitting with BOBYQA vs CG") {
    // source: https://www.otexts.org/fpp/7/2
    val orig = Array(17.55, 21.86, 23.89, 26.93, 26.89, 28.83, 30.08, 30.95, 30.19, 31.58, 32.58,
      33.48, 39.02, 41.39, 41.6)
    val ts = new DenseVector(orig)

    val mBounded = Holt.fitModel(ts, "BOBYQA")
    val mUnbounded = Holt.fitModel(ts, "CG")

    // values should be fairly similar for this problem
    mBounded.alpha should be (mUnbounded.alpha +- 1e-3)
    mBounded.beta should be (mUnbounded.beta +- 1e-3)
  }
}
