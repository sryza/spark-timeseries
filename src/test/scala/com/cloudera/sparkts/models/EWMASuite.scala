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

package com.cloudera.sparkts.models

import org.apache.spark.mllib.linalg._
import org.scalatest.{FunSuite, ShouldMatchers}

class EWMASuite extends FunSuite with ShouldMatchers {
  test("adding time dependent effects") {
    val orig = new DenseVector((1 to 10).toArray.map(_.toDouble))

    val m1 = new EWMAModel(0.2)
    val smoothed1 = new DenseVector(Array.fill(10)(0.0))
    m1.addTimeDependentEffects(orig, smoothed1)

    smoothed1(0) should be (orig(0))
    smoothed1(1) should be (m1.smoothing * orig(1) + (1 - m1.smoothing) * smoothed1(0))
    round2Dec(smoothed1.toArray.last) should be (6.54)

    val m2 = new EWMAModel(0.6)
    val smoothed2 = new DenseVector(Array.fill(10)(0.0))
    m2.addTimeDependentEffects(orig, smoothed2)

    smoothed2(0) should be (orig(0))
    smoothed2(1) should be (m2.smoothing * orig(1) + (1 - m2.smoothing) * smoothed2(0))
    round2Dec(smoothed2.toArray.last) should be (9.33)
  }

  test("removing time dependent effects") {
    val smoothed = new DenseVector(Array(1.0, 1.2, 1.56, 2.05, 2.64, 3.31, 4.05, 4.84, 5.67, 6.54))

    val m1 = new EWMAModel(0.2)
    val orig1 = new DenseVector(Array.fill(10)(0.0))
    m1.removeTimeDependentEffects(smoothed, orig1)

    round2Dec(orig1(0)) should be (1.0)
    orig1.toArray.last.toInt should be(10)
  }

  test("fitting EWMA model") {
    // We reproduce the example in ch 7.1 from
    // https://www.otexts.org/fpp/7/1
    val oil = Array(446.7, 454.5, 455.7, 423.6, 456.3, 440.6, 425.3, 485.1, 506.0, 526.8,
      514.3, 494.2)
    val model =  EWMA.fitModel(new DenseVector(oil))
    val truncatedSmoothing = (model.smoothing * 100.0).toInt
    truncatedSmoothing should be (89) // approximately 0.89
  }

  private def round2Dec(x: Double): Double = {
    (x * 100).round / 100.00
  }
}
