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

import java.util.Random

import com.cloudera.sparkts.MatrixUtil.toBreeze

import org.apache.spark.mllib.linalg._
import org.apache.commons.math3.random.MersenneTwister
import org.scalatest.FunSuite

class AutoregressionSuite extends FunSuite {
  test("fit AR(1) model") {
    val model = new ARModel(1.5, Array(.2))
    val ts = model.sample(5000, new MersenneTwister(10L))
    val fittedModel = Autoregression.fitModel(ts, 1)
    assert(fittedModel.coefficients.length == 1)
    assert(math.abs(fittedModel.c - 1.5) < .07)
    assert(math.abs(fittedModel.coefficients(0) - .2) < .03)
  }

  test("fit AR(2) model") {
    val model = new ARModel(1.5, Array(.2, .3))
    val ts = model.sample(5000, new MersenneTwister(10L))
    val fittedModel = Autoregression.fitModel(ts, 2)
    assert(fittedModel.coefficients.length == 2)
    assert(math.abs(fittedModel.c - 1.5) < .15)
    assert(math.abs(fittedModel.coefficients(0) - .2) < .03)
    assert(math.abs(fittedModel.coefficients(1) - .3) < .03)
  }

  test("add and remove time dependent effects") {
    val rand = new Random()
    val ts = new DenseVector(Array.fill(1000)(rand.nextDouble()))
    val model = new ARModel(1.5, Array(.2, .3))
    val added = model.addTimeDependentEffects(ts, Vectors.zeros(ts.size))
    val removed = model.removeTimeDependentEffects(added, Vectors.zeros(ts.size))
    assert((toBreeze(ts) - toBreeze(removed)).toArray.forall(math.abs(_) < .001))
  }
}
