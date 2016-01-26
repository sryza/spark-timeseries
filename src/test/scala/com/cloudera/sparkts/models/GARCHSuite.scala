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

import com.cloudera.sparkts.MatrixUtil.toBreeze

import org.apache.spark.mllib.linalg._
import org.apache.commons.math3.random.MersenneTwister
import org.scalatest.FunSuite

class GARCHSuite extends FunSuite {
  test("GARCH log likelihood") {
    val model = new GARCHModel(.2, .3, .4)
    val rand = new MersenneTwister(5L)
    val n  = 10000

    val ts = new DenseVector(model.sample(n, rand))
    val logLikelihoodWithRightModel = model.logLikelihood(ts)

    val logLikelihoodWithWrongModel1 = new GARCHModel(.3, .4, .5).logLikelihood(ts)
    val logLikelihoodWithWrongModel2 = new GARCHModel(.25, .35, .45).logLikelihood(ts)
    val logLikelihoodWithWrongModel3 = new GARCHModel(.1, .2, .3).logLikelihood(ts)

    assert(logLikelihoodWithRightModel > logLikelihoodWithWrongModel1)
    assert(logLikelihoodWithRightModel > logLikelihoodWithWrongModel2)
    assert(logLikelihoodWithRightModel > logLikelihoodWithWrongModel3)
    assert(logLikelihoodWithWrongModel2 > logLikelihoodWithWrongModel1)
  }

  test("gradient") {
    val alpha = 0.3
    val beta = 0.4
    val omega = 0.2
    val genModel = new GARCHModel(omega, alpha, beta)
    val rand = new MersenneTwister(5L)
    val n = 10000

    val ts = new DenseVector(genModel.sample(n, rand))

    val gradient1 = new GARCHModel(omega + .1, alpha + .05, beta + .1).gradient(ts)
    assert(gradient1.forall(_ < 0.0))
    val gradient2 = new GARCHModel(omega - .1, alpha - .05, beta - .1).gradient(ts)
    assert(gradient2.forall(_ > 0.0))
  }

  test("fit model") {
    val omega = 0.2
    val alpha = 0.3
    val beta = 0.5
    val genModel = new ARGARCHModel(0.0, 0.0, alpha, beta, omega)
    val rand = new MersenneTwister(5L)
    val n = 10000

    val ts = new DenseVector(genModel.sample(n, rand))

    val model = GARCH.fitModel(ts)
    assert(model.omega - omega < .1) // TODO: we should be able to be more accurate
    assert(model.alpha - alpha < .02)
    assert(model.beta - beta < .02)
  }

  test("fit model 2") {
    val arr = Array[Double](0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,
      0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,
      -0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,
      -0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,
      -0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,
      0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,
      0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,
      0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,
      -0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,
      0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,
      -0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,
      -0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,
      -0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,
      0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,
      0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,
      -0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,
      -0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1)
    val ts = new DenseVector(arr)
    val model = ARGARCH.fitModel(ts)
    println(s"alpha: ${model.alpha}")
    println(s"beta: ${model.beta}")
    println(s"omega: ${model.omega}")
    println(s"c: ${model.c}")
    println(s"phi: ${model.phi}")
  }

  test("standardize and filter") {
    val model = new ARGARCHModel(40.0, .4, .2, .3, .4)
    val rand = new MersenneTwister(5L)
    val n  = 10000

    val ts = new DenseVector(model.sample(n, rand))

    // de-heteroskedasticize
    val standardized = model.removeTimeDependentEffects(ts, Vectors.zeros(n))
    // heteroskedasticize
    val filtered = model.addTimeDependentEffects(standardized, Vectors.zeros(n))

    assert((toBreeze(filtered) - toBreeze(ts)).toArray.forall(math.abs(_) < .001))
  }
}
