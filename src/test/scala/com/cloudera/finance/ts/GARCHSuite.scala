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

import org.apache.commons.math3.random.MersenneTwister

import org.scalatest.FunSuite

class GARCHSuite extends FunSuite {
  test("GARCH log likelihood") {
    val model = new GARCHModel(.2, .3, .4)
    val rand = new MersenneTwister(5L)
    val n  = 10000

    val ts = model.sample(n, rand)
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

    val ts = genModel.sample(n, rand)

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

    val ts = genModel.sample(n, rand)

    val model = GARCH.fitModel(ts)._1
    assert(model.omega - omega < .02)
    assert(model.alpha - alpha < .02)
    assert(model.beta - beta < .02)
  }

  test("standardize and filter") {
    val model = new ARGARCHModel(40.0, .4, .2, .3, .4)
    val rand = new MersenneTwister(5L)
    val n  = 10000

    val ts = model.sample(n, rand)

    // de-heteroskedasticize
    val standardized = model.removeTimeDependentEffects(ts, new Array[Double](n))
    // heteroskedasticize
    val filtered = model.addTimeDependentEffects(standardized, new Array[Double](n))

    assert(filtered.zip(ts).forall(x => x._1 - x._2 < .001))
  }
}
