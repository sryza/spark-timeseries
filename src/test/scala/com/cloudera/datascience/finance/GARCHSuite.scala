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

package com.cloudera.datascience.finance

import java.util.Random

import org.scalatest.FunSuite

class GARCHSuite extends FunSuite {
  test("fit AR(1) + GARCH(1, 1) model") {
    val alpha = .3
    val beta = .4
    val omega = .2
//    val a = .5

    val n = 10000
    // generate time series
    val rand = new Random(1000)
    val ts = new Array[Double](n)
    val stddevs = new Array[Double](n)
    stddevs(0) = 1.0
    var eta = 0.0
    for (i <- 1 until n) {
      stddevs(i) = math.sqrt(omega + beta * stddevs(i-1) * stddevs(i-1) + alpha * eta * eta)
      eta = stddevs(i) * rand.nextGaussian()
      ts(i) = eta
    }

    // fit model
    /*
    val (model, likelihood) = GARCH.fitModel(ts)
    println(s"alpha: ${model.alpha}")
    println(s"beta: ${model.beta}")
    println(s"omega: ${model.omega}")
    println(s"likelihood: $likelihood")
    */
    val model = new GARCHModel(0.0, 0.0, alpha, beta, omega)

    // de-heteroskedasticize
    val standardized = model.standardize(ts)
//    println(stddevs.take(20).mkString(", "))
//    println(modelVariances.map(math.sqrt(_)).take(20).mkString(", "))

    // heteroskedasticize
    val standardizedCopy = new Array[Double](standardized.length)
    System.arraycopy(standardized, 0, standardizedCopy, 0, standardized.length)
    model.filter(standardized)
    val filtered = standardized

    assert(filtered.zip(standardizedCopy).forall(x => x._1 - x._2 < .001))
  }
}
