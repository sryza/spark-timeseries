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

import org.apache.commons.math3.random.MersenneTwister

import org.scalatest.FunSuite

class GARCHSuite extends FunSuite {
  test("standardize and filter") {
    val model = new ARGARCHModel(40.0, .4, .2, .3, .4)
    val rand = new MersenneTwister(5L)
    val n  = 10000

    val ts = model.sample(n, rand)

    // de-heteroskedasticize
    val standardized = model.standardize(ts, new Array[Double](n))
    // heteroskedasticize
    val filtered = model.filter(standardized, new Array[Double](n))

    assert(filtered.zip(standardized).forall(x => x._1 - x._2 < .001))
  }
}
