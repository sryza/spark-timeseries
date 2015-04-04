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

import breeze.linalg.DenseVector

import org.apache.commons.math3.random.MersenneTwister

import org.scalatest.FunSuite

class AugmentedDickeyFullerSuite extends FunSuite {
  test ("adftest with lag 1 and no constant term") {
    val rand = new MersenneTwister(10L)
    val arModel = new ARModel(0.0, 0.3)
    val sample = arModel.sample(500, rand)

    val (adfStat, pValue) = AugmentedDickeyFuller.adftest(new DenseVector(sample), 1)
    assert(!java.lang.Double.isNaN(adfStat))
    assert(!java.lang.Double.isNaN(pValue))
  }
}
