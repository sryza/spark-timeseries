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

import org.apache.commons.math3.random.RandomGenerator

class ValueAtRisk {
  def filteredHistoricalValueAtRisk(
      ts: Array[Double],
      pValues: Array[Double],
      numTrials: Int,
      rand: RandomGenerator): Array[Double] = {
    val model = GARCH.fitModel(ts)

    val samples = new Array[Double](ts.length)
    for (i <- numTrials) {
      sampleWithReplacement(ts, rand, samples)
    }
  }

  def sampleWithReplacement(values: Array[Double], rand: RandomGenerator, target: Array[Double])
    : Unit = {
    for (i <- 0 until target.length) {
      target(i) = values(rand.nextInt(values.length))
    }
  }
}
