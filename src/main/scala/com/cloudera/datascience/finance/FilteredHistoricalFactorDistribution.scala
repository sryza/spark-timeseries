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

import org.apache.commons.math3.distribution.MultivariateRealDistribution
import org.apache.commons.math3.random.RandomGenerator

class FilteredHistoricalFactorDistribution(
    rand: RandomGenerator,
    iidHistories: Array[Array[Double]],
    filters: Array[TimeSeriesFilter]) extends MultivariateRealDistribution with Serializable {

  def reseedRandomGenerator(seed: Long): Unit = rand.setSeed(seed)

  def density(p1: Array[Double]): Double = throw new UnsupportedOperationException

  def getDimension: Int = iidHistories.length

  def sample(): Array[Double] = {
    val numHistories = iidHistories.length
    val numTimePoints = iidHistories.head.length
    val resampledHistories = Array.ofDim[Double](numHistories, numTimePoints)
    for (i <- 0 until numTimePoints) {
      val timePoint = rand.nextInt(numTimePoints)
      for (j <- 0 until numHistories) {
        resampledHistories(j)(i) = iidHistories(j)(timePoint)
      }
    }

    for (i <- 0 until numHistories) {
      filters(i).filter(resampledHistories(i), resampledHistories(i))
    }

    val timePoint = rand.nextInt(numTimePoints)
    resampledHistories.map(_(timePoint))
  }

  def sample(p1: Int): Array[Array[Double]] = throw new UnsupportedOperationException
}
