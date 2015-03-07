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

import org.apache.commons.math3.distribution.{AbstractMultivariateRealDistribution,
  ChiSquaredDistribution}
import org.apache.commons.math3.random.RandomGenerator

/**
 * @param v The number of degrees of freedom in the distribution.
 * @param mu The mean of the distribution.
 * @param sigma The covariance matrix of the distribution.
 */
class MultivariateTDistribution(
    v: Int,
    mu: Array[Double],
    sigma: Array[Array[Double]],
    rand: RandomGenerator)
  extends AbstractMultivariateRealDistribution(rand, mu.length) {

  val normal =
    new SerializableMultivariateNormalDistribution(rand, new Array[Double](mu.length), sigma)
  val chiSquared = new ChiSquaredDistribution(v)
  reseedRandomGenerator(rand.nextLong())

  override def sample(): Array[Double] = {
    val sample = normal.sample()
    val sqrtW = math.sqrt(chiSquared.sample())

    var i = 0
    while (i < sample.length) {
      sample(i) = mu(i) + sample(i) * sqrtW
      i += 1
    }
    sample
  }

  override def density(p1: Array[Double]): Double = throw new UnsupportedOperationException

  override def reseedRandomGenerator(seed: Long): Unit = {
    normal.reseedRandomGenerator(seed)
    chiSquared.reseedRandomGenerator(seed + 1)
  }
}
