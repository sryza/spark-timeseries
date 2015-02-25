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

import breeze.linalg.DenseMatrix


import org.apache.commons.math3.distribution.MultivariateRealDistribution
import org.apache.commons.math3.random.RandomGenerator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ValueAtRisk {
  /**
   *
   * @param seed
   * @param factorDistribution Multivariate distribution used to generate factor returns for each
   *                           trial.
   * @param numTrials The number of simulations to run.
   * @param parallelism The maximum number of concurrent computations to run.
   * @param sc The SparkContext to create the RDD against.

   * @return
   */
  def simulationReturns(
      seed: Long,
      factorDistribution: MultivariateRealDistribution with Serializable,
      numTrials: Int,
      parallelism: Int,
      sc: SparkContext,
      returnsModel: InstrumentReturnsModel): RDD[Double] = {
    val seeds = (seed until seed + parallelism)
    val seedRdd = sc.parallelize(seeds, parallelism)
    val trialsPerPartition = numTrials / parallelism
      val factorReturnsRdd = seedRdd.flatMap { seed =>
      factorDistribution.reseedRandomGenerator(seed)
      val samples = (0 until trialsPerPartition).iterator.map(i => factorDistribution.sample())
      val features = samples.map(returnsModel.features)
      Util.vecArrsToMats(features, 1024)
    }

    factorReturnsRdd.flatMap { factorReturns =>
      val chunkTrialReturns = returnsModel.totalReturns(factorReturns)
      chunkTrialReturns.valuesIterator
    }
  }

  /**
   * @param pValues Confidence levels to find the VaR at.
   * @return Value at risk at each of the given p values.
   */
  def valueAtRisk(simulationReturns: RDD[Double], pValues: Array[Double]): Array[Double] = {

  }

  /**
   * Find the expected shortfall (CVaR) at the given p values.
   *
   * @param pValues Confidence levels to find the expected shortfall at.
   * @return Expected shortfall at each of the given p values.
   */
  def expectedShortfall(simulationReturns: RDD[Double], pValues: Array[Double]): Array[Double] = {

  }
}

/**
 * Model that predicts the returns of a set of instruments given the returns of a set of market
 * factors.
 */
abstract class InstrumentReturnsModel extends Serializable {
  /**
   * Given a set of simulations, each containing a set of factor returns, computes the sum of all
   * instrument returns for each simulation.
   * @param simulations Matrix where rows are simulations, cols are factors, and elements are
   *                    returns.
   * @return Vector of simulation returns.
   */
  def totalReturns(simulations: DenseMatrix[Double]): DenseMatrix[Double]

  /**
   * Optional function for transforming the factor returns into a set of features for the model.
   */
  def features(returns: Array[Double]): Array[Double] = returns
}

/**
 * Model parameterized with a vector of factor weights w for each instrument and, given factor
 * returns r, predicts the instrument's return as w T * r.
 *@param instrumentFactorWeights Matrix where rows are instruments and cols are factors.
 */
class LinearInstrumentReturnsModel(
    instrumentFactorWeights: DenseMatrix[Double]) extends InstrumentReturnsModel {
  val numInstruments = instrumentFactorWeights.rows
  val ones = new DenseMatrix[Double](numInstruments, 1, Array.fill[Double](numInstruments)(1.0))

  // Column vector where each element is the sum of all instrument-factor weights for a factor
  val factorTotalInstrumentWeights = instrumentFactorWeights.t * ones

  def totalReturns(simulations: DenseMatrix[Double]): DenseMatrix[Double] = {
    simulations * factorTotalInstrumentWeights
  }
}
