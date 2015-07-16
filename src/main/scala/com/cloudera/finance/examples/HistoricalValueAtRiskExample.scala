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

package com.cloudera.finance.examples

import breeze.linalg._

import com.cloudera.finance.YahooParser
import com.cloudera.finance.Util._
import com.cloudera.sparkts._
import com.cloudera.sparkts.DateTimeIndex._
import com.cloudera.sparkts.TimeSeriesRDD._

import com.github.nscala_time.time.Imports._

import org.apache.commons.math3.random.{MersenneTwister, RandomGenerator}
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

import org.apache.spark.{SparkConf, SparkContext}

object HistoricalValueAtRiskExample {
  def main(args: Array[String]): Unit = {
    // Read parameters
    val factorsDir = if (args.length > 0) args(0) else "factors/"
    val instrumentsDir = if (args.length > 1) args(1) else "instruments/"
    val numTrials = if (args.length > 2) args(2).toInt else 10000
    val parallelism = if (args.length > 3) args(3).toInt else 10
    val horizon = if (args.length > 4) args(4).toInt else 1

    // Initialize Spark
    val conf = new SparkConf().setMaster("local").setAppName("Historical VaR")
    val sc = new SparkContext(conf)

    // Load the data into a TimeSeriesRDD where each series holds 1-day log returns
    def loadTS(inputDir: String, lower: DateTime, upper: DateTime): TimeSeriesRDD = {
      val histories = YahooParser.yahooFiles(inputDir, sc)
      histories.cache()
      val start = histories.map(_.index.first).takeOrdered(1).head
      val end = histories.map(_.index.last).top(1).head
      val dtIndex = uniform(start, end, 1.businessDays)
      val tsRdd = timeSeriesRDD(dtIndex, histories).
        filter(_._1.endsWith("csvClose")).
        filterStartingBefore(lower).filterEndingAfter(upper)
      tsRdd.fill("linear").
        slice(lower, upper).
        price2ret().
        mapSeries(_.map(x => math.log(1 + x)))
    }

    val year2000 = nextBusinessDay(new DateTime("2008-1-1"))
    val year2010 = nextBusinessDay(year2000 + 5.years)

    val instrumentReturns = loadTS(instrumentsDir, year2000, year2010)
    val factorReturns = loadTS(factorsDir, year2000, year2010).collectAsTimeSeries()

    // Fit an AR(1) + GARCH(1, 1) model to each factor
    val garchModels = factorReturns.mapValues(ARGARCH.fitModel(_)).toMap
    val iidFactorReturns = factorReturns.mapSeriesWithKey { case (symbol, series) =>
      val model = garchModels(symbol)
      model.removeTimeDependentEffects(series, DenseVector.zeros[Double](series.length))
    }

    // Generate an RDD of simulations
    val seeds = sc.parallelize(0 until numTrials, parallelism)
    seeds.map { seed =>
      val rand = new MersenneTwister(seed)
      val factorPaths = simulatedFactorReturns(horizon, rand, iidFactorReturns, garchModels)
    }

//    val factorsDist = new FilteredHistoricalFactorDistribution(rand, iidFactorReturns.toArray,
//      garchModels.asInstanceOf[Array[TimeSeriesFilter]])
//    val returns = simulationReturns(0L, factorsDist, numTrials, parallelism, sc,
//      instrumentReturnsModel)
//    returns.cache()
//
//    // Calculate VaR and expected shortfall
//    val pValues = Array(.01, .03, .05)
//    val valueAtRisks = valueAtRisk(returns, pValues)
//    println(s"Value at risk at ${pValues.mkString(",")}: ${valueAtRisks.mkString(",")}")
//
//    val expectedShortfalls = expectedShortfall(returns, pValues)
//    println(s"Expected shortfall at ${pValues.mkString(",")}: ${expectedShortfalls.mkString(",")}")
  }

  /**
   * Generates paths of factor returns each with the given number of days.
   */
  def simulatedFactorReturns(
      nDays: Int,
      rand: RandomGenerator,
      iidSeries: TimeSeries,
      models: Map[String, TimeSeriesModel]): Matrix[Double] = {
    val mat = DenseMatrix.zeros[Double](nDays, iidSeries.data.cols)
    (0 until nDays).foreach { i =>
      mat(i, ::) := iidSeries.data(rand.nextInt(iidSeries.data.rows), ::)
    }
    (0 until models.size).foreach { i =>
      models(iidSeries.keys(i)).addTimeDependentEffects(mat(::, i), mat(::, i))
    }
    mat
  }

  /**
   * Fits a model for each instrument that predicts its returns based on the returns of the factors.
   */
  def fitInstrumentReturnsModels(instrumentReturns: TimeSeriesRDD, factorReturns: TimeSeries) {
    // Fit factor return -> instrument return predictive models
    val linearModels = instrumentReturns.mapValues { series =>
      val withLag = factorReturns.slice(2 until series.length).
        union(series(1 until series.length - 1), "instrlag1").
        union(series(0 until series.length - 2), "instrlag2")

      // Get factors in a form that we can feed into Commons Math linear regression
      val factorObservations = matToRowArrs(withLag.data)
      // Run the regression
      val regression = new OLSMultipleLinearRegression()
      regression.newSampleData(series.toArray, factorObservations)
      regression.estimateRegressionParameters()
    }.map(_._2).collect()
    arrsToMat(linearModels.iterator)
  }
}
