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

import com.cloudera.finance.YahooParser
import com.cloudera.sparkts.DateTimeIndex._
import com.cloudera.sparkts.TimeSeries
import com.cloudera.sparkts.UnivariateTimeSeries._
import com.cloudera.sparkts.TimeSeriesRDD._
import com.cloudera.sparkts.TimeSeriesStatisticalTests._

import com.github.nscala_time.time.Imports._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SimpleTickDataExample {
  def main(args: Array[String]): Unit = {
    val inputDir = args(0)

    val conf = new SparkConf().setMaster("local")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val seriesByFile: RDD[TimeSeries] = YahooParser.yahooFiles(inputDir, sc)
    seriesByFile.cache()

    // Merge the series from individual files into a TimeSeriesRDD
    val start = seriesByFile.map(_.index.first).takeOrdered(1).head
    val end = seriesByFile.map(_.index.last).top(1).head
    val dtIndex = uniform(start, end, 1.businessDays)
    val tsRdd = timeSeriesRDD(dtIndex, seriesByFile)
    println(s"Num time series: ${tsRdd.count()}")

    // Which symbols do we have the oldest data on?
    val oldestData: Array[(Int, String)] = tsRdd.mapValues(trimLeading(_).length).map(_.swap).top(5)
    println(oldestData.mkString(","))

    // Only look at open prices, and filter all symbols that don't span the 2000's
    val opensRdd = tsRdd.filter(_._1.endsWith("Open"))
    val year2000 = nextBusinessDay(new DateTime("2000-1-1"))
    val year2010 = nextBusinessDay(year2000 + 10.years)
    val filteredRdd = opensRdd.filterStartingBefore(year2000).filterEndingAfter(year2010)
    println(s"Remaining after filtration: ${filteredRdd.count()}")

    // Impute missing data with linear interpolation
    val filledRdd = filteredRdd.fill("linear")

    // Slice to the 2000's
    val slicedRdd = filledRdd.slice(year2000, year2010)
    slicedRdd.cache()

    // Find the series with the largest serial correlations
    val durbinWatsonStats: RDD[(String, Double)] = slicedRdd.mapValues(dwtest)
    durbinWatsonStats.map(_.swap).top(20)

    // Remove serial correlations
    val iidRdd = slicedRdd.mapSeries(series => ar(series, 1).removeTimeDependentEffects(series))

    // Regress a stock against all the others
    val samples = iidRdd.toSamples()
  }
}
