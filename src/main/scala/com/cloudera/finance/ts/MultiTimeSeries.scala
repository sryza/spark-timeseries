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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import org.joda.time.DateTime

class MultiTimeSeries[K](
    val index: DateTimeIndex,
    val rdd: RDD[(K, Array[Double])]) {

  def sliceSeries(start: DateTime, end: DateTime): MultiTimeSeries[K] = {
    throw new UnsupportedOperationException()
  }

  def sliceSeries(start: Int, end: Int): MultiTimeSeries[K] = {
    throw new UnsupportedOperationException()
  }

  def unionSeries(other: MultiTimeSeries[K]): MultiTimeSeries[K] = {
    // TODO: allow unioning series with different indices
    // they need to have the same period though
//    val unionRdd = rdd.join(other.rdd).mapValues { tt =>
//      UnivariateTimeSeries.union(Array(tt._1, tt._2))
//    }
//    new MultiTimeSeries(index, unionRdd)
    throw new UnsupportedOperationException()
  }

  def mapSeries[U](f: (K, Array[Double]) => U): RDD[(K, U)] = {
    rdd.map(kt => (kt._1, f(kt._1, kt._2)))
  }

  def foldLeftSeries[U](zero: U)(f: ((U, K, Double)) => U): RDD[(K, U)] = {
    mapSeries((k, t) => t.foldLeft(zero)((u, v) => f(u, k, v)))
  }

  def seriesStats(): RDD[StatCounter] = {
    rdd.map(kt => new StatCounter(kt._2))
  }

  def seriesMinMaxDates(): RDD[(K, (DateTime, DateTime))] = {
//    rdd.mapValues(series => UnivariateTimeSeries.minMaxDateTimes(index, series))
    throw new UnsupportedOperationException()
  }

  def toSamples(): TimeSamples = {
    throw new UnsupportedOperationException()
  }

}

class TimeSamples {

}
