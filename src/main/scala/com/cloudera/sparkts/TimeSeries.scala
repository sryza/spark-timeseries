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

package com.cloudera.sparkts

import breeze.linalg._

import com.github.nscala_time.time.Imports._

class TimeSeries(val index: DateTimeIndex, val data: DenseMatrix[Double],
    val labels: Array[String]) extends Serializable {

  def slice(range: Range): TimeSeries = {
    new TimeSeries(index.slice(range), data(range, ::), labels)
  }

  def union(vec: Vector[Double], key: String): TimeSeries = {
    val mat = DenseMatrix.zeros[Double](data.rows, data.cols + 1)
    (0 until data.cols).foreach(c => mat(::, c to c) := data(::, c to c))
    mat(::, -1 to -1) := vec
    new TimeSeries(index, mat, labels :+ key)
  }

  /**
   * Returns a TimeSeries where each time series is differenced with the given order. The new
   * TimeSeries will be missing the first n date-times.
   */
  def differences(lag: Int): TimeSeries = {
    mapSeries(index.slice(lag, index.size - 1), vec => diff(vec.toDenseVector, lag))
  }

  /**
   * Returns a TimeSeries where each time series is differenced with order 1. The new TimeSeries
   * will be missing the first date-time.
   */
  def differences(): TimeSeries = differences(1)

  /**
   * Returns a TimeSeries where each time series is quotiented with the given order. The new
   * TimeSeries will be missing the first n date-times.
   */
  def quotients(lag: Int): TimeSeries = {
    mapSeries(index.slice(lag, index.size - 1), vec => UnivariateTimeSeries.quotients(vec, lag))
  }

  /**
   * Returns a TimeSeries where each time series is quotiented with order 1. The new TimeSeries will
   * be missing the first date-time.
   */
  def quotients(): TimeSeries = quotients(1)

  /**
   * Returns a return series for each time series. Assumes periodic (as opposed to continuously
   * compounded) returns.
   */
  def price2ret(): TimeSeries = {
    mapSeries(index.slice(1, index.size - 1), vec => UnivariateTimeSeries.price2ret(vec, 1))
  }

  def univariateSeriesIterator(): Iterator[Vector[Double]] = {
    new Iterator[Vector[Double]] {
      var i = 0
      def hasNext: Boolean = i < data.cols
      def next(): Vector[Double] = {
        i += 1
        data(::, i - 1)
      }
    }
  }

  def univariateKeyAndSeriesIterator(): Iterator[(String, Vector[Double])] = {
    new Iterator[(String, Vector[Double])] {
      var i = 0
      def hasNext: Boolean = i < data.cols
      def next(): (String, Vector[Double]) = {
        i += 1
        (labels(i - 1), data(::, i - 1))
      }
    }
  }

  /**
   * Applies a transformation to each series that preserves the time index.
   */
  def mapSeries(f: (Vector[Double]) => Vector[Double]): TimeSeries = {
    mapSeries(index, f)
  }

  /**
   * Applies a transformation to each series that preserves the time index. Passes the label along
   * with each series.
   */
  def mapSeriesWithLabel(f: (String, Vector[Double]) => Vector[Double]): TimeSeries = {
    val newData = new DenseMatrix[Double](index.size, data.cols)
    univariateKeyAndSeriesIterator().zipWithIndex.foreach { case ((key, series), index) =>
      newData(::, index) := f(key, series)
    }
    new TimeSeries(index, newData, labels)
  }

  /**
   * Applies a transformation to each series such that the resulting series align with the given
   * time index.
   */
  def mapSeries(newIndex: DateTimeIndex, f: (Vector[Double]) => Vector[Double]): TimeSeries = {
    val newSize = newIndex.size
    val newData = new DenseMatrix[Double](newSize, data.cols)
    univariateSeriesIterator().zipWithIndex.foreach { case (vec, index) =>
      newData(::, index) := f(vec)
    }
    new TimeSeries(newIndex, newData, labels)
  }

  def mapValues[U](f: (Vector[Double]) => U): Seq[(String, U)] = {
    univariateKeyAndSeriesIterator().map(ks => (ks._1, f(ks._2))).toSeq
  }

  /**
   * Gets the first univariate series and its label.
   */
  def head(): (String, Vector[Double]) = univariateKeyAndSeriesIterator().next
}

object TimeSeries {
  def timeSeriesFromSamples(samples: Seq[(DateTime, Array[Double])], labels: Array[String])
    : TimeSeries = {
    val mat = new DenseMatrix[Double](samples.length, samples.head._2.length)
    val dts = new Array[Long](samples.length)
    for (i <- 0 until samples.length) {
      val (dt, values) = samples(i)
      dts(i) = dt.getMillis
      mat(i to i, ::) := new DenseVector[Double](values)
    }
    new TimeSeries(new IrregularDateTimeIndex(dts), mat, labels)
  }
}

trait TimeSeriesFilter extends Serializable {
  /**
   * Takes a time series of i.i.d. observations and filters it to take on this model's
   * characteristics.
   * @param ts Time series of i.i.d. observations.
   * @param dest Array to put the filtered time series, can be the same as ts.
   * @return the dest param.
   */
  def filter(ts: Array[Double], dest: Array[Double]): Array[Double]
}
