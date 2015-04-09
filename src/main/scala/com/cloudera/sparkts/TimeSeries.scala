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

class TimeSeries[K](val index: DateTimeIndex, val data: DenseMatrix[Double], val labels: Array[K])
  extends Serializable {

  def observations(): Array[Array[Double]] = {
//    Util.transpose(data)
    throw new UnsupportedOperationException()
  }

  def differences(windowSize: Int): TimeSeries[K] = {
//    new TimeSeries(index.drop(windowSize), data.map { hist =>
//      hist.sliding(windowSize).map(window => window.last - window.head).toArray
//    })
    throw new UnsupportedOperationException()
  }

  def differences(): TimeSeries[K] = differences(1)

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

  def univariateKeyAndSeriesIterator(): Iterator[(K, Vector[Double])] = {
    new Iterator[(K, Vector[Double])] {
      var i = 0
      def hasNext: Boolean = i < data.cols
      def next(): (K, Vector[Double]) = {
        i += 1
        (labels(i - 1), data(::, i - 1))
      }
    }
  }

  def mapSeries[U](f: (Vector[Double]) => U): Seq[U] = {
    throw new UnsupportedOperationException()
  }
}

object TimeSeries {
  def timeSeriesFromSamples[K](samples: Seq[(DateTime, Array[Double])], labels: Array[K])
    : TimeSeries[K] = {
    val mat = new DenseMatrix[Double](samples.length, samples.head._2.length)
    val dts = new Array[Long](samples.length)
    for (i <- 0 until samples.length) {
      val (dt, values) = samples(i)
      dts(i) = dt.getMillis
      mat(i to i, ::) := new DenseVector[Double](values)
    }
    new TimeSeries[K](new IrregularDateTimeIndex(dts), mat, labels)
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
