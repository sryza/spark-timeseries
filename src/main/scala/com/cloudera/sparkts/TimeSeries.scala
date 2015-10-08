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

import com.github.nscala_time.time.Imports._
import breeze.linalg.{diff, DenseMatrix => BDM, DenseVector => BDV}
import org.apache.spark.mllib.linalg.{DenseMatrix, Vector}

import MatrixUtil._

import scala.reflect.ClassTag

class TimeSeries[K](val index: DateTimeIndex, val data: DenseMatrix,
    val keys: Array[K])(implicit val kClassTag: ClassTag[K])
  extends Serializable {

  private def dataToBreeze: BDM[Double] = data

  /**
   * IMPORTANT: currently this assumes that the DateTimeIndex is a UniformDateTimeIndex, not an
   * Irregular one. This means that this function won't work (yet) on TimeSeries built using
   * timeSeriesFromIrregularSamples().
   *
   * Lags all individual time series of the TimeSeries instance by up to maxLag amount.
   *
   * Example input TimeSeries:
   *   time 	a 	b
   *   4 pm 	1 	6
   *   5 pm 	2 	7
   *   6 pm 	3 	8
   *   7 pm 	4 	9
   *   8 pm 	5 	10
   *
   * With maxLag 2 and includeOriginals = true, we would get:
   *   time 	a 	lag1(a) 	lag2(a)  b 	lag1(b)  lag2(b)
   *   6 pm 	3 	2 	      1         8 	7 	      6
   *   7 pm 	4 	3 	      2         9 	8 	      7
   *   8 pm   5 	4 	      3         10	9 	      8
   *
   */
  def lags(maxLag: Int, includeOriginals: Boolean)
          (implicit laggedKey: (K, Int) => K)
  : TimeSeries[K] = {
    val numCols = maxLag * keys.length + (if (includeOriginals) keys.length else 0)
    val numRows = data.numRows - maxLag

    val dataBreeze = dataToBreeze
    val laggedDataBreeze = new BDM[Double](numRows, numCols)
    (0 until data.numCols).foreach { colIndex =>
      val offset = maxLag + (if (includeOriginals) 1 else 0)
      val start = colIndex * offset

      Lag.lagMatTrimBoth(dataBreeze(::, colIndex), laggedDataBreeze(::, start to (start + offset - 1)),
        maxLag, includeOriginals)
    }

    val newKeys = keys.indices.map { keyIndex =>
      val key = keys(keyIndex)
      val lagKeys = (1 to maxLag).map(lagOrder => laggedKey(key, lagOrder)).toArray

      if (includeOriginals) Array(key) ++ lagKeys else lagKeys
    }.reduce((prev: Array[K], next: Array[K]) => prev ++ next)

    val newDatetimeIndex = index.islice(maxLag, dataBreeze.rows)

    new TimeSeries[K](newDatetimeIndex, laggedDataBreeze, newKeys)
  }

  def slice(range: Range): TimeSeries[K] = {
    new TimeSeries[K](index.islice(range), dataToBreeze(range, ::), keys)
  }

  def union(vec: Vector, key: K): TimeSeries[K] = {
    val mat = BDM.zeros[Double](data.rows, data.cols + 1)
    (0 until data.cols).foreach(c => mat(::, c to c) := dataToBreeze(::, c to c))
    mat(::, -1 to -1) := toBreeze(vec)
    new TimeSeries[K](index, mat, keys :+ key)
  }

  /**
   * Returns a TimeSeries where each time series is differenced with the given order. The new
   * TimeSeries will be missing the first n date-times.
   */
  def differences(lag: Int): TimeSeries[K] = {
    mapSeries(index.islice(lag, index.size), vec => diff(toBreeze(vec).toDenseVector, lag))
  }

  /**
   * Returns a TimeSeries where each time series is differenced with order 1. The new TimeSeries
   * will be missing the first date-time.
   */
  def differences(): TimeSeries[K] = differences(1)

  /**
   * Returns a TimeSeries where each time series is quotiented with the given order. The new
   * TimeSeries will be missing the first n date-times.
   */
  def quotients(lag: Int): TimeSeries[K] = {
    mapSeries(index.islice(lag, index.size), vec => UnivariateTimeSeries.quotients(vec, lag))
  }

  /**
   * Returns a TimeSeries where each time series is quotiented with order 1. The new TimeSeries will
   * be missing the first date-time.
   */
  def quotients(): TimeSeries[K] = quotients(1)

  /**
   * Returns a return series for each time series. Assumes periodic (as opposed to continuously
   * compounded) returns.
   */
  def price2ret(): TimeSeries[K] = {
    mapSeries(index.islice(1, index.size), vec => UnivariateTimeSeries.price2ret(vec, 1))
  }

  def univariateSeriesIterator(): Iterator[Vector] = {
    new Iterator[Vector] {
      var i = 0
      def hasNext: Boolean = i < data.cols
      def next(): Vector = {
        i += 1
        dataToBreeze(::, i - 1)
      }
    }
  }

  def univariateKeyAndSeriesIterator(): Iterator[(K, Vector)] = {
    new Iterator[(K, Vector)] {
      var i = 0
      def hasNext: Boolean = i < data.cols
      def next(): (K, Vector) = {
        i += 1
        (keys(i - 1), dataToBreeze(::, i - 1))
      }
    }
  }

  /**
   * Applies a transformation to each series that preserves the time index.
   */
  def mapSeries(f: (Vector) => Vector): TimeSeries[K] = {
    mapSeries(index, f)
  }

  /**
   * Applies a transformation to each series that preserves the time index. Passes the key along
   * with each series.
   */
  def mapSeriesWithKey(f: (K, Vector) => Vector): TimeSeries[K] = {
    val newData = new BDM[Double](index.size, data.cols)
    univariateKeyAndSeriesIterator().zipWithIndex.foreach { case ((key, series), i) =>
      newData(::, i) := toBreeze(f(key, series))
    }
    new TimeSeries[K](index, newData, keys)
  }

  /**
   * Applies a transformation to each series such that the resulting series align with the given
   * time index.
   */
  def mapSeries(newIndex: DateTimeIndex, f: (Vector) => Vector): TimeSeries[K] = {
    val newSize = newIndex.size
    val newData = new BDM[Double](newSize, data.cols)
    univariateSeriesIterator().zipWithIndex.foreach { case (vec, i) =>
      newData(::, i) := toBreeze(f(vec))
    }
    new TimeSeries[K](newIndex, newData, keys)
  }

  def mapValues[U](f: (Vector) => U): Seq[(K, U)] = {
    univariateKeyAndSeriesIterator().map(ks => (ks._1, f(ks._2))).toSeq
  }

  /**
   * Gets the first univariate series and its key.
   */
  def head(): (K, Vector) = univariateKeyAndSeriesIterator().next()
}

object TimeSeries {
  implicit def laggedStringKey(key: String, lagOrder: Int): String = s"lag${lagOrder.toString}($key)"

  def timeSeriesFromIrregularSamples[K](samples: Seq[(DateTime, Array[Double])], keys: Array[K])
     (implicit kClassTag: ClassTag[K]): TimeSeries[K] = {
    val mat = new BDM[Double](samples.length, samples.head._2.length)
    val dts = new Array[Long](samples.length)
    for (i <- samples.indices) {
      val (dt, values) = samples(i)
      dts(i) = dt.getMillis
      mat(i to i, ::) := new BDV[Double](values)
    }
    new TimeSeries[K](new IrregularDateTimeIndex(dts), mat, keys)
  }

  /**
   * This function should only be called when you can safely make the assumption that the time
   * samples are uniform (monotonously increasing) across time.
   */
  def timeSeriesFromUniformSamples[K](
      samples: Seq[Array[Double]],
      index: UniformDateTimeIndex,
      keys: Array[K])
     (implicit kClassTag: ClassTag[K]): TimeSeries[K] = {
    val mat = new BDM[Double](samples.length, samples.head.length)

    for (i <- samples.indices) {
      mat(i to i, ::) := new BDV[Double](samples(i))
    }
    new TimeSeries[K](index, mat, keys)
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
