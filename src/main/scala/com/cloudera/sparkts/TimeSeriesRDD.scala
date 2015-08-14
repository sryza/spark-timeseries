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

import scala.collection.mutable.ArrayBuffer

import breeze.linalg._

import org.apache.spark.SparkContext._
import org.apache.spark.{Partition, Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import org.joda.time.DateTime

/**
 * A lazy distributed collection of univariate series with a conformed time dimension. Lazy in the
 * sense that it is an RDD: it encapsulates all the information needed to generate its elements,
 * but doesn't materialize them upon instantiation. Distributed in the sense that different
 * univariate series within the collection can be stored and processed on different nodes. Within
 * each univariate series, observations are not distributed. The time dimension is conformed in the
 * sense that a single DateTimeIndex applies to all the univariate series. Each univariate series
 * within the RDD has a String key to identify it.
 *
 * @param index The DateTimeIndex shared by all the time series.
 */
class TimeSeriesRDD(val index: DateTimeIndex, parent: RDD[(String, Vector[Double])])
  extends RDD[(String, Vector[Double])](parent) {

  /**
   * Collects the RDD as a local TimeSeries
   */
  def collectAsTimeSeries(): TimeSeries = {
    val elements = collect()
    if (elements.isEmpty) {
      new TimeSeries(index, new DenseMatrix[Double](0, 0), new Array[String](0))
    } else {
      val mat = new DenseMatrix[Double](elements.head._2.length, elements.length)
      val labels = new Array[String](elements.length)
      for (i <- 0 until elements.length) {
        val (label, vec) = elements(i)
        mat(::, i) := vec
        labels(i) = label
      }
      new TimeSeries(index, mat, labels)
    }
  }

  /**
   * Returns a TimeSeriesRDD where each time series is differenced with the given order. The new
   * RDD will be missing the first n date-times.
   */
  def differences(n: Int): TimeSeriesRDD = {
    mapSeries(index.slice(n, index.size - 1), vec => diff(vec.toDenseVector, n))
  }

  /**
   * Returns a TimeSeriesRDD where each time series is quotiented with the given order. The new
   * RDD will be missing the first n date-times.
   */
  def quotients(n: Int): TimeSeriesRDD = {
    mapSeries(index.slice(n, index.size - 1), UnivariateTimeSeries.quotients(_, n))
  }

  /**
   * Returns a return series for each time series. Assumes periodic (as opposed to continuously
   * compounded) returns.
   */
  def price2ret(): TimeSeriesRDD = {
    mapSeries(index.slice(1, index.size - 1), vec => UnivariateTimeSeries.price2ret(vec, 1))
  }

  /**
   * {@inheritDoc}
   */
  override def filter(f: ((String, Vector[Double])) => Boolean): TimeSeriesRDD = {
    new TimeSeriesRDD(index, super.filter(f))
  }

  /**
   * Keep only time series whose first observation is before or equal to the given start date.
   */
  def filterStartingBefore(dt: DateTime): TimeSeriesRDD = {
    val startLoc = index.locAtDateTime(dt)
    filter { case (key, ts) => UnivariateTimeSeries.firstNotNaN(ts) <= startLoc }
  }

  /**
   * Keep only time series whose last observation is after or equal to the given end date.
   */
  def filterEndingAfter(dt: DateTime): TimeSeriesRDD = {
    val endLoc = index.locAtDateTime(dt)
    filter { case (key, ts) => UnivariateTimeSeries.lastNotNaN(ts) >= endLoc}
  }

  /**
   * Returns a TimeSeriesRDD that's a sub-slice of the given series.
   * @param start The start date the for slice.
   * @param end The end date for the slice (inclusive).
   */
  def slice(start: DateTime, end: DateTime): TimeSeriesRDD = {
    val targetIndex = index.slice(start, end)
    new TimeSeriesRDD(targetIndex,
      mapSeries(TimeSeriesUtils.rebase(index, targetIndex, _, Double.NaN)))
  }

  /**
   * Takes the first N series in the TimeSeriesRDD and returns a new, reduced, TimeSeriesRDD.
   * Works by creating an array of series to include and then checks for membership.
   * @param n the number of series to take (first n taken) n >= 0
   * @return reduced TimeSeriesRDD
   */
  def takeNSeries(n: Int): TimeSeriesRDD = {
    require(n >= 0, "n cannot be negative")
    val includeSeries = this.keys.take(n)
    this.filter(x => includeSeries.contains(x._1))
  }

  /**
   * Drops the first N series in the TimeSeriesRDD and returns a new, reduced, TimeSeriesRDD.
   * Works by creating an array of series to exclude and then checks for membership.
   * @param n the number of series to drop (first n dropped) n >= 0
   * @return reduced TimeSeriesRDD
   */
  def dropNSeries(n: Int): TimeSeriesRDD = {
    require(n >= 0, "n cannot be negative")
    val excludeSeries = this.keys.take(n)
    this.filter(x => !excludeSeries.contains(x._1))
  }

  /**
   * Fills in missing data (NaNs) in each series according to a given imputation method.
   *
   * @param method "linear", "nearest", "next", or "previous"
   * @return A TimeSeriesRDD with missing observations filled in.
   */
  def fill(method: String): TimeSeriesRDD = {
    mapSeries(UnivariateTimeSeries.fillts(_, method))
  }

  /**
   * Applies a transformation to each time series that preserves the time index of this
   * TimeSeriesRDD.
   */
  def mapSeries[U](f: (Vector[Double]) => Vector[Double]): TimeSeriesRDD = {
    new TimeSeriesRDD(index, map(kt => (kt._1, f(kt._2))))
  }

  /**
   * Applies a transformation to each time series and returns a TimeSeriesRDD with the given index.
   * The caller is expected to ensure that the time series produced line up with the given index.
   */
  def mapSeries[U](index: DateTimeIndex, f: (Vector[Double]) => Vector[Double])
    : TimeSeriesRDD = {
    new TimeSeriesRDD(index, map(kt => (kt._1, f(kt._2))))
  }

  /**
   * Gets stats like min, max, mean, and standard deviation for each time series.
   */
  def seriesStats(): RDD[StatCounter] = {
    map(kt => new StatCounter(kt._2.valuesIterator))
  }

  /**
   * Essentially transposes the time series matrix to create an RDD where each record contains a
   * single instant in time and all the values that correspond to it. Involves a shuffle operation.
   *
   * In the returned RDD, the ordering of values within each record corresponds to the ordering of
   * the time series records in the original RDD. The records are ordered by time.
   */
  def toInstants(nPartitions: Int = -1): RDD[(DateTime, Vector[Double])] = {
    val maxChunkSize = 20

    val dividedOnMapSide = mapPartitionsWithIndex { case (partitionId, iter) =>
      new Iterator[((Int, Int), Vector[Double])] {
        // Each chunk is a buffer of time series
        var chunk = new ArrayBuffer[Vector[Double]]()
        // Current date time.  Gets reset for every chunk.
        var dtLoc: Int = _
        var chunkId: Int = -1

        override def hasNext: Boolean = iter.hasNext || dtLoc < index.size
        override def next(): ((Int, Int), Vector[Double]) = {
          if (chunkId == -1 || dtLoc == index.size) {
            chunk.clear()
            while (chunk.size < maxChunkSize && iter.hasNext) {
              chunk += iter.next._2
            }
            dtLoc = 0
            chunkId += 1
          }

          val arr = new Array[Double](chunk.size)
          var i = 0
          while (i < chunk.size) {
            arr(i) = chunk(i)(dtLoc)
            i += 1
          }
          dtLoc += 1
          ((dtLoc - 1, partitionId * maxChunkSize + chunkId), new DenseVector(arr))
        }
      }
    }

    // At this point, dividedOnMapSide is an RDD of snippets of full samples that will be
    // assembled on the reduce side.  Each key is a tuple of
    // (date-time, position of snippet in full sample)

    val partitioner = new Partitioner() {
      val nPart = if (nPartitions == -1) parent.partitions.size else nPartitions
      override def numPartitions: Int = nPart
      override def getPartition(key: Any): Int = key.asInstanceOf[(Int, _)]._1 / nPart
    }
    implicit val ordering = new Ordering[(Int, Int)] {
      override def compare(a: (Int, Int), b: (Int, Int)): Int = {
        val dtDiff = a._1 - b._1
        if (dtDiff != 0){
          dtDiff
        } else {
          a._2 - b._2
        }
      }
    }
    val repartitioned = dividedOnMapSide.repartitionAndSortWithinPartitions(partitioner)
    repartitioned.mapPartitions { iter0: Iterator[((Int, Int), Vector[Double])] =>
      new Iterator[(DateTime, Vector[Double])] {
        var snipsPerSample = -1
        var elementsPerSample = -1
        var iter: Iterator[((Int, Int), Vector[Double])] = _

        // Read the first sample specially so that we know the number of elements and snippets
        // for succeeding samples.
        def firstSample(): ArrayBuffer[((Int, Int), Vector[Double])] = {
          var snip = iter0.next()
          val snippets = new ArrayBuffer[((Int, Int), Vector[Double])]()
          val firstDtLoc = snip._1._1

          while (snip != null && snip._1._1 == firstDtLoc) {
            snippets += snip
            snip = if (iter0.hasNext) iter0.next() else null
          }
          iter = if (snip == null) iter0 else Iterator(snip) ++ iter0
          snippets
        }

        def assembleSnips(snips: Iterator[((Int, Int), Vector[Double])])
          : (DateTime, Vector[Double]) = {
          val resVec = DenseVector.zeros[Double](elementsPerSample)
          var dtLoc = -1
          var i = 0
          for (j <- 0 until snipsPerSample) {
            val ((loc, _), snipVec) = snips.next()
            dtLoc = loc
            resVec(i until i + snipVec.length) := snipVec
            i += snipVec.length
          }
          (index.dateTimeAtLoc(dtLoc), resVec)
        }

        override def hasNext: Boolean = {
          if (iter == null) {
            iter0.hasNext
          } else {
            iter.hasNext
          }
        }

        override def next(): (DateTime, Vector[Double]) = {
          if (snipsPerSample == -1) {
            val firstSnips = firstSample()
            snipsPerSample = firstSnips.length
            elementsPerSample = firstSnips.map(_._2.length).sum
            assembleSnips(firstSnips.toIterator)
          } else {
            assembleSnips(iter)
          }
        }
      }
    }
  }

  def compute(split: Partition, context: TaskContext): Iterator[(String, Vector[Double])] = {
    parent.iterator(split, context)
  }

  protected def getPartitions: Array[Partition] = parent.partitions
}

object TimeSeriesRDD {
  /**
   * Instantiates a TimeSeriesRDD.
   *
   * @param targetIndex DateTimeIndex to conform all the indices to.
   * @param seriesRDD RDD of time series, each with their own DateTimeIndex.
   */
  def timeSeriesRDD(targetIndex: UniformDateTimeIndex,
      seriesRDD: RDD[(String, UniformDateTimeIndex, Vector[Double])]): TimeSeriesRDD = {
    val rdd = seriesRDD.map { case (key, index, vec) =>
      val newVec = TimeSeriesUtils.rebase(index, targetIndex, vec, Double.NaN)
      (key, newVec)
    }
    new TimeSeriesRDD(targetIndex, rdd)
  }

  /**
   * Instantiates a TimeSeriesRDD.
   *
   * @param targetIndex DateTimeIndex to conform all the indices to.
   * @param seriesRDD RDD of time series, each with their own DateTimeIndex.
   */
  def timeSeriesRDD(targetIndex: DateTimeIndex, seriesRDD: RDD[TimeSeries]): TimeSeriesRDD = {
    val rdd = seriesRDD.flatMap { series =>
      series.univariateKeyAndSeriesIterator().map { case (key, vec) =>
        (key, TimeSeriesUtils.rebase(series.index, targetIndex, vec, Double.NaN))
      }
    }
    new TimeSeriesRDD(targetIndex, rdd)
  }
}
