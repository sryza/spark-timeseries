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
 * within the RDD has a key to identify it.
 *
 * @param index The DateTimeIndex shared by all the time series.
 * @tparam K The type of the keys used to identify time series.
 */
class TimeSeriesRDD[K <: AnyRef](val index: DateTimeIndex, parent: RDD[(K, Vector[Double])])
  extends RDD[(K, Vector[Double])](parent) {

  /**
   * Collects the RDD as a local TimeSeries
   */
  def collectAsTimeSeries(): TimeSeries[K] = {
    val elements = collect()
    if (elements.isEmpty) {
      new TimeSeries[K](index, new DenseMatrix[Double](0, 0),
        new Array[AnyRef](0).asInstanceOf[Array[K]])
    } else {
      val mat = new DenseMatrix[Double](elements.head._2.length, elements.length)
      val labels = new Array[AnyRef](elements.length).asInstanceOf[Array[K]]
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
  def difference(n: Int): TimeSeriesRDD[K] = {
    mapSeries(index.slice(n, index.size - 1), vec => diff(vec.toDenseVector, n))
  }

  /**
   * {@inheritDoc}
   */
  override def filter(f: ((K, Vector[Double])) => Boolean): TimeSeriesRDD[K] = {
    new TimeSeriesRDD(index, super.filter(f))
  }

  /**
   * Keep only time series whose first observation is before or equal to the given start date.
   */
  def filterStartingBefore(dt: DateTime): TimeSeriesRDD[K] = {
    val startLoc = index.locAtDateTime(dt)
    filter { case (key, ts) => UnivariateTimeSeries.firstNotNaN(ts) <= startLoc }
  }

  /**
   * Keep only time series whose last observation is after or equal to the given end date.
   */
  def filterEndingAfter(dt: DateTime): TimeSeriesRDD[K] = {
    val endLoc = index.locAtDateTime(dt)
    filter { case (key, ts) => UnivariateTimeSeries.lastNotNaN(ts) >= endLoc}
  }

  /**
   * Returns a TimeSeriesRDD that's a sub-slice of the given series.
   * @param start The start date the for slice.
   * @param end The end date for the slice (inclusive).
   */
  def slice(start: DateTime, end: DateTime): TimeSeriesRDD[K] = {
    val targetIndex = index.slice(start, end)
    new TimeSeriesRDD(targetIndex,
      mapSeries(TimeSeriesUtils.rebase(index, targetIndex, _, Double.NaN)))
  }

  /**
   * Fills in missing data (NaNs) in each series according to a given imputation method.
   *
   * @param method "linear", "nearest", "next", or "previous"
   * @return A TimeSeriesRDD with missing observations filled in.
   */
  def fill(method: String): TimeSeriesRDD[K] = {
    mapSeries(UnivariateTimeSeries.fillts(_, method))
  }

  /**
   * Applies a transformation to each time series that preserves the time index of this
   * TimeSeriesRDD.
   */
  def mapSeries[U](f: (Vector[Double]) => Vector[Double]): TimeSeriesRDD[K] = {
    new TimeSeriesRDD(index, map(kt => (kt._1, f(kt._2))))
  }

  /**
   * Applies a transformation to each time series and returns a TimeSeriesRDD with the given index.
   * The caller is expected to ensure that the time series produced line up with the given index.
   */
  def mapSeries[U](index: DateTimeIndex, f: (Vector[Double]) => Vector[Double])
    : TimeSeriesRDD[K] = {
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
  def toSamples(nPartitions: Int = -1): RDD[(DateTime, Vector[Double])] = {
    val maxChunkSize = 20

    val dividedOnMapSide = mapPartitionsWithIndex { case (partitionId, iter) =>
      new Iterator[((Int, Int), Vector[Double])] {
        // Each chunk is a buffer of time series
        var chunk: ArrayBuffer[Vector[Double]] = _
        // Current date time.  Gets reset for every chunk.
        var dtLoc: Int = _
        var chunkId: Int = -1

        def hasNext: Boolean = iter.hasNext || dtLoc < index.size
        def next(): ((Int, Int), Vector[Double]) = {
          if (chunk == null || dtLoc == index.size) {
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
          ((dtLoc, partitionId * maxChunkSize + chunkId), new DenseVector(arr))
        }
      }
    }

    // At this point, dividedOnMapSide is an RDD of snippets of full samples that will be
    // assembled on the reduce side.  Each key is a tuple of
    // (date-time, position of snippet in full sample)

    val partitioner = new Partitioner() {
      val nPart = if (nPartitions == -1) parent.partitions.size else nPartitions
      def numPartitions: Int = nPart
      def getPartition(key: Any): Int = key.asInstanceOf[(Int, _, _)]._1 / nPart
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
    repartitioned.mapPartitions { iter: Iterator[((Int, Int), Vector[Double])] =>
      new Iterator[(DateTime, Vector[Double])] {
        var cur = if (iter.hasNext) iter.next() else null
        def hasNext: Boolean = next != null

        def next(): (DateTime, Vector[Double]) = {
          val snippets = new ArrayBuffer[Vector[Double]]()
          var vecSize = 0
          val dtLoc = cur._1._1
          while (cur != null && cur._1._1 == dtLoc) {
            snippets += cur._2
            vecSize += cur._2.size
            if (iter.hasNext) {
              cur = iter.next()
            }
          }
          val dt = index.dateTimeAtLoc(dtLoc)
          val vec = if (snippets.size == 1) {
            snippets(0)
          } else {
            val resVec = DenseVector.zeros[Double](vecSize)
            var i = 0
            snippets.foreach { snip =>
              resVec(i until snip.length) := snip
              i += snip.length
            }
            resVec
          }
          (dt, vec)
        }
      }
    }
  }

  def compute(split: Partition, context: TaskContext): Iterator[(K, Vector[Double])] = {
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
  def timeSeriesRDD[K <: AnyRef](targetIndex: UniformDateTimeIndex,
      seriesRDD: RDD[(K, UniformDateTimeIndex, Vector[Double])]): TimeSeriesRDD[K] = {
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
  def timeSeriesRDD[K <: AnyRef](targetIndex: DateTimeIndex, seriesRDD: RDD[TimeSeries[K]])
    : TimeSeriesRDD[K] = {
    val rdd = seriesRDD.flatMap { series =>
      series.univariateKeyAndSeriesIterator().map { case (key, vec) =>
        (key, TimeSeriesUtils.rebase(series.index, targetIndex, vec, Double.NaN))
      }
    }
    new TimeSeriesRDD(targetIndex, rdd)
  }
}
