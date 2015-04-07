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

import scala.collection.mutable.ArrayBuffer

import breeze.linalg._

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import org.joda.time.DateTime
import org.apache.spark.{Partition, Partitioner, TaskContext}

class TimeSeriesRDD[K](val index: DateTimeIndex, parent: RDD[(K, Vector[Double])])
  extends RDD[(K, Vector[Double])](parent) {

  def sliceSeries(start: DateTime, end: DateTime): TimeSeriesRDD[K] = {
    throw new UnsupportedOperationException()
  }

  def sliceSeries(start: Int, end: Int): TimeSeriesRDD[K] = {
    throw new UnsupportedOperationException()
  }

  def unionSeries(other: TimeSeriesRDD[K]): TimeSeriesRDD[K] = {
    // TODO: allow unioning series with different indices
    // they need to have the same period though
//    val unionRdd = rdd.join(other.rdd).mapValues { tt =>
//      UnivariateTimeSeries.union(Array(tt._1, tt._2))
//    }
//    new MultiTimeSeries(index, unionRdd)
    throw new UnsupportedOperationException()
  }

  def mapSeries[U](f: (K, Vector[Double]) => U): RDD[(K, U)] = {
    map(kt => (kt._1, f(kt._1, kt._2)))
  }

  def foldLeftSeries[U](zero: U)(f: ((U, K, Double)) => U): RDD[(K, U)] = {
    mapSeries((k, t) => t.valuesIterator.foldLeft(zero)((u, v) => f(u, k, v)))
  }

  def seriesStats(): RDD[StatCounter] = {
    map(kt => new StatCounter(kt._2.valuesIterator))
  }

  def seriesMinMaxDates(): RDD[(K, (DateTime, DateTime))] = {
//    rdd.mapValues(series => UnivariateTimeSeries.minMaxDateTimes(index, series))
    throw new UnsupportedOperationException()
  }

  /**
   * Essentially transposes the time series matrix to create an RDD where each record contains a
   * single instant in time and all the values that correspond to it. Involves a shuffle operation.
   *
   * In the returned RDD, the ordering of values within each record corresponds to the ordering of
   * the time series records in the original RDD. The records are ordered by time.
   */
  def toSamples(nPartitions: Int): RDD[(DateTime, Vector[Double])] = {
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
      def numPartitions: Int = nPartitions
      def getPartition(key: Any): Int = key.asInstanceOf[(Int, _, _)]._1 / nPartitions
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
  def timeSeriesRDD[K](targetIndex: UniformDateTimeIndex,
      seriesRDD: RDD[(K, UniformDateTimeIndex, Vector[Double])]): TimeSeriesRDD[K] = {
    val rdd = seriesRDD.map { case (key, index, vec) =>
      val newVec = UnivariateTimeSeries.openSlice(index, targetIndex, vec)
      (key, newVec)
    }
    new TimeSeriesRDD(targetIndex, rdd)
  }
}
