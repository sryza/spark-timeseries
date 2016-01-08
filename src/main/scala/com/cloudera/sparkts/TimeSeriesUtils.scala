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

import java.time._

/**
 * Internal utilities for dealing with 1-D time series.
 */
private[sparkts] object TimeSeriesUtils {
  type Rebaser = Vector[Double] => Vector[Double]

  def union(series: Array[Array[Double]]): Array[Double] = {
    val unioned = Array.fill(series.head.length)(Double.NaN)
    var i = 0
    while (i < unioned.length) {
      var j = 0
      while (java.lang.Double.isNaN(series(j)(i))) {
        j += 1
      }
      if (j < series.length) {
        unioned(i) = series(j)(i)
      }
    }
    i += 1
    unioned
  }

  def union(indexes: Array[UniformDateTimeIndex], series: Array[Array[Double]])
    : (UniformDateTimeIndex, Array[Double]) = {
    val freq = indexes.head.frequency
    if (!indexes.forall(_.frequency == freq)) {
      throw new IllegalArgumentException("Series must have conformed frequencies to be unioned")
    }

    throw new UnsupportedOperationException()
  }

  /**
   * Accepts a series of values indexed by the given source index and moves it to conform to a
   * target index. The target index need not fit inside the source index - non-overlapping regions
   * will be filled with NaNs or the given default value.
   *
   * The source index must have the same frequency as the target index.
   */
  def rebase(
      sourceIndex: DateTimeIndex,
      targetIndex: DateTimeIndex,
      vec: Vector[Double],
      defaultValue: Double): Vector[Double] = {
    rebaser(sourceIndex, targetIndex, defaultValue)(vec)
  }

  /**
   * Returns a function that accepts a series of values indexed by the given source index and moves
   * it to conform to a target index. The target index need not fit inside the source index -
   * non-overlapping regions will be filled with NaNs or the given default value.
   *
   * The source index must have the same frequency as the target index.
   */
  def rebaser(
      sourceIndex: DateTimeIndex,
      targetIndex: DateTimeIndex,
      defaultValue: Double): Rebaser = {
    targetIndex match {
      case targetDTI: UniformDateTimeIndex =>
        sourceIndex match {
          case sourceDTI: UniformDateTimeIndex =>
            rebaserWithUniformSource(sourceDTI, targetDTI, defaultValue)
          case sourceDTI: IrregularDateTimeIndex =>
            rebaserWithIrregularSource(sourceDTI, targetDTI, defaultValue)
          case _ =>
            rebaserGeneric(sourceIndex, targetIndex, defaultValue)
        }
      case targetDTI: IrregularDateTimeIndex =>
        sourceIndex match {
          case sourceDTI: IrregularDateTimeIndex =>
            rebaserIrregularSourceIrregularTarget(sourceDTI, targetDTI, defaultValue)
          case _ =>
            rebaserGeneric(sourceIndex, targetIndex, defaultValue)
        }
      case _ =>
        rebaserGeneric(sourceIndex, targetIndex, defaultValue)
    }
  }

  /**
   * Implementation for rebase when source index is uniform.
   */
  private def rebaserWithUniformSource(
      sourceIndex: UniformDateTimeIndex,
      targetIndex: UniformDateTimeIndex,
      defaultValue: Double): Rebaser = {
    val startLoc = sourceIndex.frequency.difference(sourceIndex.first, targetIndex.first)
    val endLoc = sourceIndex.frequency.difference(sourceIndex.first, targetIndex.last) + 1

    val safeStartLoc = math.max(startLoc, 0)
    val resultStartLoc = if (startLoc < 0) -startLoc else 0

    vec: Vector[Double] => {
      if (startLoc >= 0 && endLoc <= vec.length) {
        vec(startLoc until endLoc)
      } else {
        val resultVec = DenseVector.fill(endLoc - startLoc) { defaultValue }
        val safeEndLoc = math.min(endLoc, vec.length)
        val resultEndLoc = resultStartLoc + (safeEndLoc - safeStartLoc)
        resultVec(resultStartLoc until resultEndLoc) := vec(safeStartLoc until safeEndLoc)
        resultVec
      }
    }
  }

  /**
   * Implementation for rebase when source index is irregular.
   */
  private def rebaserWithIrregularSource(
      sourceIndex: IrregularDateTimeIndex,
      targetIndex: UniformDateTimeIndex,
      defaultValue: Double): Rebaser = {
    val startLoc = -targetIndex.locAtDateTime(sourceIndex.first)
    val startLocInSourceVec = math.max(0, startLoc)
    val dtsRelevant: Iterator[Long] = sourceIndex.instants.iterator.drop(startLocInSourceVec)

    vec: Vector[Double] => {
      val vecRelevant: Iterator[Double] = vec(startLocInSourceVec until vec.length).valuesIterator
      val iter = iterateWithUniformFrequency(dtsRelevant.map(dts =>
        longToZonedDateTime(dts, targetIndex.zone))
        .zip(vecRelevant), targetIndex.frequency, defaultValue)

      val resultArr = new Array[Double](targetIndex.size)
      for (i <- 0 until targetIndex.size) {
        // Add leading or trailing NaNs if target index starts earlier than source index or ends
        // after the source index
        resultArr(i) = if (i < -startLoc || !iter.hasNext) {
          defaultValue
        } else {
          assert(iter.hasNext)
          iter.next()._2
        }
      }
      new DenseVector(resultArr)
    }
  }

  private def rebaserIrregularSourceIrregularTarget(
      sourceIndex: IrregularDateTimeIndex,
      targetIndex: IrregularDateTimeIndex,
      defaultValue: Double): Rebaser = {
    // indexMapping(i) = j means that resultArr(i) should be filled with the value from vec(j)
    val indexMapping = new Array[Int](targetIndex.size)

    val sourceStamps = sourceIndex.instants
    val targetStamps = targetIndex.instants
    var posInSource = 0
    var i = 0
    while (i < targetIndex.size) {
      while (posInSource < sourceStamps.length && sourceStamps(posInSource) < targetStamps(i)) {
        posInSource += 1
      }
      if (posInSource < sourceStamps.length && sourceStamps(posInSource) == targetStamps(i)) {
        indexMapping(i) = posInSource
      } else {
        indexMapping(i) = -1
      }
      i += 1
    }

    indexMappingRebaser(indexMapping, defaultValue)
  }

  /**
   * Note: time complexity of this method depends on the type of the source index
   * , more specifically on the time complexity of the locAtDateTime method of the
   * source index. If n and m are the lengths of the target and source indices
   * respectively, this method takes O(n.f(m)) where f(m) is the complexity of
   * locAtDateTime method of the source index.
   *
   * Source     Complexity
   * uniform    O(n)
   * irregular  O(n.log(m))
   * hybrid     O(n(log(p) + f(q))); where
   *            p: number of indices
   *            q: length of sub index containing the search query
   *
   * more efficient implementations could achieve O(n)
   */
  def rebaserGeneric(
      sourceIndex: DateTimeIndex,
      targetIndex: DateTimeIndex,
      defaultValue: Double): Rebaser = {
    // indexMapping(i) = j means that resultArr(i) should be filled with the value from vec(j)
    val indexMapping = targetIndex.zonedDateTimeIterator.map(sourceIndex.locAtDateTime).toArray
    indexMappingRebaser(indexMapping, defaultValue)
  }

  private def indexMappingRebaser(indexMapping: Array[Int], defaultValue: Double): Rebaser = {
    vec: Vector[Double] => {
      val resultArr = indexMapping.map {
        case -1 => defaultValue
        case otherwise: Any => vec(otherwise)
      }
      new DenseVector(resultArr)
    }
  }

  def samplesToTimeSeries(samples: Iterator[(ZonedDateTime, Double)], frequency: Frequency)
    : (UniformDateTimeIndex, DenseVector[Double]) = {
    val arr = new ArrayBuffer[Double]()
    val iter = iterateWithUniformFrequency(samples, frequency)
    val (firstDT, firstValue) = iter.next()
    arr += firstValue
    while (iter.hasNext) {
      arr += iter.next()._2
    }
    val index = DateTimeIndex.uniform(firstDT, arr.size, frequency)
    (index, new DenseVector[Double](arr.toArray))
  }

  def samplesToTimeSeries(samples: Iterator[(ZonedDateTime, Double)], index: UniformDateTimeIndex)
    : (DenseVector[Double]) = {
    val arr = new Array[Double](index.size)
    val iter = iterateWithUniformFrequency(samples, index.frequency)
    var i = 0
    while (i < arr.length) {
      arr(i) = iter.next()._2
      i += 1
    }
    new DenseVector[Double](arr)
  }

  /**
   * Takes an iterator over time samples not necessarily at uniform intervals and returns an
   * iterator of values at uniform intervals, with NaNs (or a given default value) placed where
   * there is no data.
   *
   * The input samples must be aligned on the given frequency.
   */
  def iterateWithUniformFrequency(samples: Iterator[(ZonedDateTime, Double)], frequency: Frequency,
      defaultValue: Double = Double.NaN): Iterator[(ZonedDateTime, Double)] = {
    // TODO: throw exceptions for points with non-aligned frequencies
    new Iterator[(ZonedDateTime, Double)]() {
      var curTup = if (samples.hasNext) samples.next() else null
      var curUniformDT = if (curTup != null) curTup._1 else null

      def hasNext: Boolean = curTup != null

      def next(): (ZonedDateTime, Double) = {
        val retValue = if (curTup._1 == curUniformDT) {
          val value = curTup._2
          curTup = if (samples.hasNext) samples.next() else null
          value
        } else {
          assert(curTup._1.isAfter(curUniformDT))
          defaultValue
        }
        val dt = curUniformDT
        curUniformDT = frequency.advance(curUniformDT, 1)
        (dt, retValue)
      }
    }
  }

  def minMaxDateTimes(
      index: UniformDateTimeIndex,
      series: Array[Double]): (ZonedDateTime, ZonedDateTime) = {
    var min = Double.MaxValue
    var minDt: ZonedDateTime = null
    var max = Double.MinValue
    var maxDt: ZonedDateTime = null

    var i = 0
    while (i < series.length) {
      if (series(i) < min) {
        min = series(i)
        minDt = index.dateTimeAtLoc(i)
      }
      if (series(i) > max) {
        max = series(i)
        maxDt = index.dateTimeAtLoc(i)
      }
      i += 1
    }
    (minDt, maxDt)
  }

  def longToZonedDateTime(dt: Long, zone: ZoneId = ZoneId.systemDefault()): ZonedDateTime = {
    ZonedDateTime.ofInstant(Instant.ofEpochSecond(dt / 1000000000L, dt % 1000000000L), zone)
  }

  def zonedDateTimeToLong(dt: ZonedDateTime): Long = {
    val secondsInNano = dt.toInstant().getEpochSecond() * 1000000000L
    secondsInNano + dt.getNano()
  }
}
