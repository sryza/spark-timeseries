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

import com.github.nscala_time.time.Imports._

import org.joda.time.DateTime

/**
 * Internal utilities for dealing with 1-D time series.
 */
private[sparkts] object TimeSeriesUtils {
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
   *
   * Currently only irregular target indices are not supported with uniform source indices.
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
   *
   * Currently only irregular target indices are not supported with uniform source indices.
   */
  def rebaser(
      sourceIndex: DateTimeIndex,
      targetIndex: DateTimeIndex,
      defaultValue: Double): Vector[Double] => Vector[Double] = {
    targetIndex match {
      case targetDTI: UniformDateTimeIndex =>
        sourceIndex match {
          case sourceDTI: UniformDateTimeIndex =>
            rebaserWithUniformSource(sourceDTI, targetDTI, defaultValue)
          case sourceDTI: IrregularDateTimeIndex =>
            rebaserWithIrregularSource(sourceDTI, targetDTI, defaultValue)
          case _ =>
            throw new scala.UnsupportedOperationException("Unrecognized source index type")
        }
      case targetDTI: IrregularDateTimeIndex =>
        sourceIndex match {
          case sourceDTI: IrregularDateTimeIndex =>
            rebaserIrregularSourceIrregularTarget(sourceDTI, targetDTI, defaultValue)
          case _ =>
            throw new scala.UnsupportedOperationException(
              "Irregular targets only supported with irregular sources")
        }
    }
  }

  /**
   * Implementation for rebase when source index is uniform.
   */
  private def rebaserWithUniformSource(
      sourceIndex: UniformDateTimeIndex,
      targetIndex: UniformDateTimeIndex,
      defaultValue: Double): Vector[Double] => Vector[Double] = {
    val startLoc = sourceIndex.frequency.difference(
      new DateTime(sourceIndex.first), targetIndex.first)
    val endLoc = sourceIndex.frequency.difference(
      new DateTime(sourceIndex.first), targetIndex.last) + 1

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
      defaultValue: Double): Vector[Double] => Vector[Double] = {
    val startLoc = -targetIndex.locAtDateTime(sourceIndex.first)
    val startLocInSourceVec = math.max(0, startLoc)
    val dtsRelevant = sourceIndex.instants.iterator.drop(startLocInSourceVec).map(new DateTime(_))

    vec: Vector[Double] => {
      val vecRelevant = vec(startLocInSourceVec until vec.length).valuesIterator
      val iter = iterateWithUniformFrequency(dtsRelevant.zip(vecRelevant), targetIndex.frequency,
        defaultValue)

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
      defaultValue: Double): Vector[Double] => Vector[Double] = {
    // indexMapping(i) = j means that resultArr(i) should be filled with the value from
    // vec(j)
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

    vec: Vector[Double] => {
      var i = 0
      val resultArr = new Array[Double](targetIndex.size)
      while (i < indexMapping.length) {
        if (indexMapping(i) != -1) {
          resultArr(i) = vec(indexMapping(i))
        } else {
          resultArr(i) = defaultValue
        }
        i += 1
      }
      new DenseVector(resultArr)
    }
  }

  def samplesToTimeSeries(samples: Iterator[(DateTime, Double)], frequency: Frequency)
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

  def samplesToTimeSeries(samples: Iterator[(DateTime, Double)], index: UniformDateTimeIndex)
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
  def iterateWithUniformFrequency(samples: Iterator[(DateTime, Double)], frequency: Frequency,
      defaultValue: Double = Double.NaN): Iterator[(DateTime, Double)] = {
    // TODO: throw exceptions for points with non-aligned frequencies
    new Iterator[(DateTime, Double)]() {
      var curTup = if (samples.hasNext) samples.next() else null
      var curUniformDT = if (curTup != null) curTup._1 else null

      def hasNext: Boolean = curTup != null

      def next(): (DateTime, Double) = {
        val retValue = if (curTup._1 == curUniformDT) {
          val value = curTup._2
          curTup = if (samples.hasNext) samples.next() else null
          value
        } else {
          assert(curTup._1 > curUniformDT)
          defaultValue
        }
        val dt = curUniformDT
        curUniformDT = frequency.advance(curUniformDT, 1)
        (dt, retValue)
      }
    }
  }

  def minMaxDateTimes(index: UniformDateTimeIndex, series: Array[Double]): (DateTime, DateTime) = {
    var min = Double.MaxValue
    var minDt: DateTime = null
    var max = Double.MinValue
    var maxDt: DateTime = null

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
}
