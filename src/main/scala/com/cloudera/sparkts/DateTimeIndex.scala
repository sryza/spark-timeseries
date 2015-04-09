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

import DateTimeIndex._

/**
 * A DateTimeIndex maintains a bi-directional mapping between integers and an ordered collection of
 * date-times. Multiple date-times may correspond to the same integer, implying multiple samples
 * at the same date-time.
 */
trait DateTimeIndex extends Serializable {
  /**
   * Returns a sub-slice of the index, starting and ending at the given date-times.
   */
  def slice(start: DateTime, end: DateTime): DateTimeIndex

  def sliceSeries(start: DateTime, end: DateTime, series: Vector[Double])
    : (DateTimeIndex, Vector[Double])

  /**
   * The first date-time in the index.
   */
  def first(): DateTime

  /**
   * The last date-time in the index. Inclusive.
   */
  def last(): DateTime

  /**
   * The number of date-times in the index.
   */
  def size(): Int

  def splitEvenly(numPartitions: Int): Array[DateTimeIndex]

  def dateTimeAtLoc(loc: Int): DateTime

  def locAtDateTime(dt: DateTime, round: Boolean): Int
}

class UniformDateTimeIndex(val start: Long, val periods: Int, val frequency: Frequency)
  extends DateTimeIndex {

  override def sliceSeries(start: DateTime, end: DateTime, series: Vector[Double])
  : (DateTimeIndex, Vector[Double]) = {
    throw new UnsupportedOperationException()
  }

  /**
   * {@inheritDoc}
   */
  override def first(): DateTime = new DateTime(start)

  /**
   * {@inheritDoc}
   */
  override def last(): DateTime = frequency.advance(new DateTime(first), periods - 1)

  /**
   * {@inheritDoc}
   */
  override def size: Int = periods

  def apply(i: Int): DateTime = {
    frequency.advance(new DateTime(first), i)
  }

  def slice(start: DateTime, end: DateTime): UniformDateTimeIndex = {
    throw new UnsupportedOperationException()
  }

  def slice(lower: Int, upper: Int): UniformDateTimeIndex = {
    uniform(frequency.advance(new DateTime(first), lower), upper - lower, frequency)
  }

  def union(other: UniformDateTimeIndex): UniformDateTimeIndex = {
    //    val minStart =
    //    val maxEnd =
    throw new UnsupportedOperationException()
  }

  def union(others: Seq[UniformDateTimeIndex]): UniformDateTimeIndex = {
    others.fold(this)(_.union(_))
  }

  def splitEvenly(numPartitions: Int): Array[DateTimeIndex] = ???

  /**
   * {@inheritDoc}
   */
  override def dateTimeAtLoc(loc: Int): DateTime = frequency.advance(new DateTime(first), loc)

  /**
   * {@inheritDoc}
   */
  override def locAtDateTime(dt: DateTime, round: Boolean): Int = {
    frequency.difference(new DateTime(first), dt)
  }
}

class IrregularDateTimeIndex(val instants: Array[Long]) extends DateTimeIndex {

  override def sliceSeries(start: DateTime, end: DateTime, series: Vector[Double])
  : (IrregularDateTimeIndex, Vector[Double]) = {
    // binary search for start
    // binary search for end
    throw new UnsupportedOperationException()
  }

  override def slice(start: DateTime, end: DateTime): IrregularDateTimeIndex = {
    throw new UnsupportedOperationException()
  }

  /**
   * {@inheritDoc}
   */
  override def first(): DateTime = new DateTime(instants(0))

  /**
   * {@inheritDoc}
   */
  override def last(): DateTime = new DateTime(instants(instants.length - 1))

  /**
   * {@inheritDoc}
   */
  override def size(): Int = instants.length

  override def splitEvenly(numPartitions: Int): Array[DateTimeIndex] = ???

  /**
   * {@inheritDoc}
   */
  override def dateTimeAtLoc(loc: Int): DateTime = new DateTime(instants(loc))

  /**
   * {@inheritDoc}
   */
  override def locAtDateTime(dt: DateTime, round: Boolean): Int = {
    // TODO: round
    java.util.Arrays.binarySearch(instants, dt.getMillis)
  }

}

object DateTimeIndex {
  def uniform(start: DateTime, periods: Int, frequency: Frequency): UniformDateTimeIndex = {
    new UniformDateTimeIndex(start.getMillis, periods, frequency)
  }

  def uniform(start: DateTime, end: DateTime, frequency: Frequency): UniformDateTimeIndex = {
    uniform(start, frequency.difference(start, end) + 1, frequency)
  }

  def irregular(dts: Array[DateTime]): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts.map(_.getMillis))
  }

  implicit def periodToFrequency(period: Period): Frequency = {
    if (period.getDays != 0) {
      return new DayFrequency(period.getDays)
    }
    throw new UnsupportedOperationException()
  }

  implicit def intToBusinessDayRichInt(n: Int): BusinessDayRichInt = new BusinessDayRichInt(n)
}