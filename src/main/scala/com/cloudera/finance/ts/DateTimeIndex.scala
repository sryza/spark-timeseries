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

import com.github.nscala_time.time.Imports._

/**
 * A DateTimeIndex maintains a bi-directional mapping between integers and an ordered collection of
 * date-times. Multiple date-times may correspond to the same integer, implying multiple samples
 * at the same date-time.
 */
trait DateTimeIndex {
  /**
   * Returns a sub-slice of the index, starting and ending at the given date-times.
   */
  def slice(start: DateTime, end: DateTime): DateTimeIndex

  def sliceSeries(start: DateTime, end: DateTime, series: Vector[Double])
    : (DateTimeIndex, Vector[Double])

  /**
   * The first date-time in the index.
   */
  def start(): DateTime

  /**
   * The last date-time in the index. Inclusive.
   */
  def end(): DateTime

  /**
   * The number of date-times in the index.
   */
  def size(): Int

  def splitEvenly(numPartitions: Int): Array[DateTimeIndex]

  def dateTimeAtLoc(loc: Int): DateTime
}

class UniformDateTimeIndex(val start: DateTime, val periods: Int, val frequency: Frequency)
  extends DateTimeIndex {

  def sliceSeries(start: DateTime, end: DateTime, series: Vector[Double])
    : (DateTimeIndex, Vector[Double]) = {
    throw new UnsupportedOperationException()
  }

  def end(): DateTime = {
    throw new UnsupportedOperationException()
  }

  def size: Int = periods

  def locOfDateTime(dateTime: DateTime, round: Boolean): Int = {
    throw new UnsupportedOperationException()
  }

  def apply(i: Int): DateTime = {
    frequency.advance(start, i)
  }

  def slice(start: DateTime, end: DateTime): UniformDateTimeIndex = {
    throw new UnsupportedOperationException()
  }

  def slice(lower: Int, upper: Int): UniformDateTimeIndex = {
    new UniformDateTimeIndex(frequency.advance(start, lower), upper - lower, frequency)
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

  def dateTimeAtLoc(loc: Int): DateTime = ???
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
   * The first date-time in the index.
   */
  override def start(): DateTime = new DateTime(instants(0))

  /**
   * The last date-time in the index. Inclusive.
   */
  override def end(): DateTime = new DateTime(instants(instants.length - 1))

  /**
   * The number of date-times in the index.
   */
  override def size(): Int = instants.length

  override def splitEvenly(numPartitions: Int): Array[DateTimeIndex] = ???

  override def dateTimeAtLoc(loc: Int): DateTime = new DateTime(instants(loc))

}

object DateTimeIndex {
  def uniform(start: DateTime, end: DateTime, frequency: Frequency): UniformDateTimeIndex = {
    throw new UnsupportedOperationException()
  }

  implicit def periodToFrequency(period: Period): Frequency = new PeriodFrequency(period)

  implicit def intToBusinessDayRichInt(n: Int): BusinessDayRichInt = new BusinessDayRichInt(n)
}

class BusinessDayRichInt(n: Int) {
  def businessDays(): BusinessDayFrequency = new BusinessDayFrequency(n)
}

/**
 * A frequency for a uniform index.
 */
trait Frequency {
  /**
   * Advances the given DateTime by this frequency n times.
   */
  def advance(dt: DateTime, n: Int): DateTime
}

class PeriodFrequency(period: Period) extends Frequency {
  /**
   * Advances the given DateTime by the period n times.
   */
  def advance(dt: DateTime, n: Int): DateTime = dt + (n * period)
}

class BusinessDayFrequency(days: Int) extends Frequency {
  /**
   * Advances the given DateTime by n business days.
   */
  def advance(dt: DateTime, n: Int): DateTime = {
    val dayOfWeek = dt.getDayOfWeek
    if (dayOfWeek > 5) {
      throw new IllegalArgumentException(s"$dt is not a business day")
    }
    val totalDays = n * days
    val standardWeekendDays = (totalDays / 5) * 2
    val remaining = totalDays % 5
    val extraWeekendDays = if (dayOfWeek + remaining > 5) 2 else 0
    dt + (totalDays + standardWeekendDays + extraWeekendDays).days
  }
}