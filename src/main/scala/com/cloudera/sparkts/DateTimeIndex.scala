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

import org.joda.time.DateTimeConstants

import scala.language.implicitConversions

import com.github.nscala_time.time.Imports._

import com.cloudera.sparkts.DateTimeIndex._

/**
 * A DateTimeIndex maintains a bi-directional mapping between integers and an ordered collection of
 * date-times. Multiple date-times may correspond to the same integer, implying multiple samples
 * at the same date-time.
 *
 * To avoid confusion between the meaning of "index" as it appears in "DateTimeIndex" and "index"
 * as a location in an array, in the context of this class, we use "location", or "loc", to refer
 * to the latter.
 *
 * NOTE: In the sequence, index 0 represents the oldest point in time. The highest index is the most
 * recent point.
 */
trait DateTimeIndex extends Serializable {
  /**
   * Returns a sub-slice of the index, confined to the given interval (inclusive).
   */
  def slice(interval: Interval): DateTimeIndex

  /**
   * Returns a sub-slice of the index, starting and ending at the given date-times (inclusive).
   */
  def slice(start: DateTime, end: DateTime): DateTimeIndex

  /**
   * Returns a sub-slice of the index, starting and ending at the given date-times in millis
   * since the epoch (inclusive).
   */
  def slice(start: Long, end: Long): DateTimeIndex

  /**
   * Returns a sub-slice of the index with the given range of indices.
   */
  def islice(range: Range): DateTimeIndex

  /**
   * Returns a sub-slice of the index, starting and ending at the given indices.
   *
   * Unlike the slice methods, islice is exclusive at the top of the range, as is the norm with
   * typical array indexing, meaning that dtIndex.slice(dateTimeAtLoc(0), dateTimeAtLoc(5)) will
   * return an index including one more element than dtIndex.islice(0, 5).
   */
  def islice(start: Int, end: Int): DateTimeIndex

  /**
   * The first date-time in the index.
   */
  def first: DateTime

  /**
   * The last date-time in the index. Inclusive.
   */
  def last: DateTime

  /**
   * The time zone of date-times in the index.
   */
  def zone: DateTimeZone

  /**
   * The number of date-times in the index.
   */
  def size: Int

  /**
   * The i-th date-time in the index.
   */
  def dateTimeAtLoc(i: Int): DateTime

  /**
   * The location of the given date-time. If the index contains the date-time more than once,
   * returns its first appearance. If the given date-time does not appear in the index, returns -1.
   */
  def locAtDateTime(dt: DateTime): Int

  /**
   * The location of the given date-time, as milliseconds since the epoch. If the index contains the
   * date-time more than once, returns its first appearance. If the given date-time does not appear
   * in the index, returns -1.
   */
  def locAtDateTime(dt: Long): Int

  /**
   * Returns the contents of the DateTimeIndex as an array of millisecond values from the epoch.
   */
  def toMillisArray(): Array[Long]
}

/**
 * An implementation of DateTimeIndex that contains date-times spaced at regular intervals. Allows
 * for constant space storage and constant time operations.
 */
class UniformDateTimeIndex(val start: Long, val periods: Int, val frequency: Frequency,
                           val dateTimeZone: DateTimeZone = DateTimeZone.getDefault())
  extends DateTimeIndex {

  override def first: DateTime = new DateTime(start, dateTimeZone)

  override def last: DateTime = frequency.advance(new DateTime(first, dateTimeZone), periods - 1)

  override def zone: DateTimeZone = dateTimeZone

  override def size: Int = periods

  override def slice(interval: Interval): UniformDateTimeIndex = {
    slice(interval.start, interval.end)
  }

  override def slice(start: DateTime, end: DateTime): UniformDateTimeIndex = {
    uniform(start, frequency.difference(start, end) + 1, frequency, dateTimeZone)
  }

  override def slice(start: Long, end: Long): UniformDateTimeIndex = {
    slice(new DateTime(start, dateTimeZone), new DateTime(end, dateTimeZone))
  }

  override def islice(range: Range): UniformDateTimeIndex = {
    islice(range.head, range.last + 1)
  }

  override def islice(lower: Int, upper: Int): UniformDateTimeIndex = {
    uniform(frequency.advance(new DateTime(first, dateTimeZone), lower),
      upper - lower, frequency, dateTimeZone)
  }

  override def dateTimeAtLoc(loc: Int): DateTime =
    frequency.advance(new DateTime(first, dateTimeZone), loc)

  override def locAtDateTime(dt: DateTime): Int = {
    val loc = frequency.difference(new DateTime(first, dateTimeZone), dt)
    if (loc >= 0 && loc < size && dateTimeAtLoc(loc) == dt) {
      loc
    } else {
      -1
    }
  }

  override def locAtDateTime(dt: Long): Int = {
    locAtDateTime(new DateTime(dt, dateTimeZone))
  }

  override def toMillisArray(): Array[Long] = {
    val arr = new Array[Long](periods)
    for (i <- 0 until periods) {
      arr(i) = dateTimeAtLoc(i).getMillis
    }
    arr
  }

  override def equals(other: Any): Boolean = {
    val otherIndex = other.asInstanceOf[UniformDateTimeIndex]
    otherIndex.first == first && otherIndex.periods == periods && otherIndex.frequency == frequency
  }

  override def toString: String = {
    Array(
      "uniform", dateTimeZone.toString, new DateTime(start, dateTimeZone).toString,
      periods.toString, frequency.toString).mkString(",")
  }
}

/**
 * An implementation of DateTimeIndex that allows date-times to be spaced at uneven intervals.
 * Lookups or slicing by date-time are O(log n) operations.
 */
class IrregularDateTimeIndex(val instants: Array[Long],
                             val dateTimeZone: DateTimeZone = DateTimeZone.getDefault())
  extends DateTimeIndex {

  override def slice(interval: Interval): IrregularDateTimeIndex = {
    slice(interval.start, interval.end)
  }

  override def slice(start: DateTime, end: DateTime): IrregularDateTimeIndex = {
    slice(start.getMillis, end.getMillis)
  }

  override def slice(start: Long, end: Long): IrregularDateTimeIndex = {
    val startLoc = locAtDateTime(start)
    val endLoc = locAtDateTime(end)
    new IrregularDateTimeIndex(instants.slice(startLoc, endLoc + 1), dateTimeZone)
  }

  override def islice(range: Range): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(instants.slice(range.head, range.last + 1), dateTimeZone)
  }

  override def islice(start: Int, end: Int): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(instants.slice(start, end), dateTimeZone)
  }

  override def first: DateTime = new DateTime(instants(0), dateTimeZone)

  override def last: DateTime = new DateTime(instants(instants.length - 1), dateTimeZone)

  override def zone: DateTimeZone = dateTimeZone

  override def size: Int = instants.length

  override def dateTimeAtLoc(loc: Int): DateTime = new DateTime(instants(loc), dateTimeZone)

  override def locAtDateTime(dt: DateTime): Int = {
    java.util.Arrays.binarySearch(instants, dt.getMillis)
  }

  override def locAtDateTime(dt: Long): Int = {
    java.util.Arrays.binarySearch(instants, dt)
  }

  override def toMillisArray(): Array[Long] = {
    instants.toArray
  }

  override def equals(other: Any): Boolean = {
    val otherIndex = other.asInstanceOf[IrregularDateTimeIndex]
    otherIndex.instants.sameElements(instants)
  }

  override def toString: String = {
    "irregular," + dateTimeZone.toString + "," +
      instants.map(new DateTime(_, dateTimeZone).toString).mkString(",")
  }
}

object DateTimeIndex {
  /**
   * Create a UniformDateTimeIndex with the given start time, number of periods, and frequency.
   */
  def uniform(start: Long, periods: Int, frequency: Frequency): UniformDateTimeIndex = {
    new UniformDateTimeIndex(start, periods, frequency)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time, number of periods, frequency
   * and time zone.
   */
  def uniform(start: Long, periods: Int, frequency: Frequency, zone: DateTimeZone)
  : UniformDateTimeIndex = {
    new UniformDateTimeIndex(start, periods, frequency, zone)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time, number of periods, and frequency
   * using the time zone of start time.
   */
  def uniform(start: DateTime, periods: Int, frequency: Frequency): UniformDateTimeIndex = {
    new UniformDateTimeIndex(start.getMillis, periods, frequency, start.getZone)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time, number of periods, frequency
   * , and time zone
   */
  def uniform(start: DateTime, periods: Int, frequency: Frequency, zone: DateTimeZone)
  : UniformDateTimeIndex = {
    new UniformDateTimeIndex(start.getMillis, periods, frequency, zone)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency.
   */
  def uniform(start: Long, end: Long, frequency: Frequency): UniformDateTimeIndex = {
    uniform(start, frequency.difference(new DateTime(start, DateTimeZone.getDefault()),
      new DateTime(end, DateTimeZone.getDefault())) + 1, frequency, DateTimeZone.getDefault())
  }

  /**
   * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency.
   */
  def uniform(start: Long, end: Long, frequency: Frequency, zone: DateTimeZone): UniformDateTimeIndex = {
    uniform(start, frequency.difference(new DateTime(start, zone), new DateTime(end, zone)) + 1,
      frequency, zone)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency
   * using the time zone of start time.
   */
  def uniform(start: DateTime, end: DateTime, frequency: Frequency): UniformDateTimeIndex = {
    uniform(start, frequency.difference(start, end) + 1, frequency)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency
   * and time zone.
   */
  def uniform(start: DateTime, end: DateTime, frequency: Frequency, zone: DateTimeZone)
  : UniformDateTimeIndex = {
    uniform(start, frequency.difference(start, end) + 1, frequency, zone)
  }

  /**
   * Create an IrregularDateTimeIndex composed of the given date-times using the time zone
   * of the first date-time in dts array.
   */
  def irregular(dts: Array[DateTime]): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts.map(_.getMillis), dts.head.getZone)
  }

  /**
   * Create an IrregularDateTimeIndex composed of the given date-times and zone
   */
  def irregular(dts: Array[DateTime], zone: DateTimeZone): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts.map(_.getMillis), zone)
  }

  /**
   * Create an IrregularDateTimeIndex composed of the given date-times, as millis from the epoch
   * using the default date-time zone.
   */
  def irregular(dts: Array[Long]): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts)
  }

  /**
   * Create an IrregularDateTimeIndex composed of the given date-times, as millis from the epoch
   * using the provided date-time zone.
   */
  def irregular(dts: Array[Long], zone: DateTimeZone): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts, zone)
  }

  /**
   * Given the ISO index of the day of week, the method returns the day
   * of week index relative to the first day of week i.e. assuming the
   * the first day of week is the base index and has the value of 1
   *
   * For example, if the first day of week is set to be Sunday,
   * the week would be indexed:
   *   Sunday   : 1
   *   Monday   : 2
   *   Tuesday  : 3
   *   Wednesday: 4
   *   Thursday : 5
   *   Friday   : 6 <-- Weekend
   *   Saturday : 7 <-- Weekend
   *
   * If the first day of week is set to be Monday,
   * the week would be indexed:
   *   Monday   : 1
   *   Tuesday  : 2
   *   Wednesday: 3
   *   Thursday : 4
   *   Friday   : 5
   *   Saturday : 6 <-- Weekend
   *   Sunday   : 7 <-- Weekend
   *
   * @param dayOfWeek the ISO index of the day of week
   * @return the day of week index aligned w.r.t the first day of week
   */
  def rebaseDayOfWeek(dayOfWeek: Int, firstDayOfWeek: Int = DateTimeConstants.MONDAY): Int = {
    (dayOfWeek - firstDayOfWeek + DateTimeConstants.MONDAY +
      DateTimeConstants.DAYS_PER_WEEK - 1) % DateTimeConstants.DAYS_PER_WEEK + 1
  }

  /**
   * Finds the next business day occurring at or after the given date-time.
   */
  def nextBusinessDay(dt: DateTime, firstDayOfWeek: Int = DateTimeConstants.MONDAY): DateTime = {
    val rebasedDayOfWeek = rebaseDayOfWeek(dt.getDayOfWeek)
    if (rebasedDayOfWeek == 6) {
      dt + 2.days
    } else if (rebasedDayOfWeek == 7) {
      dt + 1.days
    } else {
      dt
    }
  }

  implicit def periodToFrequency(period: Period): Frequency = {
    if (period.getDays != 0) {
      return new DayFrequency(period.getDays)
    }
    throw new UnsupportedOperationException()
  }

  implicit def intToBusinessDayRichInt(n: Int) : BusinessDayRichInt = new BusinessDayRichInt(n)

  implicit def intToBusinessDayRichInt(tuple: (Int, Int)) : BusinessDayRichInt = new BusinessDayRichInt(tuple._1, tuple._2)

  /**
   * Parses a DateTimeIndex from the output of its toString method
   */
  def fromString(str: String): DateTimeIndex = {
    val tokens = str.split(",")
    tokens(0) match {
      case "uniform" =>
        val zone = DateTimeZone.forID(tokens(1))
        val start = new DateTime(tokens(2), zone)
        val periods = tokens(3).toInt
        val freqTokens = tokens(4).split(" ")
        val freq = freqTokens(0) match {
          case "days" => new DayFrequency(freqTokens(1).toInt)
          case "businessDays" => new BusinessDayFrequency(freqTokens(1).toInt, freqTokens(3).toInt)
          case _ => throw new IllegalArgumentException(s"Frequency ${freqTokens(0)} not recognized")
        }
        uniform(start, periods, freq)
      case "irregular" =>
        val zone = DateTimeZone.forID(tokens(1))
        val dts = new Array[DateTime](tokens.length - 2)
        for (i <- 2 until tokens.length) {
          dts(i - 2) = new DateTime(tokens(i), zone)
        }
        irregular(dts, zone)
      case _ => throw new IllegalArgumentException(
        s"DateTimeIndex type ${tokens(0)} not recognized")
    }
  }
}