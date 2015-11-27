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

import java.util.{Comparators, Comparator}

import org.threeten.extra._
import scala.language.implicitConversions
import java.time._
import com.cloudera.sparkts.DateTimeIndex._
import com.cloudera.sparkts.TimeSeriesUtils._

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
  def slice(start: ZonedDateTime, end: ZonedDateTime): DateTimeIndex

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
  def first: ZonedDateTime

  /**
   * The last date-time in the index. Inclusive.
   */
  def last: ZonedDateTime

  /**
   * The time zone of date-times in the index.
   */
  def zone: ZoneId

  /**
   * The number of date-times in the index.
   */
  def size: Int

  /**
   * The i-th date-time in the index.
   */
  def dateTimeAtLoc(i: Int): ZonedDateTime

  /**
   * The location of the given date-time. If the index contains the date-time more than once,
   * returns its first appearance. If the given date-time does not appear in the index, returns -1.
   */
  def locAtDateTime(dt: ZonedDateTime): Int

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

  /**
    * Returns the contents of the DateTimeIndex as an array of ZonedDateTime
    */
  def toZonedDateTimeArray(): Array[ZonedDateTime]
}

/**
 * An implementation of DateTimeIndex that contains date-times spaced at regular intervals. Allows
 * for constant space storage and constant time operations.
 */
class UniformDateTimeIndex(
    val start: ZonedDateTime,
    val periods: Int,
    val frequency: Frequency,
    val dateTimeZone: ZoneId = ZoneId.systemDefault())
  extends DateTimeIndex {

  override def first: ZonedDateTime = start

  override def last: ZonedDateTime = frequency.advance(first, periods - 1)

  override def zone: ZoneId = dateTimeZone

  override def size: Int = periods

  override def slice(interval: Interval): UniformDateTimeIndex = {
    slice(ZonedDateTime.ofInstant(interval.getStart(), zone),
      ZonedDateTime.ofInstant(interval.getEnd(), zone))
  }

  override def slice(start: ZonedDateTime, end: ZonedDateTime): UniformDateTimeIndex = {
    uniform(start, frequency.difference(start, end) + 1, frequency, dateTimeZone)
  }

  override def slice(start: Long, end: Long): UniformDateTimeIndex = {
    slice(longToZonedDateTime(start, dateTimeZone),
      longToZonedDateTime(end, dateTimeZone))
  }

  override def islice(range: Range): UniformDateTimeIndex = {
    islice(range.head, range.last + 1)
  }

  override def islice(lower: Int, upper: Int): UniformDateTimeIndex = {
    uniform(frequency.advance(first, lower), upper - lower, frequency, dateTimeZone)
  }

  override def dateTimeAtLoc(loc: Int): ZonedDateTime =
    frequency.advance(first, loc)

  override def locAtDateTime(dt: ZonedDateTime): Int = {
    val loc = frequency.difference(first, dt)
    if (loc >= 0 && loc < size && dateTimeAtLoc(loc) == dt) {
      loc
    } else {
      -1
    }
  }

  override def locAtDateTime(dt: Long): Int = {
    locAtDateTime(longToZonedDateTime(dt, dateTimeZone))
  }

  override def toMillisArray(): Array[Long] = {
    val arr = new Array[Long](periods)
    for (i <- 0 until periods) {
      arr(i) = zonedDateTimeToLong(dateTimeAtLoc(i)) / 1000000L
    }
    arr
  }

  override def toZonedDateTimeArray(): Array[ZonedDateTime] = {
    val arr = new Array[ZonedDateTime](periods)
    for (i <- 0 until periods) {
      arr(i) = dateTimeAtLoc(i)
    }
    arr
  }

  override def equals(other: Any): Boolean = {
    val otherIndex = other.asInstanceOf[UniformDateTimeIndex]
    otherIndex.first == first && otherIndex.periods == periods && otherIndex.frequency == frequency
  }

  override def toString: String = {
    Array(
      "uniform", dateTimeZone.toString, start.toString,
      periods.toString, frequency.toString).mkString(",")
  }
}

/**
 * An implementation of DateTimeIndex that allows date-times to be spaced at uneven intervals.
 * Lookups or slicing by date-time are O(log n) operations.
 */
class IrregularDateTimeIndex(
    val instants: Array[Long],
    val dateTimeZone: ZoneId = ZoneId.systemDefault())
  extends DateTimeIndex {

  override def slice(interval: Interval): IrregularDateTimeIndex = {
    slice(ZonedDateTime.ofInstant(interval.getStart(), zone),
      ZonedDateTime.ofInstant(interval.getEnd(), zone))
  }

  override def slice(start: ZonedDateTime, end: ZonedDateTime): IrregularDateTimeIndex = {
    slice(zonedDateTimeToLong(start), zonedDateTimeToLong(end))
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

  override def first: ZonedDateTime = longToZonedDateTime(instants(0), dateTimeZone)

  override def last: ZonedDateTime = longToZonedDateTime(instants(instants.length - 1), dateTimeZone)

  override def zone: ZoneId = dateTimeZone

  override def size: Int = instants.length

  override def dateTimeAtLoc(loc: Int): ZonedDateTime = longToZonedDateTime(instants(loc), dateTimeZone)

  override def locAtDateTime(dt: ZonedDateTime): Int = {
    val loc = java.util.Arrays.binarySearch(instants, zonedDateTimeToLong(dt))
    if (loc < 0) -1 else loc
  }

  override def locAtDateTime(dt: Long): Int = {
    val loc = java.util.Arrays.binarySearch(instants, dt)
    if (loc < 0) -1 else loc
  }

  override def toMillisArray(): Array[Long] = {
    instants.map(dt => dt / 1000000L)
  }

  override def toZonedDateTimeArray(): Array[ZonedDateTime] = {
    instants.map(l => longToZonedDateTime(l, dateTimeZone))
  }

  override def equals(other: Any): Boolean = {
    val otherIndex = other.asInstanceOf[IrregularDateTimeIndex]
    otherIndex.instants.sameElements(instants)
  }

  override def toString: String = {
    "irregular," + dateTimeZone.toString + "," +
      instants.map(longToZonedDateTime(_, dateTimeZone).toString).mkString(",")
  }
}

/**
 * An implementation of DateTimeIndex that holds a hybrid collection of DateTimeIndex
 * implementations.
 *
 * Indices are assumed to be sorted and disjoint such that
 * for any two consecutive indices i and j: i.last < j.first
 *
 */
class HybridDateTimeIndex(
    val indices: Array[DateTimeIndex],
    val dateTimeZone: ZoneId = ZoneId.systemDefault())
  extends DateTimeIndex {

  override def slice(interval: Interval): DateTimeIndex = ???

  override def slice(start: ZonedDateTime, end: ZonedDateTime): DateTimeIndex = ???

  override def slice(start: Long, end: Long): DateTimeIndex = ???

  override def islice(range: Range): DateTimeIndex = ???

  override def islice(start: Int, end: Int): DateTimeIndex = ???

  override def first: ZonedDateTime = new ZonedDateTime(indices(0).first, dateTimeZone)

  override def last: ZonedDateTime = new ZonedDateTime(indices(indices.length - 1).last, dateTimeZone)

  override def zone: ZoneId = dateTimeZone

  override def size: Long = indices.map(_.size.toLong).sum

  override def dateTimeAtLoc(i: Int): ZonedDateTime = ???

  override def locAtDateTime(dt: ZonedDateTime): Int = ???

  override def locAtDateTime(dt: Long): Int = ???

  override def toMillisArray(): Array[Long] = {
    indices.map(_.toMillisArray).reduce(_ ++ _)
  }

  override def toZonedDateTimeArray(): Array[ZonedDateTime] = {
    indices.map(_.toZonedDateTimeArray).reduce(_ ++ _)
  }

  override def equals(other: Any): Boolean = {
    val otherIndex = other.asInstanceOf[HybridDateTimeIndex]
    otherIndex.indices.sameElements(indices)
  }

  override def toString: String = {
    "hybrid," + dateTimeZone.toString + "," +
      indices.map(_.toString).mkString(";")
  }
}

object DateTimeIndex {
  /**
   * Create a UniformDateTimeIndex with the given start time, number of periods, frequency,
   * and time zone(optionally)
   */
  def uniform(
      start: Long,
      periods: Int,
      frequency: Frequency,
      zone: ZoneId = ZoneId.systemDefault()): UniformDateTimeIndex = {

    new UniformDateTimeIndex(longToZonedDateTime(start, zone), periods, frequency)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time, number of periods, and frequency
   * using the time zone of start time.
   */
  def uniform(start: ZonedDateTime, periods: Int, frequency: Frequency): UniformDateTimeIndex = {
    new UniformDateTimeIndex(start, periods, frequency, start.getZone)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time, number of periods, frequency
   * , and time zone
   */
  def uniform(start: ZonedDateTime, periods: Int, frequency: Frequency, zone: ZoneId)
    : UniformDateTimeIndex = {
    new UniformDateTimeIndex(start, periods, frequency, zone)
  }

  /**
    * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency.
    */
  def uniform(start: ZonedDateTime, end: ZonedDateTime, frequency: Frequency)
    : UniformDateTimeIndex = {
    val tz = ZoneId.systemDefault()
    uniform(start, frequency.difference(start, end) + 1, frequency, tz)
  }

  /**
    * Create a UniformDateTimeIndex with the given start time and end time (inclusive), frequency
    * and time zone.
    */
  def uniform(start: ZonedDateTime, end: ZonedDateTime, frequency: Frequency, tz: ZoneId)
    : UniformDateTimeIndex = {
    uniform(start, frequency.difference(start, end) + 1, frequency, tz)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency.
   */
  def uniform(start: Long, end: Long, frequency: Frequency): UniformDateTimeIndex = {
    val tz = ZoneId.systemDefault()
    uniform(start, frequency.difference(longToZonedDateTime(start, tz),
      longToZonedDateTime(end, tz)) + 1, frequency, tz)
  }

  /**
    * Create a UniformDateTimeIndex with the given start time and end time (inclusive), frequency
    * and time zone.
    */
  def uniform(start: Long, end: Long, frequency: Frequency, tz: ZoneId): UniformDateTimeIndex = {
    uniform(start, frequency.difference(longToZonedDateTime(start, tz),
      longToZonedDateTime(end, tz)) + 1, frequency, tz)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency.
   */
  def uniformFromInterval(start: Long, end: Long, frequency: Frequency, zone: ZoneId): UniformDateTimeIndex = {
    uniform(start, frequency.difference(longToZonedDateTime(start, zone),
      longToZonedDateTime(end, zone)) + 1, frequency, zone)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency
   * using the time zone of start time.
   */
  def uniformFromInterval(start: ZonedDateTime, end: ZonedDateTime, frequency: Frequency): UniformDateTimeIndex = {
    uniform(start, frequency.difference(start, end) + 1, frequency)
  }

  /**
   * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency
   * and time zone.
   */
  def uniformFromInterval(start: ZonedDateTime, end: ZonedDateTime, frequency: Frequency, zone: ZoneId)
    : UniformDateTimeIndex = {
    uniform(start, frequency.difference(start, end) + 1, frequency, zone)
  }

  /**
   * Create an IrregularDateTimeIndex composed of the given date-times using the time zone
   * of the first date-time in dts array.
   */
  def irregular(dts: Array[ZonedDateTime]): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts.map(zdt => zonedDateTimeToLong(zdt)), dts.head.getZone)
  }

  /**
   * Create an IrregularDateTimeIndex composed of the given date-times and zone
   */
  def irregular(dts: Array[ZonedDateTime], zone: ZoneId): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts.map(zdt => zonedDateTimeToLong(zdt)), zone)
  }

  /**
   * Create an IrregularDateTimeIndex composed of the given date-times, as millis from the epoch
   * using the default date-time zone.
   */
  def irregular(dts: Array[Long]): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts, ZoneId.systemDefault())
  }

  /**
   * Create an IrregularDateTimeIndex composed of the given date-times, as millis from the epoch
   * using the provided date-time zone.
   */
  def irregular(dts: Array[Long], zone: ZoneId): IrregularDateTimeIndex = {
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
  def rebaseDayOfWeek(dayOfWeek: Int, firstDayOfWeek: Int = DayOfWeek.MONDAY.getValue): Int = {
    (dayOfWeek - firstDayOfWeek + DayOfWeek.MONDAY.getValue +
      DayOfWeek.values.length - 1) % DayOfWeek.values.length + 1
  }

  /**
   * Finds the next business day occurring at or after the given date-time.
   */
  def nextBusinessDay(dt: ZonedDateTime, firstDayOfWeek: Int = DayOfWeek.MONDAY.getValue): ZonedDateTime = {
    val rebasedDayOfWeek = rebaseDayOfWeek(dt.getDayOfWeek.getValue)
    if (rebasedDayOfWeek == 6) {
      dt.plusDays(2)
    } else if (rebasedDayOfWeek == 7) {
      dt.plusDays(1)
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
        val start = ZonedDateTime.parse(tokens(2))
        val periods = tokens(3).toInt
        val freqTokens = tokens(4).split(" ")
        val freq = freqTokens(0) match {
          case "days" => new DayFrequency(freqTokens(1).toInt)
          case "businessDays" => new BusinessDayFrequency(freqTokens(1).toInt, freqTokens(3).toInt)
          case _ => throw new IllegalArgumentException(s"Frequency ${freqTokens(0)} not recognized")
        }
        uniform(start, periods, freq)
      case "irregular" =>
        val zone = ZoneId.of(tokens(1))
        val dts = new Array[ZonedDateTime](tokens.length - 2)
        for (i <- 2 until tokens.length) {
          dts(i - 2) = ZonedDateTime.parse(tokens(i))
        }
        irregular(dts, zone)
      case _ => throw new IllegalArgumentException(
        s"DateTimeIndex type ${tokens(0)} not recognized")
    }
  }
}