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

import java.time._
import java.util.Arrays

import org.threeten.extra._
import scala.annotation.tailrec
import scala.language.implicitConversions

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
   * The location of the given date-time, as nanoseconds since the epoch. If the index contains the
   * date-time more than once, returns its first appearance. If the given date-time does not appear
   * in the index, returns -1.
   */
  def locAtDateTime(dt: Long): Int

  /**
    * The location of the given date-time. If the index contains the date-time more than once,
    * returns its first appearance. If the given date-time does not appear in the index, returns the
    * location of the closest anterior DateTime that exists.
    */
  def locAtOrBeforeDateTime(dt: ZonedDateTime): Int
  def locAtOrAfterDateTime(dt: ZonedDateTime): Int

  /**
   * The location at which the given date-time could be inserted. It is the location of the first
   * date-time that is greater than the given date-time. If the given date-time is greater than
   * or equal to the last date-time in the index, the index size is returned.
   */
  def insertionLoc(dt: ZonedDateTime): Int

  /**
   * The location at which the given date-time, as milliseconds since the epoch, could be inserted.
   * It is the location of the first date-time that is greater than the given date-time. If the
   * given date-time is greater than or equal to the last date-time in the index, the index size
   * is returned.
   */
  def insertionLoc(dt: Long): Int

  /**
   * Returns the contents of the DateTimeIndex as an array of nanosecond values from the epoch.
   */
  def toNanosArray(): Array[Long]

  /**
    * Returns the contents of the DateTimeIndex as an array of ZonedDateTime
    */
  def toZonedDateTimeArray(): Array[ZonedDateTime]

  /**
   * Returns an iterator over the contents of the DateTimeIndex as nanosecond values from the epoch.
   */
  def nanosIterator(): Iterator[Long]

  /**
   * Returns an iterator over the contents of the DateTimeIndex as ZonedDateTime
   */
  def zonedDateTimeIterator(): Iterator[ZonedDateTime]

  /**
   * Returns a new DateTimeIndex with instants at the specified zone
   */
  def atZone(zone: ZoneId): DateTimeIndex
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

  override def locAtOrBeforeDateTime(dt: ZonedDateTime): Int = {
    val loc = frequency.difference(first, dt)
    if (loc >= 0 && loc < size) {
      if (dateTimeAtLoc(loc).compareTo(dt) > 0) {
        loc - 1
      } else {
        loc
      }
    } else if (loc < 0) {
      0
    } else {
      size
    }
  }

  override def locAtOrAfterDateTime(dt: ZonedDateTime): Int = {
    val loc = frequency.difference(first, dt)
    if (loc >= 0 && loc < size) {
      if (dateTimeAtLoc(loc).compareTo(dt) < 0) {
        loc + 1
      } else {
        loc
      }
    } else if (loc < 0) {
      0
    } else {
      size
    }
  }

  override def insertionLoc(dt: ZonedDateTime): Int = {
    val loc = frequency.difference(first, dt)
    if (loc >= 0 && loc < size) {
      if (dateTimeAtLoc(loc).compareTo(dt) <= 0) {
        loc + 1
      } else {
        loc
      }
    } else if (loc < 0) {
      0
    } else {
      size
    }
  }

  override def insertionLoc(dt: Long): Int = {
    insertionLoc(longToZonedDateTime(dt, dateTimeZone))
  }

  override def toNanosArray(): Array[Long] = {
    val arr = new Array[Long](periods)
    for (i <- 0 until periods) {
      arr(i) = zonedDateTimeToLong(dateTimeAtLoc(i))
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

  override def hashCode(): Int = {
    first.hashCode() ^ periods ^ frequency.hashCode()
  }

  override def toString: String = {
    Array(
      "uniform", dateTimeZone.toString, start.toString,
      periods.toString, frequency.toString).mkString(",")
  }

  override def nanosIterator(): Iterator[Long] = {
    zonedDateTimeIterator.map(zonedDateTimeToLong(_))
  }

  override def zonedDateTimeIterator(): Iterator[ZonedDateTime] = {
    (0 until periods).view.map(frequency.advance(start, _)).iterator
  }

  override def atZone(zone: ZoneId): UniformDateTimeIndex = {
    new UniformDateTimeIndex(start.withZoneSameInstant(zone), periods, frequency, zone)
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

  override def last: ZonedDateTime =
    longToZonedDateTime(instants(instants.length - 1), dateTimeZone)

  override def zone: ZoneId = dateTimeZone

  override def size: Int = instants.length

  override def dateTimeAtLoc(loc: Int): ZonedDateTime =
    longToZonedDateTime(instants(loc), dateTimeZone)

  override def locAtDateTime(dt: ZonedDateTime): Int = {
    val loc = java.util.Arrays.binarySearch(instants, zonedDateTimeToLong(dt))
    if (loc < 0) -1 else loc
  }

  override def locAtDateTime(dt: Long): Int = {
    val loc = java.util.Arrays.binarySearch(instants, dt)
    if (loc < 0) -1 else loc
  }

  override def locAtOrBeforeDateTime(dt: ZonedDateTime): Int = {
    var loc = java.util.Arrays.binarySearch(instants, zonedDateTimeToLong(dt))
    if (loc >= 0) {
      while (loc > 0 && instants(loc) > zonedDateTimeToLong(dt)) loc -= 1
      loc
    } else {
      -loc - 2
    }
  }

  override def locAtOrAfterDateTime(dt: ZonedDateTime): Int = {
    val instant = zonedDateTimeToLong(dt)
    var loc = java.util.Arrays.binarySearch(instants, instant)
    if (loc >= 0) {
      while (loc < size && instants(loc) < instant) loc += 1

      loc
    } else {
      -loc - 1
    }
  }

  override def insertionLoc(dt: ZonedDateTime): Int = {
    insertionLoc(zonedDateTimeToLong(dt))
  }

  override def insertionLoc(dt: Long): Int = {
    var loc = java.util.Arrays.binarySearch(instants, dt)
    if (loc >= 0) {
      do loc += 1
      while (loc < size && instants(loc) == dt)
      loc
    } else {
      -loc - 1
    }
  }

  override def toNanosArray(): Array[Long] = {
    instants
  }

  override def toZonedDateTimeArray(): Array[ZonedDateTime] = {
    instants.map(l => longToZonedDateTime(l, dateTimeZone))
  }

  override def equals(other: Any): Boolean = {
    val otherIndex = other.asInstanceOf[IrregularDateTimeIndex]
    otherIndex.instants.sameElements(instants)
  }

  override def hashCode(): Int = {
    Arrays.hashCode(instants)
  }

  override def toString: String = {
    "irregular," + dateTimeZone.toString + "," +
      instants.map(longToZonedDateTime(_, dateTimeZone).toString).mkString(",")
  }

  override def nanosIterator(): Iterator[Long] = {
    instants.iterator
  }

  override def zonedDateTimeIterator(): Iterator[ZonedDateTime] = {
    instants.iterator.map(longToZonedDateTime(_, dateTimeZone))
  }

  override def atZone(zone: ZoneId): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(instants, zone)
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

  private val sizeOnLeft: Array[Int] = {
    indices.init.scanLeft(0)(_ + _.size)
  }

  override def slice(interval: Interval): HybridDateTimeIndex = {
    slice(ZonedDateTime.ofInstant(interval.getStart, dateTimeZone),
      ZonedDateTime.ofInstant(interval.getEnd, dateTimeZone))
  }

  override def slice(start: ZonedDateTime, end: ZonedDateTime): HybridDateTimeIndex = {
    require(start.isBefore(end), s"start($start) should be less than end($end)")

    val startIndex = binarySearch(0, indices.length - 1, start)._1
    val endIndex = binarySearch(0, indices.length - 1, end)._1

    val newIndices =
      if (startIndex == endIndex) {
        Array(indices(startIndex).slice(start, end))
      } else {
        val startDateTimeIndex = indices(startIndex)
        val endDateTimeIndex = indices(endIndex)

        val startSlice = Array(startDateTimeIndex.slice(start, startDateTimeIndex.last))
        val endSlice = Array(endDateTimeIndex.slice(endDateTimeIndex.first, end))

        if (endIndex - startIndex == 1) {
          startSlice ++ endSlice
        } else {
          startSlice ++ indices.slice(startIndex + 1, endIndex) ++ endSlice
        }
      }

    new HybridDateTimeIndex(newIndices, dateTimeZone)
  }

  override def slice(start: Long, end: Long): HybridDateTimeIndex = {
    slice(longToZonedDateTime(start, dateTimeZone),
      longToZonedDateTime(end, dateTimeZone))
  }

  override def islice(range: Range): HybridDateTimeIndex = {
    islice(range.head, range.last + 1)
  }

  override def islice(start: Int, end: Int): HybridDateTimeIndex = {
    require(start < end, s"start($start) should be less than end($end)")

    val startIndex = binarySearch(0, indices.length - 1, start)
    val endIndex = binarySearch(0, indices.length - 1, end)

    val alignedStart = start - sizeOnLeft(startIndex)
    val alignedEnd = end - sizeOnLeft(endIndex)

    val newIndices =
      if (startIndex == endIndex) {
        Array(indices(startIndex).islice(alignedStart, alignedEnd))
      } else {
        val startDateTimeIndex = indices(startIndex)
        val endDateTimeIndex = indices(endIndex)

        val startSlice = Array(startDateTimeIndex.islice(alignedStart, startDateTimeIndex.size))
        val endSlice = Array(endDateTimeIndex.islice(0, alignedEnd))

        if (endIndex - startIndex == 1) {
          startSlice ++ endSlice
        } else {
          startSlice ++ indices.slice(startIndex + 1, endIndex) ++ endSlice
        }
      }

    new HybridDateTimeIndex(newIndices, dateTimeZone)
  }

  override def first: ZonedDateTime = indices(0).first.withZoneSameInstant(dateTimeZone)

  override def last: ZonedDateTime = indices(indices.length - 1).last
    .withZoneSameInstant(dateTimeZone)

  override def zone: ZoneId = dateTimeZone

  override def size: Int = indices.map(_.size).sum

  override def dateTimeAtLoc(loc: Int): ZonedDateTime = {
    val i = binarySearch(0, indices.length - 1, loc)
    if (i < 0) {
      throw new ArrayIndexOutOfBoundsException(s"no dateTime at loc $loc")
    }
    indices(i).dateTimeAtLoc(loc - sizeOnLeft(i))
  }

  @tailrec
  private def binarySearch(low: Int, high: Int, i: Int): Int = {
    if (low <= high) {
      val mid = (low + high) >>> 1
      val midIndex = indices(mid)
      val sizeOnLeftOfMid = sizeOnLeft(mid)
      if (i < sizeOnLeftOfMid) {
        binarySearch(low, mid - 1, i)
      } else if (i >= sizeOnLeftOfMid + midIndex.size) {
        binarySearch(mid + 1, high, i)
      } else {
        mid
      }
    } else {
      -1
    }
  }

  override def locAtDateTime(dt: ZonedDateTime): Int = {
    val i = binarySearch(0, indices.length - 1, dt)._1
    if (i > -1) {
      val loc = indices(i).locAtDateTime(dt)
      if (loc > -1) {
        sizeOnLeft(i) + loc
      } else {
        -1
      }
    } else {
      -1
    }
  }

  override def locAtDateTime(dt: Long): Int = {
    locAtDateTime(longToZonedDateTime(dt, dateTimeZone))
  }

  override def locAtOrBeforeDateTime(dt: ZonedDateTime): Int = {
    val loc = binarySearch(0, indices.length - 1, dt)._2
    if (loc >= 0) {
      sizeOnLeft(loc) + indices(loc).locAtOrBeforeDateTime(dt)
    } else if (dt.isBefore(first)) {
      0
    } else {
      size
    }
  }

  override def locAtOrAfterDateTime(dt: ZonedDateTime): Int = {
    val loc = binarySearch(0, indices.length - 1, dt)._2
    if (loc >= 0) {
      sizeOnLeft(loc) + indices(loc).locAtOrAfterDateTime(dt)
    } else if (dt.isAfter(last)) {
      size
    } else {
      0
    }
  }

  override def insertionLoc(dt: ZonedDateTime): Int = {
    val loc = binarySearch(0, indices.length - 1, dt)._2
    if (loc >= 0) {
      sizeOnLeft(loc) + indices(loc).insertionLoc(dt)
    } else if (dt.isBefore(first)) {
      0
    } else {
      size
    }
  }

  override def insertionLoc(dt: Long): Int = {
    insertionLoc(longToZonedDateTime(dt, dateTimeZone))
  }

  /**
   * Returns a tuple (a, b):
   *  a: is the array index of the date-time index that contains the queried date-time dt
   *     or -1 if dt is not found. This value is used by locAtDateTime method.
   *  b: is the array index of the date-time index where the queried date-time dt could
   *     be inserted. This value is used by insertionLoc method.
   */
  @tailrec
  private def binarySearch(low: Int, high: Int, dt: ZonedDateTime): (Int, Int) = {
    if (low <= high) {
      val mid = (low + high) >>> 1
      val midIndex = indices(mid)
      if (dt.isBefore(midIndex.first)) {
        binarySearch(low, mid - 1, dt)
      } else if (dt.isAfter(midIndex.last)) {
        binarySearch(mid + 1, high, dt)
      } else {
        (mid, mid)
      }
    } else {
      // if coming from the call "binarySearch(low, mid - 1, dt)"
      // on the condition "if (dt.isBefore(midIndex.first))"
      if (high >= 0 && dt.isAfter(indices(high).last)) {
        (-1, high)
        // if coming from the call "binarySearch(mid + 1, high, dt)"
        // on the condition "if (dt.isAfter(midIndex.last))"
      } else if (low < indices.length && dt.isBefore(indices(low).first)) {
        (-1, low)
      } else {
        (-1, -1)
      }
    }
  }

  override def toNanosArray(): Array[Long] = {
    indices.flatMap(_.toNanosArray)
  }

  override def toZonedDateTimeArray(): Array[ZonedDateTime] = {
    indices.flatMap(_.toZonedDateTimeArray)
  }

  override def equals(other: Any): Boolean = {
    val otherIndex = other.asInstanceOf[HybridDateTimeIndex]
    otherIndex.indices.sameElements(indices)
  }

  override def hashCode(): Int = {
    Arrays.hashCode(indices.asInstanceOf[Array[AnyRef]])
  }

  override def toString: String = {
    "hybrid," + dateTimeZone.toString + "," +
      indices.map(_.toString).mkString(";")
  }

  override def nanosIterator(): Iterator[Long] = {
    indices.foldLeft(Iterator[Long]())(_ ++ _.nanosIterator)
  }

  override def zonedDateTimeIterator(): Iterator[ZonedDateTime] = {
    indices.foldLeft(Iterator[ZonedDateTime]())(_ ++ _.zonedDateTimeIterator)
  }

  override def atZone(zone: ZoneId): HybridDateTimeIndex = {
    new HybridDateTimeIndex(indices.map(_.atZone(zone)), zone)
  }
}

object DateTimeIndex {
  /**
   * Creates a UniformDateTimeIndex with the given start time, number of periods, frequency,
   * and time zone(optionally)
   */
  def uniform(
      start: Long,
      periods: Int,
      frequency: Frequency): UniformDateTimeIndex = {
    val zone = ZoneId.systemDefault()
    new UniformDateTimeIndex(longToZonedDateTime(start, zone), periods, frequency)
  }

  /**
   * Creates a UniformDateTimeIndex with the given start time, number of periods, frequency,
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
   * Creates a UniformDateTimeIndex with the given start time, number of periods, and frequency
   * using the time zone of start time.
   */
  def uniform(start: ZonedDateTime, periods: Int, frequency: Frequency): UniformDateTimeIndex = {
    new UniformDateTimeIndex(start, periods, frequency, start.getZone)
  }

  /**
   * Creates a UniformDateTimeIndex with the given start time, number of periods, frequency
   * , and time zone
   */
  def uniform(start: ZonedDateTime, periods: Int, frequency: Frequency, zone: ZoneId)
    : UniformDateTimeIndex = {
    new UniformDateTimeIndex(start, periods, frequency, zone)
  }

  /**
   * Creates a UniformDateTimeIndex with the given start time and end time (inclusive)
   * and frequency.
   */
  def uniformFromInterval(start: Long, end: Long, frequency: Frequency): UniformDateTimeIndex = {
    val tz = ZoneId.systemDefault()
    uniform(start, frequency.difference(longToZonedDateTime(start, tz),
      longToZonedDateTime(end, tz)) + 1, frequency, tz)
  }

  /**
    * Creates a UniformDateTimeIndex with the given start time and end time (inclusive), frequency
    * and time zone.
    */
  def uniformFromInterval(start: Long, end: Long, frequency: Frequency, tz: ZoneId)
    : UniformDateTimeIndex = {
    uniform(start, frequency.difference(longToZonedDateTime(start, tz),
      longToZonedDateTime(end, tz)) + 1, frequency, tz)
  }

  /**
   * Creates a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency
   * using the time zone of start time.
   */
  def uniformFromInterval(
      start: ZonedDateTime,
      end: ZonedDateTime,
      frequency: Frequency): UniformDateTimeIndex = {
    uniform(start, frequency.difference(start, end) + 1, frequency)
  }

  /**
   * Creates a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency
   * and time zone.
   */
  def uniformFromInterval(
      start: ZonedDateTime,
      end: ZonedDateTime,
      frequency: Frequency,
      zone: ZoneId): UniformDateTimeIndex = {
    uniform(start, frequency.difference(start, end) + 1, frequency, zone)
  }

  /**
   * Creates an IrregularDateTimeIndex composed of the given date-times using the time zone
   * of the first date-time in dts array.
   */
  def irregular(dts: Array[ZonedDateTime]): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts.map(zdt => zonedDateTimeToLong(zdt)), dts.head.getZone)
  }

  /**
   * Creates an IrregularDateTimeIndex composed of the given date-times and zone
   */
  def irregular(dts: Array[ZonedDateTime], zone: ZoneId): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts.map(zdt => zonedDateTimeToLong(zdt)), zone)
  }

  /**
   * Creates an IrregularDateTimeIndex composed of the given date-times, as nanos from the epoch
   * using the default date-time zone.
   */
  def irregular(dts: Array[Long]): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts, ZoneId.systemDefault())
  }

  /**
   * Creates an IrregularDateTimeIndex composed of the given date-times, as nanos from the epoch
   * using the provided date-time zone.
   */
  def irregular(dts: Array[Long], zone: ZoneId): IrregularDateTimeIndex = {
    new IrregularDateTimeIndex(dts, zone)
  }

  /**
   * Creates a HybridDateTimeIndex composed of the given indices.
   * All indices should have the same zone.
   */
  def hybrid(indices: Array[DateTimeIndex]): HybridDateTimeIndex = {
    val zone = indices.head.zone
    require(indices.forall(_.zone.equals(zone)), "All indices should have the same zone")
    new HybridDateTimeIndex(indices, zone)
  }

  /**
   * Unions the given indices into a single index at the default date-time zone
   */
  def union(indices: Array[DateTimeIndex]): DateTimeIndex = {
    DateTimeIndexUtils.union(indices)
  }

  /**
   * Unions the given indices into a single index at the given date-time zone
   */
  def union(indices: Array[DateTimeIndex], zone: ZoneId): DateTimeIndex = {
    DateTimeIndexUtils.union(indices, zone)
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
  private[sparkts] def rebaseDayOfWeek(
      dayOfWeek: Int,
      firstDayOfWeek: Int = DayOfWeek.MONDAY.getValue): Int = {
    (dayOfWeek - firstDayOfWeek + DayOfWeek.MONDAY.getValue +
      DayOfWeek.values.length - 1) % DayOfWeek.values.length + 1
  }

  /**
   * Finds the next business day occurring at or after the given date-time.
   */
  private[sparkts] def nextBusinessDay(
      dt: ZonedDateTime,
      firstDayOfWeek: Int = DayOfWeek.MONDAY.getValue): ZonedDateTime = {
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
    if (period.getDays == 0) {
      throw new UnsupportedOperationException()
    }
    new DayFrequency(period.getDays)
  }

  implicit def intToBusinessDayRichInt(n: Int) : BusinessDayRichInt = new BusinessDayRichInt(n)

  implicit def intToBusinessDayRichInt(tuple: (Int, Int)) : BusinessDayRichInt =
    new BusinessDayRichInt(tuple._1, tuple._2)

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
      case "hybrid" =>
        val zone = ZoneId.of(tokens(1))
        val indices = str.split(",", 3).last.split(";").map(fromString)
        new HybridDateTimeIndex(indices, zone)
      case _ => throw new IllegalArgumentException(
        s"DateTimeIndex type ${tokens(0)} not recognized")
    }
  }
}
