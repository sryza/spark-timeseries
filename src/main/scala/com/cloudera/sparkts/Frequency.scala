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

import com.cloudera.sparkts.DateTimeIndex._
import java.time._
import java.time.temporal._

class BusinessDayRichInt(n: Int, firstDayOfWeek: Int = DayOfWeek.MONDAY.getValue) {
  def businessDays: BusinessDayFrequency = new BusinessDayFrequency(n, firstDayOfWeek)
}

/**
 * A frequency for a uniform index.
 */
trait Frequency extends Serializable {
  /**
   * Advances the given DateTime by this frequency n times.
   */
  def advance(dt: ZonedDateTime, n: Int): ZonedDateTime

  /**
   * The number of times this frequency occurs between the two DateTimes, rounded down.
   */
  def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int
}

class DurationFrequency(val duration: Duration) extends Frequency {
  val durationNanos = duration.getSeconds * 1000000000L + duration.getNano

  def advance(dt: ZonedDateTime, n: Int): ZonedDateTime = dt.plus(duration.multipliedBy(n))

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val between = Duration.between(dt1, dt2)
    val betweenNanos = between.getSeconds * 1000000000L + between.getNano
    (betweenNanos / durationNanos).toInt
  }

  override def equals(other: Any): Boolean = {
    other match {
      case frequency: DurationFrequency => frequency.duration == duration
      case _ => false
    }
  }

  override def hashCode(): Int = {
    duration.hashCode()
  }
}

abstract class PeriodFrequency(val period: Period) extends Frequency {
  def advance(dt: ZonedDateTime, n: Int): ZonedDateTime = dt.plus(period.multipliedBy(n))

  override def equals(other: Any): Boolean = {
    other match {
      case frequency: PeriodFrequency => frequency.period == period
      case _ => false
    }
  }

  override def hashCode(): Int = {
    period.hashCode()
  }
}

class MillisecondFrequency(val ms: Int)
  extends DurationFrequency(ChronoUnit.MILLIS.getDuration.multipliedBy(ms)) {

  override def toString: String = s"milliseconds $ms"
}

class MicrosecondFrequency(val us: Int)
  extends DurationFrequency(ChronoUnit.MICROS.getDuration.multipliedBy(us)) {

  override def toString: String = s"microseconds $us"
}

class MonthFrequency(val months: Int)
  extends PeriodFrequency(Period.ofMonths(months)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val diffMonths = ChronoUnit.MONTHS.between(dt1.toLocalDate, dt2.toLocalDate)
    (diffMonths / months).toInt
  }

  override def toString: String = s"months $months"
}

class YearFrequency(val years: Int)
  extends PeriodFrequency(Period.ofYears(years)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val diffYears = ChronoUnit.YEARS.between(dt1.toLocalDate, dt2.toLocalDate)
    (diffYears / years).toInt
  }

  override def toString: String = s"years $years"
}

class DayFrequency(val days: Int)
  extends PeriodFrequency(Period.ofDays(days)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val diffDays = ChronoUnit.DAYS.between(dt1, dt2)

    (diffDays / days).toInt
  }

  override def toString: String = s"days $days"
}

class HourFrequency(val hours: Int)
  extends DurationFrequency(ChronoUnit.HOURS.getDuration.multipliedBy(hours)) {

  override def toString: String = s"hours $hours"
}

class MinuteFrequency(val minutes: Int)
  extends DurationFrequency(ChronoUnit.MINUTES.getDuration.multipliedBy(minutes)) {

  override def toString: String = s"minutes $minutes"
}

class SecondFrequency(val seconds: Int)
  extends DurationFrequency(ChronoUnit.SECONDS.getDuration.multipliedBy(seconds)) {

  override def toString: String = s"seconds $seconds"
}

class BusinessDayFrequency(
  val days: Int,
  val firstDayOfWeek: Int = DayOfWeek.MONDAY.getValue)
  extends Frequency {
  /**
   * Advances the given DateTime by (n * days) business days.
   */
  def advance(dt: ZonedDateTime, n: Int): ZonedDateTime = {
    val dayOfWeek = dt.getDayOfWeek
    val alignedDayOfWeek = rebaseDayOfWeek(dayOfWeek.getValue, firstDayOfWeek)
    if (alignedDayOfWeek > 5) {
      throw new IllegalArgumentException(s"$dt is not a business day")
    }
    val totalDays = n * days
    val standardWeekendDays = (totalDays / 5) * 2
    val remaining = totalDays % 5
    val extraWeekendDays = if (alignedDayOfWeek + remaining > 5) 2 else 0
    dt.plusDays(totalDays + standardWeekendDays + extraWeekendDays)
  }

  def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    if (dt2.isBefore(dt1)) {
      return -difference(dt2, dt1)
    }
    val daysBetween = ChronoUnit.DAYS.between(dt1, dt2)
    val dayOfWeek1 = dt1.getDayOfWeek
    val alignedDayOfWeek1 = rebaseDayOfWeek(dayOfWeek1.getValue, firstDayOfWeek)
    if (alignedDayOfWeek1 > 5) {
      throw new IllegalArgumentException(s"$dt1 is not a business day")
    }
    val standardWeekendDays = (daysBetween / 7) * 2
    val remaining  = daysBetween % 7
    val extraWeekendDays = if (alignedDayOfWeek1 + remaining > 5) 2 else 0
    ((daysBetween - standardWeekendDays - extraWeekendDays) / days).toInt
  }

  override def equals(other: Any): Boolean = {
    other match {
      case frequency: BusinessDayFrequency => frequency.days == days
      case _ => false
    }
  }

  override def hashCode(): Int = days

  override def toString: String = s"businessDays $days firstDayOfWeek $firstDayOfWeek"
}
