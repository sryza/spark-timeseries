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

// Warning: this is not DST-aware. If you need DST-awareness, derive a new class from this
// one and override the appropriate methods with your DST-aware implementation.
abstract class PeriodFrequency(val period: Duration) extends Frequency {

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
  extends PeriodFrequency(ChronoUnit.MILLIS.getDuration.multipliedBy(ms)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val duration = Duration.between(dt1.toLocalDateTime, dt2.toLocalDateTime)
    (duration.toMillis() / ms).toInt
  }

  override def toString: String = s"milliseconds $ms"
}

class MicrosecondFrequency(val us: Int)
  extends PeriodFrequency(ChronoUnit.MICROS.getDuration.multipliedBy(us)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val duration = Duration.between(dt1.toLocalDateTime, dt2.toLocalDateTime)
    ((duration.toNanos() / 1000.0) / us).toInt
  }

  override def toString: String = s"microseconds $us"
}

class MonthFrequency(val months: Int)
  extends PeriodFrequency(ChronoUnit.MONTHS.getDuration.multipliedBy(months)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val period = Period.between(dt1.toLocalDate, dt2.toLocalDate)
    period.getMonths / months
  }

  override def toString: String = s"months $months"
}

class YearFrequency(val years: Int)
  extends PeriodFrequency(ChronoUnit.YEARS.getDuration.multipliedBy(years)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val period = Period.between(dt1.toLocalDate, dt2.toLocalDate)
    period.getYears / years
  }

  override def toString: String = s"years $years"
}

class DayFrequency(val days: Int)
  extends PeriodFrequency(ChronoUnit.DAYS.getDuration.multipliedBy(days)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val period = Period.between(dt1.toLocalDate, dt2.toLocalDate)
    period.getDays / days
  }

  override def toString: String = s"days $days"
}

class HourFrequency(val hours: Int)
  extends PeriodFrequency(ChronoUnit.HOURS.getDuration.multipliedBy(hours)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val duration = Duration.between(dt1.toLocalDateTime, dt2.toLocalDateTime)
    (duration.getSeconds / (hours * 3600)).toInt
  }

  override def toString: String = s"hours $hours"
}

class MinuteFrequency(val minutes: Int)
  extends PeriodFrequency(ChronoUnit.MINUTES.getDuration.multipliedBy(minutes)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val duration = Duration.between(dt1.toLocalDateTime, dt2.toLocalDateTime)
    (duration.getSeconds / (minutes * 60)).toInt
  }

  override def toString: String = s"minutes $minutes"
}

class SecondFrequency(val seconds: Int)
  extends PeriodFrequency(ChronoUnit.SECONDS.getDuration.multipliedBy(seconds)) {

  override def difference(dt1: ZonedDateTime, dt2: ZonedDateTime): Int = {
    val duration = Duration.between(dt1.toLocalDateTime, dt2.toLocalDateTime)
    (duration.getSeconds / seconds).toInt
  }

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
