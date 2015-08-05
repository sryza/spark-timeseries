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

package com.cloudera.finance.parsers

import com.cloudera.sparkts.TimeSeries
import com.cloudera.sparkts.TimeSeries._

import org.joda.time.format.DateTimeFormatter

import scala.util.control.NonFatal

abstract class CSVParser {
  protected val dateTimeFormatter: DateTimeFormatter

  private val UTF8_BOM: String = "\uFEFF"

  // if we're missing or have bad data, use Double.NaN
  private def parseDouble(s: String): Double = try { s.toDouble } catch { case NonFatal(_) => Double.NaN }

  def stringToTimeSeries(text: String, keyPrefix: String = ""): TimeSeries = {
    val lines = stripUTF8BOM(text).split('\n')
    val labels = lines(0).split(',').tail.map(keyPrefix + _)

    val samples = lines.tail.map { line =>
      val tokens = line.split(',')
      val dt = dateTimeFormatter.parseDateTime(tokens.head)

      (dt, tokens.tail.map(parseDouble(_)))
    }.reverse

    timeSeriesFromSamples(samples, labels)
  }

  private def stripUTF8BOM(s: String): String = {
    if (s.startsWith(UTF8_BOM)) {
      s.substring(1)
    } else {
      s
    }
  }
}
