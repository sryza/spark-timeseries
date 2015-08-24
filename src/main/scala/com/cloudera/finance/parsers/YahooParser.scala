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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object YahooParser extends TimestampedCSVParser {
  override protected val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd")

  // TODO: figure out what to do with this function
  def yahooFiles(dir: String, sc: SparkContext): RDD[TimeSeries] = {
    sc.wholeTextFiles(dir).map { case (path, text) =>
      csvStringToTimeSeries(text, path.split('/').last)
    }
  }
}
