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
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import org.json4s._
import org.json4s.jackson.JsonMethods._

object QuandlParser extends CSVParser with JSONParser {
  override protected val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd")

  override def jsonStringToTimeSeries(s: String): TimeSeries = {
    implicit lazy val formats = org.json4s.DefaultFormats
    val json = parse(s)

    val labels = (json \ "column_names" \ classOf[JString]).map( cn => cn.toString ) toArray
    val data = (json \ "data").extract[List[List[Object]]]

    val samples = data.map { quote =>
      val dt = dateTimeFormatter.parseDateTime(quote.head.toString())

      (dt, quote.tail.map(n => parseDouble(n.toString())) toArray)
    }.reverse

    timeSeriesFromSamples(samples, labels)
  }
}
