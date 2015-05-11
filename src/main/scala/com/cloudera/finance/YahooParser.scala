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

package com.cloudera.finance

import com.cloudera.sparkts.TimeSeries
import com.cloudera.sparkts.TimeSeries._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.joda.time.DateTime

object YahooParser {
  def yahooStringToTimeSeries(text: String, keyPrefix: String = ""): TimeSeries = {
    val lines = text.split('\n')
    val labels = lines(0).split(',').tail.map(keyPrefix + _)
    val samples = lines.tail.map { line =>
      val tokens = line.split(',')
      val dt = new DateTime(tokens.head)
      (dt, tokens.tail.map(_.toDouble).toArray)
    }.reverse
    timeSeriesFromSamples(samples, labels)
  }

  def yahooFiles(dir: String, sc: SparkContext): RDD[TimeSeries] = {
    sc.wholeTextFiles(dir).map { case (path, text) =>
      YahooParser.yahooStringToTimeSeries(text, path.split('/').last)
    }
  }
}
