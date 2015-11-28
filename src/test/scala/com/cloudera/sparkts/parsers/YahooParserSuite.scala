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
package com.cloudera.sparkts.parsers

import java.time.ZoneId
import org.scalatest.{FunSuite, ShouldMatchers}

class YahooParserSuite extends FunSuite with ShouldMatchers {
  test("yahoo parser") {
    val is = getClass.getClassLoader.getResourceAsStream("GOOG.csv")
    val lines = scala.io.Source.fromInputStream(is).getLines().toArray
    val text = lines.mkString("\n")
    val ts = YahooParser.yahooStringToTimeSeries(text, zone = ZoneId.of("Z"))
    ts.data.numRows should be (lines.length - 1)
  }
}
