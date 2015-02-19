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

package com.cloudera.datascience.finance

import scala.Double.NaN

import com.cloudera.datascience.finance.TimeSeries._

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class TimeSeriesSuite extends FunSuite {
  test("nearest") {
    fillNearest(Array(1.0, NaN, NaN, 2.0)) should be (Array(1.0, 1.0, 2.0, 2.0))
  }
}
