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

import scala.Double.NaN

import com.github.nscala_time.time.Imports._

import TimeSeries.timeSeriesFromSamples
import UnivariateTimeSeries._

import org.scalatest.{FunSuite, ShouldMatchers}

class TimeSeriesSuite extends FunSuite with ShouldMatchers {
  ignore("nearest") {
    fillNearest(Array(1.0)) should be (Array(1.0))
    fillNearest(Array(1.0, 1.0, 2.0)) should be (Array(1.0, 1.0, 2.0))
    fillNearest(Array(1.0, NaN, NaN, 2.0)) should be (Array(1.0, 1.0, 2.0, 2.0))
    // round down to previous
    fillNearest(Array(1.0, NaN, 2.0)) should be (Array(1.0, 1.0, 2.0))
    fillNearest(Array(1.0, NaN, NaN, NaN, 2.0)) should be (Array(1.0, 1.0, 1.0, 2.0, 2.0))
    fillNearest(Array(1.0, NaN, 3.0, NaN, 2.0)) should be (Array(1.0, 1.0, 3.0, 3.0, 2.0))
  }

  ignore("previous") {
    fillPrevious(Array(1.0)) should be (Array(1.0))
    fillPrevious(Array(1.0, 1.0, 2.0)) should be (Array(1.0, 1.0, 2.0))
    fillPrevious(Array(1.0, NaN, 2.0)) should be (Array(1.0, 1.0, 2.0))
    fillPrevious(Array(1.0, NaN, NaN, 2.0)) should be (Array(1.0, 1.0, 1.0, 2.0))
    fillPrevious(Array(1.0, NaN, NaN, NaN, 2.0)) should be (Array(1.0, 1.0, 1.0, 1.0, 2.0))
    fillPrevious(Array(1.0, NaN, 3.0, NaN, 2.0)) should be (Array(1.0, 1.0, 3.0, 3.0, 2.0))
  }

  ignore("next") {
    fillNext(Array(1.0)) should be (Array(1.0))
    fillNext(Array(1.0, 1.0, 2.0)) should be (Array(1.0, 1.0, 2.0))
    fillNext(Array(1.0, NaN, 2.0)) should be (Array(1.0, 2.0, 2.0))
    fillNext(Array(1.0, NaN, NaN, 2.0)) should be (Array(1.0, 2.0, 2.0, 2.0))
    fillNext(Array(1.0, NaN, NaN, NaN, 2.0)) should be (Array(1.0, 2.0, 2.0, 2.0, 2.0))
    fillNext(Array(1.0, NaN, 3.0, NaN, 2.0)) should be (Array(1.0, 3.0, 3.0, 2.0, 2.0))
  }

  ignore("linear") {
    fillLinear(Array(1.0)) should be (Array(1.0))
    fillLinear(Array(1.0, 1.0, 2.0)) should be (Array(1.0, 1.0, 2.0))
    fillLinear(Array(1.0, NaN, 2.0)) should be (Array(1.0, 1.5, 2.0))
    fillLinear(Array(2.0, NaN, 1.0)) should be (Array(2.0, 1.5, 1.0))
    fillLinear(Array(1.0, NaN, NaN, 4.0)) should be (Array(1.0, 2.0, 3.0, 4.0))
    fillLinear(Array(1.0, NaN, NaN, NaN, 5.0)) should be (Array(1.0, 2.0, 3.0, 4.0, 5.0))
    fillLinear(Array(1.0, NaN, 3.0, NaN, 2.0)) should be (Array(1.0, 2.0, 3.0, 1.5, 2.0))
  }

  test("timeSeriesFromSamples") {
    val dt = new DateTime("2015-4-8")
    val samples = Array(
      ((dt, Array(1.0, 2.0, 3.0))),
      ((dt + 1.days, Array(4.0, 5.0, 6.0))),
      ((dt + 2.days, Array(7.0, 8.0, 9.0))),
      ((dt + 4.days, Array(10.0, 11.0, 12.0)))
    )
    val labels = Array("a", "b", "c", "d")
    val ts = timeSeriesFromSamples(samples, labels)
    ts.data.valuesIterator.toArray should be ((1 to 12).map(_.toDouble).toArray)
  }
}
