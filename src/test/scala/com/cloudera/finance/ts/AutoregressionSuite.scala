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

package com.cloudera.finance.ts

import Autoregression._

import java.util.Random

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class AutoregressionSuite extends FunSuite {
  test("lagMatTrimBoth") {
    val expected = Array(Array(2.0, 1.0), Array(3.0, 2.0))
    lagMatTrimBoth(Array(1.0, 2.0, 3.0, 4.0), 2) should be (expected)
  }

  test("add and remove time dependent effects") {
    val rand = new Random()
    val ts = Array.fill(1000)(rand.nextDouble())
    val model = new ARModel(1.5, Array(.2, .3))
    val added = model.addTimeDependentEffects(ts, new Array[Double](ts.length))
    val removed = model.removeTimeDependentEffects(added, new Array[Double](ts.length))
    assert(ts.zip(removed).forall(x => x._1 - x._2 < .001))
  }
}
