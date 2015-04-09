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

import breeze.linalg._

object UnivariateTimeSeries {
  /**
   * Trim leading NaNs from a series.
   */
  def trimLeading(ts: Vector[Double]): Vector[Double] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Trim trailing NaNs from a series.
   */
  def trimTrailing(ts: Vector[Double]): Vector[Double] = {
    throw new UnsupportedOperationException()
  }

  def firstNotNaN(ts: Vector[Double]): Int = {
    var i = 0
    while (i < ts.length) {
      if (!java.lang.Double.isNaN(ts(i))) {
        return i
      }
      i += 1
    }
    i
  }

  def lastNotNaN(ts: Vector[Double]): Int = {
    var i = ts.length - 1
    while (i >= 0) {
      if (!java.lang.Double.isNaN(i)) {
        return i
      }
      i -= 1
    }
    i
  }
}
