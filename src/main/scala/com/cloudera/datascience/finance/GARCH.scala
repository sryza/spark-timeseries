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

import net.finmath.timeseries.models.parametric.GARCH

object GARCH {
  /**
   * @param ts
   * @return The model and its log likelihood on the input data.
   */
  def fitModel(ts: Array[Double]): (GARCHModel, Double) = {
    val model = new GARCH(ts)
    // Fit the model
    val params = model.getBestParameters
    (new GARCHModel(params.get("Alpha"), params.get("Beta"), params.get("Omega")),
      params.get("Likelihood"))
  }
}

class GARCHModel(alpha: Double, gamma: Double, omega: Double) extends TimeSeriesFilter {
  /**
   * Takes a time series that is assumed to have this model's characteristics and standardizes it
   * to make the observations i.i.d.
   * @param ts Time series of observations with this model's characteristics.
   */
  def standardize(ts: Array[Double]): Array[Double] = {
    throw new UnsupportedOperationException
  }

  /**
   * Takes a time series of i.i.d. observations and filters it to take on this model's
   * characteristics. Modifies the given array in place.
   * @param ts Time series of i.i.d. observations.
   */
  def filter(ts: Array[Double]): Unit = {
    throw new UnsupportedOperationException
  }
}
