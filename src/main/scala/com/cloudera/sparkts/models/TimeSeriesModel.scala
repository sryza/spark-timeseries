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

package com.cloudera.sparkts.models

import org.apache.spark.mllib.linalg.Vector

/**
 * Models time dependent effects in a time series.
 */
trait TimeSeriesModel extends Serializable {
  /**
   * Takes a time series that is assumed to have this model's characteristics and returns a time
   * series with time-dependent effects of this model removed.
   *
   * This is the inverse of [[TimeSeriesModel#addTimeDependentEffects]].
   *
   * @param ts Time series of observations with this model's characteristics.
   * @param dest Array to put the filtered series, can be the same as ts.
   * @return The dest series, for convenience.
   */
  def removeTimeDependentEffects(ts: Vector, dest: Vector = null): Vector

  /**
   * Takes a series of i.i.d. observations and returns a time series based on it with the
   * time-dependent effects of this model added.
   *
   * @param ts Time series of i.i.d. observations.
   * @param dest Array to put the filtered series, can be the same as ts.
   * @return The dest series, for convenience.
   */
  def addTimeDependentEffects(ts: Vector, dest: Vector = null): Vector
}
