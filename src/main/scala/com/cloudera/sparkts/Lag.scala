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

object Lag {
  /**
   * Makes a lag matrix from the given time series with the given lag, trimming both rows and
   * columns so that every element in the matrix is full.
   */
  private[sparkts] def lagMatTrimBoth(x: Array[Double], maxLag: Int): Array[Array[Double]] = {
    lagMatTrimBoth(x, maxLag, false)
  }

  /**
   * Makes a lag matrix from the given time series with the given lag, trimming both rows and
   * columns so that every element in the matrix is full.
   */
  private[sparkts] def lagMatTrimBoth(x: Array[Double], maxLag: Int, includeOriginal: Boolean)
    : Array[Array[Double]] = {
    val numObservations = x.length
    val numRows = numObservations - maxLag
    val numCols = maxLag + (if (includeOriginal) 1 else 0)
    val lagMat = Array.ofDim[Double](numRows, numCols)
    for (r <- 0 until numObservations - maxLag) {
      for (c <- 0 until maxLag) {
        lagMat(r)(c) = x(r - c + maxLag - 1)
      }
      if (includeOriginal) {
        lagMat(r)(numCols - 1) = x(r)
      }
    }
    lagMat
  }

  /**
   * Makes a lag matrix from the given time series with the given lag, trimming both rows and
   * columns so that every element in the matrix is full.
   */
  private[sparkts] def lagMatTrimBoth(x: Vector[Double], maxLag: Int): Matrix[Double] = {
    lagMatTrimBoth(x, maxLag, false)
  }

  /**
   * Makes a lag matrix from the given time series with the given lag, trimming both rows and
   * columns so that every element in the matrix is full.
   */
  private[sparkts] def lagMatTrimBoth(x: Vector[Double], maxLag: Int, includeOriginal: Boolean)
    : Matrix[Double] = {
    val numObservations = x.size
    val numRows = numObservations - maxLag
    val numCols = maxLag + (if (includeOriginal) 1 else 0)
    val lagMat = new DenseMatrix[Double](numRows, numCols)
    for (r <- 0 until numObservations - maxLag) {
      for (c <- 0 until maxLag) {
        lagMat(r, c) = x(r - c + maxLag - 1)
      }
      if (includeOriginal) {
        lagMat(r, numCols - 1) = x(r)
      }
    }
    lagMat
  }
}
