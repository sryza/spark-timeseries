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
    val numObservations = x.size
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
  //private[sparkts]
  def lagMatTrimBoth(x: Vector[Double], maxLag: Int, includeOriginal: Boolean)
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

  /**
   * Creates a lagged matrix from a current matrix, by treating each column as a separate
   * vector, lagging and trimming this, and then concatenating these columns. So a matrix
   * [a b c], where a/b/c are column vectors, and lag of 2 becomes a matrix of the form
   * [a a_-1 a_-2 b b_-1 b_-2 c c_-1 c_-2], if the original is included, otherwise of the form
   * [a_-1 a_-2 ...]
   */
  //private[sparkts]
  def lagMatTrimBoth(x: Matrix[Double], maxLag: Int, includeOriginal: Boolean)
    : Matrix[Double] = { (maxLag, includeOriginal) match {
      // want to return something that is well behaved
    case (0, false) => DenseMatrix.zeros[Double](0, 0)
    case _ =>
      // so that we can slice
      val denseX = x.toDenseMatrix
      // one matrix per column, consisting of all its lags
      val matrices = (0 until denseX.cols).map { j =>
        val col: DenseVector[Double] = denseX(::, j)
        lagMatTrimBoth(col, maxLag, includeOriginal)
      }
      // merge the matrices into 1 matrix by concatenating col-wise
      matrices.reduceLeft((x, y) => DenseMatrix.horzcat(x, y))
    }
  }

  private[sparkts] def lagMatTrimBoth(x: Matrix[Double], maxLag: Int)
    : Matrix[Double] = {
    lagMatTrimBoth(x, maxLag, false)
  }
}
