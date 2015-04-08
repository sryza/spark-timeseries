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

import breeze.linalg._

import org.apache.commons.math3.random.RandomGenerator

private[finance] object Util {
  def sampleWithReplacement(values: Array[Double], rand: RandomGenerator, target: Array[Double])
    : Unit = {
    for (i <- 0 until target.length) {
      target(i) = values(rand.nextInt(values.length))
    }
  }

  def transpose(arr: Array[Array[Double]]): Array[Array[Double]] = {
    val mat = new Array[Array[Double]](arr.head.length)
    for (i <- 0 until arr.head.length) {
      mat(i) = arr.map(_(i)).toArray
    }
    mat
  }

  def matToRowArrs(mat: Matrix[Double]): Array[Array[Double]] = {
    val arrs = new Array[Array[Double]](mat.rows)
    for (r <- 0 until mat.rows) {
      arrs(r) = mat(r to r, 0 to mat.cols - 1).toDenseMatrix.toArray
    }
    arrs
  }

  def arrsToMat(arrs: Iterator[Array[Double]]): DenseMatrix[Double] = {
    vecArrsToMats(arrs, arrs.length).next
  }

  def vecArrsToMats(vecArrs: Iterator[Array[Double]], chunkSize: Int)
    : Iterator[DenseMatrix[Double]] = {
    new Iterator[DenseMatrix[Double]] {
      def hasNext: Boolean = vecArrs.hasNext

      def next(): DenseMatrix[Double] = {
        val firstVec = vecArrs.next()
        val vecLen = firstVec.length
        val arr = new Array[Double](chunkSize * vecLen)
        System.arraycopy(firstVec, 0, arr, 0, vecLen)

        var i = 1
        var offs = 0
        while (i < chunkSize && vecArrs.hasNext) {
          val vec = vecArrs.next()
          System.arraycopy(vec, 0, arr, offs, vecLen)
          i += 1
          offs += vecLen
        }

        new DenseMatrix[Double](i, firstVec.length, arr)
      }
    }
  }
}
