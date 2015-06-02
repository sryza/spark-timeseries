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

import breeze.linalg._

import com.cloudera.sparkts.TimeSeriesStatisticalTests._

import org.apache.commons.math3.random.MersenneTwister

import org.scalatest.{FunSuite, ShouldMatchers}

class TimeSeriesStatisticalTestsSuite extends FunSuite with ShouldMatchers {
  test("breusch-godfrey") {
    // Replicating the example provided by R package lmtest for bgtest 
    val rand = new MersenneTwister(5L) 
    val n = 100
    val coef = 0.5 // coefficient for lagged series
    val x = Array.fill(n / 2)(Array(1.0, -1.0)).flatten
    val y1 = x.map(_ + 1 + rand.nextDouble) // stationary 
    // AR(1), recursive filter with 0.5 coef
    val y2 = y1.scanLeft(0.0) { case (prior, curr) => prior * coef + curr }.tail 
    val pthreshold = 0.05
    
    // there should be no evidence of serial correlation
    bgtest(new DenseVector(y1), new DenseMatrix(x.length, 1, x), 1)._2 should be > pthreshold 
    bgtest(new DenseVector(y1), new DenseMatrix(x.length, 1, x), 4)._2 should be > pthreshold 
    // there should be evidence of serial correlation
    bgtest(new DenseVector(y2), new DenseMatrix(x.length, 1, x), 1)._2 should be < pthreshold
    bgtest(new DenseVector(y2), new DenseMatrix(x.length, 1, x), 4)._2 should be < pthreshold
  }

}
