/**
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
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

import com.cloudera.sparkts.TimeSeriesStatisticalTests._

import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression
import org.apache.commons.math3.random.MersenneTwister

import org.scalatest.{FunSuite, ShouldMatchers}

class TimeSeriesStatisticalTestsSuite extends FunSuite with ShouldMatchers {
  test("breusch-godfrey") {
    // Replicating the example provided by R package lmtest for bgtest
    val rand = new MersenneTwister(5L)
    val n = 100
    val coef = 0.5 // coefficient for lagged series
    val x = Array.fill(n / 2)(Array(1.0, -1.0)).flatten
    // stationary series
    val y1 = x.map(_ + 1 + rand.nextGaussian())
    // AR(1) series, recursive filter with coef coefficient
    val y2 = y1.scanLeft(0.0) { case (prior, curr) => prior * coef + curr }.tail

    val pthreshold = 0.05

    val OLS1 = new OLSMultipleLinearRegression()
    OLS1.newSampleData(y1, x.map(Array(_)))
    val resids1 = OLS1.estimateResiduals()

    val OLS2 = new OLSMultipleLinearRegression()
    OLS2.newSampleData(y2, x.map(Array(_)))
    val resids2 = OLS2.estimateResiduals()

    // there should be no evidence of serial correlation
    bgtest(new DenseVector(resids1), new DenseMatrix(x.length, 1, x), 1)._2 should be > pthreshold
    bgtest(new DenseVector(resids1), new DenseMatrix(x.length, 1, x), 4)._2 should be > pthreshold
    // there should be evidence of serial correlation
    bgtest(new DenseVector(resids2), new DenseMatrix(x.length, 1, x), 1)._2 should be < pthreshold
    bgtest(new DenseVector(resids2), new DenseMatrix(x.length, 1, x), 4)._2 should be < pthreshold
  }
  
  test("breusch-pagan") {
    // Replicating the example provided by R package lmtest for bptest
    val rand = new MersenneTwister(5L)
    val n = 100
    val x = Array.fill(n / 2)(Array(-1.0, 1.0)).flatten

    // homoskedastic residuals with variance 1 throughout
    val err1 = Array.fill(n)(rand.nextGaussian)
    // heteroskedastic residuals with alternating variance of 1 and 4
    val varFactor = 2
    val err2 = err1.zipWithIndex.map { case (x, i) =>  if(i % 2 == 0) x * varFactor else x }

    // generate dependent variables
    val y1 = x.zip(err1).map { case (xi, ei) => xi + ei + 1 }
    val y2 = x.zip(err2).map { case (xi, ei) => xi + ei + 1 }

    // create models and calculate residuals
    val OLS1 = new OLSMultipleLinearRegression()
    OLS1.newSampleData(y1, x.map(Array(_)))
    val resids1 = OLS1.estimateResiduals()

    val OLS2 = new OLSMultipleLinearRegression()
    OLS2.newSampleData(y2, x.map(Array(_)))
    val resids2 = OLS2.estimateResiduals()

    val pthreshold = 0.05
    // there should be no evidence of heteroskedasticity
    bptest(new DenseVector(resids1), new DenseMatrix(x.length, 1, x))._2 should be > pthreshold
    // there should be evidence of heteroskedasticity
    bptest(new DenseVector(resids2), new DenseMatrix(x.length, 1, x))._2 should be < pthreshold
  }

  test("ljung-box test") {
    val rand = new MersenneTwister(5L)
    val n = 100
    val indep = Array.fill(n)(rand.nextGaussian)
    val vecIndep = new DenseVector(indep)
    val (stat1, pval1) = lbtest(vecIndep, 1)
    pval1 > 0.05

    // serially correlated
    val coef = 0.3
    val dep = indep.scanLeft(0.0) { case (prior, curr) => prior * coef + curr }.tail
    val vecDep = new DenseVector(dep)
    val (stat2, pval2) = lbtest(vecDep, 2)
    pval2 < 0.05
  }
}
