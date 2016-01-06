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

package com.cloudera.sparkts.stats

import breeze.linalg._
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests._
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
    val err2 = err1.zipWithIndex.map { case (xi, i) =>  if(i % 2 == 0) xi * varFactor else xi }

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
    pval1 should be > 0.05

    // serially correlated
    val coef = 0.3
    val dep = indep.scanLeft(0.0) { case (prior, curr) => prior * coef + curr }.tail
    val vecDep = new DenseVector(dep)
    val (stat2, pval2) = lbtest(vecDep, 2)
    pval2 should be < 0.05
  }

  test("KPSS test: R equivalence") {
    // Note that we only test the statistic, as in contrast with the R tseries implementation
    // we do not calculate a p-value, but rather return a map of appropriate critical values
    // R's tseries linearly interpolates between critical values.

    // version.string R version 3.2.0 (2015-04-16)
    // > set.seed(10)
    // > library(tseries)
    // > v <- rnorm(20)
    // > kpss.test(v, "Level")
    //
    // KPSS Test for Level Stationarity
    //
    // data:  v
    // KPSS Level = 0.27596, Truncation lag parameter = 1, p-value = 0.1
    //
    // Warning message:
    // In kpss.test(v, "Level") : p-value greater than printed p-value
    // > kpss.test(v, "Trend")
    //
    // KPSS Test for Trend Stationarity
    //
    // data:  v
    // KPSS Trend = 0.05092, Truncation lag parameter = 1, p-value = 0.1
    //
    // Warning message:
    // In kpss.test(v, "Trend") : p-value greater than printed p-value

    val arr = Array(0.0187461709418264, -0.184252542069064, -1.37133054992251, -0.599167715783718,
      0.294545126567508, 0.389794300700167, -1.20807617542949, -0.363676017470862,
      -1.62667268170309, -0.256478394123992, 1.10177950308713, 0.755781508027337,
      -0.238233556018718, 0.98744470341339, 0.741390128383824, 0.0893472664958216,
      -0.954943856152377, -0.195150384667239, 0.92552126209408, 0.482978524836611
    )
    val dv = new DenseVector(arr)
    val cTest = kpsstest(dv, "c")._1
    val ctTest = kpsstest(dv, "ct")._1

    cTest should be (0.2759 +- 1e-4)
    ctTest should be (0.05092 +- 1e-4)
  }
}
