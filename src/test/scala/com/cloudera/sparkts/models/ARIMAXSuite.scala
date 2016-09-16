/**
 * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
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

import breeze.linalg.{DenseMatrix, DenseVector => BreezeDenseVector}
import org.apache.spark.mllib.linalg.DenseVector
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class ARIMAXSuite extends FunSuite {
  // Data from http://www.robjhyndman.com/data/ - command to use this data available on website
  // robjhyndman.com/talks/RevolutionR/exercises1.pdf

  def getTrainData(col1: Int, col2: Int) = {
    val train = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("data_train.csv")).getLines()

    train.drop(1).map(a => a.split(",",4).map(_.trim).slice(col1,col2).map(va => va.toDouble)).toArray.flatten
  }

  def getTestData(col1: Int, col2: Int) = {
    val test = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("data_test.csv")).getLines()

    test.drop(1).map(a => a.split(",",4).map(_.trim).slice(col1,col2).map(va => va.toDouble)).toArray.flatten
  }


  val gdp_train = getTrainData(3,4)
  val sales_train =  getTrainData(1,2)
  val adBudget_train = getTrainData(2,3)

  val gdp_test = getTestData(3,4)
  val sales_test = getTestData(1,2)
  val adBudget_test = getTestData(2,3)

  val tsTrain = new DenseVector(gdp_train)
  val xregTrain = new DenseMatrix(rows = sales_train.length, cols = 2, data = sales_train ++ adBudget_train)
  val tsTest = new BreezeDenseVector(gdp_test)
  val xregTest = new DenseMatrix(rows = sales_test.length, cols = 2, data = sales_test ++ adBudget_test)

  val tsTrain_2 = new DenseVector(Array(93.0,82,109,110,109,84,100,91,119,78,99,92,76,99,84,103,107,106,106,89,121,103,92,94,99,94,90,99,100,125,78,95,92,84,99,88,85,121,119,94,89,121,110,110,78,88,86,77,106,127,91,98,108,110,88,118,112,104,97,100,97,96,95,111,84,102,98,110,108,92,121,104,109,105,93,74,106,118,97,109,90,91,95,95,111,112,96,122,108,96,78,124,79,89,98,127,110,92,120,109,106,124,135,110,98,108,109,103,106,92,89,82,118,94,112,86))
  val xregTrain_2 = new DenseMatrix(rows = 116, cols = 4, data = Array(416,393,444,445,426,435,471,397,454,416,424,395,401,471,400,418,476,436,442,472,492,443,418,417,423,382,433,409,436,437,372,419,423,415,432,413,361,415,437,391,395,468,415,386,410,437,401,446,492,443,438,417,384,418,403,408,380,422,432,405,437,444,485,426,411,440,400,440,432,439,431,384,404,439,401,401,427,375,411,428,376,407,403,454,478,418,428,401,467,456,446,509,406,431,458,469,450,462,538,435,485,439,451,457,495,479,418,423,430,477,423,462,481,406,450,405,
    0,0,0,0,1,1,0,0,0,0,0,1.0,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,0,0,0,0,
    28,28,28,28,28,28,29,29,29,29,29,29,29,21,21,21,21,21,21,21,28,28,28,28,28,28,28,21,21,21,21,21,21,21,30,30,30,30,30,30,30,42,42,42,15,15,15,15,19,19,19,19,19,19,19,23,23,23,23,23,23,23,25,25,25,25,25,25,25,16,16,16,16,16,16,16,17,17,17,17,17,17,17,21,21,21,21,21,26,26,26,35,35,35,35,35,35,35,34,34,34,34,34,34,34,25,25,25,25,25,25,25,24,24,24,24,
    55,57,53,55,57,50,50,53,51,55,48,46,42,41,48,48,55,59,57,55,59,53,46,44,41,33,32,42,41,37,44,41,44,42,41,37,46,46,37,44,42,39,41,35,57,62,55,53,53,55,55,42,46,42,42,48,50,44,50,48,50,57,55,59,59,53,57,60,55,51,44,42,41,48,50,46,41,39,50,53,48,42,39,33,44,37,35,41,54,53,50,47,52,52,57,53,53,50,55,46,51,56,57,57,57,53,50,42,49,52,53,50,46,48,49,52))
  val tsTest_2 = new BreezeDenseVector(Array(100.0 ,98 ,102 ,98 ,112 ,99 ,99 ,87 ,103 ,115 ,101 ,125 ,117 ,109 ,111 ,105))
  val xregTest_2 = new DenseMatrix(rows = 16, cols = 4, data = Array( 465,453,472,454,432,431,475,393,437,537,462,539,471,455,466,490,
    1,1,0,0,0,0,0,1,1,0,0,0,0,0,1.0,1,
    24,24,25,25,25,25,25,25,25,23,23,23,23,23,23,23,
    51,54,49,46,42,41,45,46,48,41,42,48,43,47,48,46 ))

  def mean(t: BreezeDenseVector[Double]): Double = {
    val ts = new DenseVector(t.toArray)
    var sum = 0.0
    ts.values.map( va => sum += va)
    sum / t.length
  }
  def maxDeviation(mean: Double, ts: BreezeDenseVector[Double]): Double ={
    var max = 0.0
    for (t <- ts) {
      val error = Math.abs(t - mean)
      if (error > max) max = error }
    max
  }

  /* First data set */
  test("MAX(0,0,1) 1 true - first data set"){
    val model = ARIMAX.fitModel(0, 0, 1, tsTrain, xregTrain, 1)
    assert(model.coefficients.length == 6)
    val results = model.forecast(tsTest, xregTest)
    results.length should be (tsTest.length)
    val avg = mean(tsTest)

    for (i <- results ) {
      i should be (avg +- 10)
    }
  }

  test("MAX(0,0,2) 1 true"){
    val model = ARIMAX.fitModel(0, 0, 2, tsTrain_2, xregTrain_2, 1)
    assert(model.coefficients.length == 11)
    val results = model.forecast(tsTest_2, xregTest_2)
    results.length should be (tsTest_2.length)
    val avg = mean(tsTest_2)

    for (i <- results ) {
      i should be (avg +- 2)
    }
  }
  test("IMAX(0,1,1) 1 true"){
    val model = ARIMAX.fitModel(0, 1, 1, tsTrain_2, xregTrain_2, 1)
    assert(model.coefficients.length == 10)
    val results = model.forecast(tsTest_2, xregTest_2)
    results.length should be (tsTest_2.length)
    val avg = mean(tsTest_2)

    for (i <- results ) {
      i should be (avg +- 2)
    }
  }

  test("ARIMAX(2,1,1) 1 true false - first data set"){
    val model = ARIMAX.fitModel(2, 1, 1, tsTrain, xregTrain, 1, includeIntercept = false)
    assert(model.coefficients.length == 8)
    val results = model.forecast(tsTest, xregTest)
    results.length should be (tsTest.length)
    val avg = mean(tsTest)

    // The timeseries data are quite scattered that's why we multiply max deviation by 2 
    for (i <- results ) {
      i should be (avg +- maxDeviation(mean(tsTest),tsTest )*2)
    }
  }

  test("ARIMAX(1,1,1) 1 true true"){
    val model = ARIMAX.fitModel(1, 1, 1, tsTrain_2, xregTrain_2, 1)
    assert(model.coefficients.length == 11)
    val results = model.forecast(tsTest_2, xregTest_2)
    results.length should be (tsTest_2.length)
    val avg = mean(tsTest_2)

    for (i <- results ) {
      i should be (avg +- 5)
    }
  }

  test("ARIMAX(2,1,1) 1 true false"){
    val model = ARIMAX.fitModel(2, 1, 1, tsTrain_2, xregTrain_2, 1, includeIntercept = false)
    assert(model.coefficients.length == 12)
    val results = model.forecast(tsTest_2, xregTest_2)
    results.length should be (tsTest_2.length)
    val avg = mean(tsTest_2)

    for (i <- results ) {
      i should be (avg +- 10)
    }
  }
}
