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

import org.apache.commons.math3.analysis.{MultivariateFunction}
import org.apache.spark.mllib.linalg._
import org.apache.commons.math3.optimization.direct.BOBYQAOptimizer
import org.apache.commons.math3.optimization.GoalType

/**
 * TODO: add explanation
 */
object HoltWinters {
  
  def fitModel(ts: Vector, period: Int, modelType: String = "additive", method: String = "BOBYQA"): HoltWintersModel = {
    method match {
      //case "CG" => fitModelWithCG(ts)
      case "BOBYQA" => fitModelWithBOBYQA(ts, period, modelType)
      case _ => throw new UnsupportedOperationException("Currently only supports 'CG' and 'BOBYQA'")
    }
  }

  def fitModelWithBOBYQA(ts: Vector, period: Int, modelType:String): HoltWintersModel = {
    val optimizer = new BOBYQAOptimizer(5)
    val objectiveFunction = new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new HoltWintersModel(modelType, period, params(0), params(1), params(2)).sse(ts)
      }
    }
    val goal = GoalType.MINIMIZE
    val params = Array(0.3,0.1,0.1)
		val lower = Array(0.0,0.0,0.0)
		val upper = Array(1.0,1.0,1.0)
		
    val optimal = optimizer.optimize(30000,objectiveFunction,goal,params,lower,upper)
    val cmvalues = optimal.getPoint
    new HoltWintersModel(modelType, period, cmvalues(0), cmvalues(1), cmvalues(2))
  }
}

class HoltWintersModel(
  val modelType: String,
  val period: Int,
  val alpha: Double,
  val beta: Double,
  val gamma: Double) extends TimeSeriesModel {
  /**
   * Calculates sum of squared errors, used to estimate the alpha and beta parameters
   * @param ts A time series for which we want to calculate the SSE, given the current parameters
   * @return SSE
   */
  def sse(ts: Vector): Double = {
    val n = ts.size
    val smoothed = new DenseVector(Array.fill(n)(0.0))
    addTimeDependentEffects(ts, smoothed)

    var error = 0.0
    var sqrErrors = 0.0
    
    for(i <- period to (n - 1)) {
      error = ts(i) - smoothed(i)
      sqrErrors += error * error
    }
    
    sqrErrors
  }

  /**
   * {@inheritDoc}
   */
  override def removeTimeDependentEffects(ts: Vector, dest: Vector = null): Vector = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  /**
   * {@inheritDoc}
   */
  override def addTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
    val destArr = dest.toArray
    val fitted = getHoltWintersComponents(ts)._1
    for (i <- 0 to (dest.size - 1)) {
      destArr(i) = fitted(i)
    }
    dest
  }

  
  /**
   * TODO, explain
   * @param ts
   * @param dest
   */
  
  def forecast(ts: Vector, dest: Vector) = {
    val destArr = dest.toArray
    val (fitted, level, trend, season) = getHoltWintersComponents(ts)
    val n = ts.size
    var si = 0
    val levelVal = level(n - 1)
    val trendVal = trend(n - 1)

    for (i <- 0 until dest.size) {
      // if in sample, fitted, else forecasted
      if (i > n - 1) {
          si = if ((i - n) % period == 0) n else (n - period) + ((i - n) % period)
          destArr(i) = levelVal + (i - n + 1) * trendVal + season(si - 1)
        } else {
          destArr(i) = fitted(i)
        }
      si += 1
    }
    dest
  }

  /**
   * TODO: explain
   * 3 components, level, trend, seasonality
   */
  def getHoltWintersComponents(ts: Vector): (Vector, Vector, Vector, Vector) = {
    val n = ts.size
    require(n >= 2, "Requires length of at least 2")

    val dest = new Array[Double](n)
    
    val level = new Array[Double](n)
    val trend = new Array[Double](n)
    val season = new Array[Double](n)
    
    val (initLevel, initTrend, initSeason) = initHoltWinters(ts)
    level(0) = initLevel
    trend(0) = initTrend
    for(i <- 0 to (initSeason.size - 1)){
      season(i) = initSeason(i)
    }
    
    for (i <- 0 to (n - period - 1)) {
      dest(i + period) = level(i) + trend(i) + season(i)
      
      val levelWeight = ts(i + period) - season(i)
      level(i+1) = alpha * levelWeight + (1 - alpha) * (level(i) + trend(i))
      
      trend(i+1) =  beta * (level(i+1) - level(i)) + (1 - beta) * trend(i)
      
      val seasonWeight = ts(i + period) - level(i+1)
      season(i+period) = gamma * seasonWeight + (1 - gamma) * season(i)
    }
    
    (Vectors.dense(dest), Vectors.dense(level), Vectors.dense(trend), Vectors.dense(season))
  }

  //TODO: add check for length...bad method for short/noisy series as per source
  def initHoltWintersSimple(ts: Vector): (Double, Double, Array[Double]) = {
    val arrTs = ts.toArray
    val lm = arrTs.take(period).sum / period //average value for first m obs
    val bm = arrTs.take(period * 2).splitAt(period).zipped.map { case (prevx, x) =>
        x - prevx
      }.sum / (period * period)
    val siPrelim = arrTs.take(period).map(_ - lm)
    //first m periods initialized to seasonal values, rest zeroed out
    val si = siPrelim ++ Array.fill(arrTs.length - period)(0.0)
    (lm, bm, si)
  }
  
  def getKernel(): (Array[Double]) = {
    
    if (period % 2 == 0){
      var kernel = Array.fill(period+1)(1.0 / period)
      kernel(0) = 0.5 / period
      kernel(period) = 0.5 / period
      kernel
    }
    else{
      val kernel = Array.fill(period)(1.0 / period)
      kernel
    }
    
  }
  
  def convolve(inData: Array[Double],kernel: Array[Double]): (Array[Double]) = {
    val kernelSize = kernel.size
    val dataSize = inData.size
    
    val outData = new Array[Double](dataSize - kernelSize + 1)
    
    var end = 0
		while (end <= (dataSize - kernelSize)) {
			var sum = 0.0
			for ( i <- 0 to (kernelSize - 1))
			{		
				sum += kernel(i) * inData(end+i)
			}
			
			outData(end) = sum
			end = end+1;
		}

    outData
  }
  
  // http://robjhyndman.com/hyndsight/hw-initialization/
  // We initialize using the 2008 suggestion
  def initHoltWinters(ts: Vector): (Double, Double, Array[Double]) = {
    val arrTs = ts.toArray
    
    // Decompose a window of time series into level trend and seasonal using standard convolution  
    val kernel = getKernel()
    val kernelSize = kernel.size
    val trend = convolve(arrTs.take(period * 2), kernel)
    
    //var i = 0
    //for(i <- 0 to trend.size-1) print(trend(i)+",")
    
    // Remove the trend from time series. Subtract for additive and divide for multiplicative                   
	  val removeTrend = arrTs.take(period * 2).zip(
	      Array.fill(((kernelSize - 1) / 2))(0.0) ++ trend ++ Array.fill(((kernelSize - 1) / 2))(0.0)).map{
      case(a,t) => if(t == 0) 0 else (a - t) 
    }
    
		// seasonal mean is sum of mean of all season values of that period
    val seasonalMean = removeTrend.splitAt(period).zipped.map { case (prevx, x) =>
        if(prevx == 0 || x == 0) (x + prevx) else (x + prevx) / 2
      }
    
    val meanOfFigures = seasonalMean.sum / period
      
    // The seasonal mean is then centered and subtracted to get season
    val initSeason = seasonalMean.map(_ - meanOfFigures )
    
    // Do Simple Linear Regression to find the initial level and trend
    val indices = 1 to trend.size
    val xbar = (indices.sum:Double) / indices.size
    val ybar = trend.sum / trend.size
		
    val xxbar = indices.map( x => (x - xbar) * (x - xbar) ).sum
    val xybar = indices.zip(trend).map{
        case(x, y) => (x - xbar) * (y - ybar)
      }.sum
    
    val initTrend = xybar / xxbar
    val initLevel = ybar - (initTrend * xbar)
    
    (initLevel, initTrend, initSeason)
  }
  
}
