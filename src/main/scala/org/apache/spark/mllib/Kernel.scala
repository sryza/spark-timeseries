package org.apache.spark.mllib

/**
 * @author debasish83, santanu.das
 */

import KernelType._
import org.apache.spark.mllib.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.mllib.util._

trait Kernel {
  def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double

  def compute(vi: Vector, vj: Vector): Double = {
    compute(vi: Vector, 0, vj: Vector, 0)
  }
}

case class CosineKernelWithNorm(rowNorms: Map[Long, Double], threshold: Double) extends Kernel {
  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    val similarity = BLAS.dot(vi, vj) / rowNorms(indexi) / rowNorms(indexj)
    if (similarity <= threshold) return 0.0
    similarity
  }
}

case class CosineKernel() extends Kernel {
  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    val similarity = BLAS.dot(vi, vj) / Vectors.norm(vi, 2) / Vectors.norm(vi, 2)
    similarity
  }
}

case class EuclideanKernelWithNorm(rowNorms: Map[Long, Double], threshold: Double) extends Kernel {
  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    val distanceSquare = MLUtils.fastSquaredDistance(vi, Vectors.norm(vi, 2), vj, Vectors.norm(vj, 2))
    val similarity = Math.sqrt(distanceSquare)
    if (similarity <= threshold) return 0.0
    similarity
  }
}

case class EuclideanKernel() extends Kernel {
  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    val distanceSquare = MLUtils.fastSquaredDistance(vi, Vectors.norm(vi, 2), vj, Vectors.norm(vj, 2))
    val similarity = Math.sqrt(distanceSquare)
    similarity
  }
}

case class ProductKernel() extends Kernel {
  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    BLAS.dot(vi, vj)
  }
}

case class ScaledProductKernelWithNorm(rowNorms: Map[Long, Double]) extends Kernel {
  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    BLAS.dot(vi, vj) / rowNorms(indexi)
  }
}

case class ScaledProductKernel() extends Kernel {
  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    BLAS.dot(vi, vj) / Vectors.norm(vi, 2)
  }
}

// TO DO: Add more sparse kernels like poly2 and neural net kernel for kernel factorization/classification
object Kernel {
  def apply(metric: KernelType) : Kernel = {
    metric match {
      case Euclidean => new EuclideanKernel()
      case Cosine => new CosineKernel()
      case Product => new ProductKernel()
      case ScaledProduct => new ScaledProductKernel()
    }
  }
}
