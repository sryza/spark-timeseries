package org.apache.spark.mllib

/**
 * Supported kernel functions by Kernel
 * @author santanu.das
 */

object KernelType extends Enumeration {
  type KernelType = Value
  val Cosine, Euclidean, Product, ScaledProduct = Value
}
