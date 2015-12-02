package com.cloudera.sparkts

import java.time.ZoneId

import scala.collection.mutable.{ListBuffer, PriorityQueue}

private[sparkts] object DateTimeIndexUtils {
  implicit val dateTimeIndexOrdering = new Ordering[DateTimeIndex] {
    override def compare(x: DateTimeIndex, y: DateTimeIndex): Int = {
      val c = x.first.compareTo(y.first)
      if (c == 0) x.size.compareTo(y.size)
      else c
    }
  }

  /**
   * indices are sorted and non-overlapping
   */
  def simplify(indices: Array[DateTimeIndex]): Array[DateTimeIndex] = {
    val simplified = new ListBuffer[DateTimeIndex]
    val accumulator = new ListBuffer[Long]
    for (i <- 0 until indices.length) {
      val currentIndex = indices(i)
      if (currentIndex.isInstanceOf[IrregularDateTimeIndex]) {
        accumulator ++= currentIndex.asInstanceOf[IrregularDateTimeIndex].instants
      } else {
        if (accumulator.size > 0) {
          val newIndex = new IrregularDateTimeIndex(accumulator.toArray, indices(i - 1).zone)
          simplified += newIndex
          accumulator.clear()
        }
        simplified += currentIndex
      }
    }
    simplified.toArray
  }
  
  def union(indices: Array[DateTimeIndex], zone: ZoneId = ZoneId.systemDefault())
    : DateTimeIndex = {
    val indicesPQ = new PriorityQueue[DateTimeIndex]()
    indices.foreach(indicesPQ.enqueue(_))

    val unionList = new ListBuffer[DateTimeIndex]
    unionList += indicesPQ.dequeue

    while (!indicesPQ.isEmpty) {
      val a = unionList.remove(unionList.length - 1)
      var b = indicesPQ.dequeue
      
      var isBTrimmed = false
      while (a.locAtDateTime(b.first) > -1) {
        b = b.islice(1, b.size)
        isBTrimmed = true
      }
      
      if (isBTrimmed && b.size > 0) {
        unionList += a
        indicesPQ.enqueue(b)
      } else {
        val splitLoc = a.insertionLoc(b.first)
        if (splitLoc < a.size) {
          val aLower = a.islice(0, splitLoc)
          val aUpper = a.islice(splitLoc, a.size)
          unionList += aLower
          unionList += b
          indicesPQ.enqueue(aUpper)
        } else {
          unionList += a
          unionList += b
        }
      }
    }

    val simplified = simplify(unionList.toArray)
    
    new HybridDateTimeIndex(simplified)
  }
}
