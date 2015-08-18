package com.cloudera.finance.parsers

import org.joda.time.format.DateTimeFormatter

import scala.util.control.NonFatal

trait Parser {
  protected val dateTimeFormatter: DateTimeFormatter

  // if we're missing or have bad data, use Double.NaN
  protected def parseDouble(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case NonFatal(_) => Double.NaN
    }
  }
}
