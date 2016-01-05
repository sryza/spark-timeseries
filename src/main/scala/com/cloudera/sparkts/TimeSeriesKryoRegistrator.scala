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

import com.esotericsoftware.kryo.{Serializer, Kryo}
import com.esotericsoftware.kryo.io.{Output, Input}

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import com.cloudera.sparkts.TimeSeriesUtils._

import java.time._

class TimeSeriesKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[TimeSeries[_]])
    kryo.register(classOf[UniformDateTimeIndex])
    kryo.register(classOf[IrregularDateTimeIndex])
    kryo.register(classOf[BusinessDayFrequency])
    kryo.register(classOf[DayFrequency])
    kryo.register(classOf[ZonedDateTime], new DateTimeSerializer)
  }
}

class DateTimeSerializer extends Serializer[ZonedDateTime] {
  def write(kryo: Kryo, out: Output, dt: ZonedDateTime): Unit = {
    out.writeLong(zonedDateTimeToLong(dt), true)
  }

  def read(kryo: Kryo, in: Input, clazz: Class[ZonedDateTime]): ZonedDateTime = {
    longToZonedDateTime(in.readLong(true), ZoneId.systemDefault())
  }
}

object TimeSeriesKryoRegistrator {
  def registerKryoClasses(conf: SparkConf): Unit = {
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[TimeSeriesKryoRegistrator].getName)
  }
}
