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

import org.apache.spark.serializer.KryoRegistrator

import org.joda.time.DateTime
import com.esotericsoftware.kryo.io.{Output, Input}

class TimeSeriesKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[TimeSeries[_]])
    kryo.register(classOf[UniformDateTimeIndex])
    kryo.register(classOf[IrregularDateTimeIndex])
    kryo.register(classOf[BusinessDayFrequency])
    kryo.register(classOf[DayFrequency])
    kryo.register(classOf[DateTime], new DateTimeSerializer)
  }
}

class DateTimeSerializer extends Serializer[DateTime] {
  def write(kryo: Kryo, out: Output, dt: DateTime) = {
    out.writeLong(dt.getMillis(), true)
  }

  def read(kryo: Kryo, in: Input, clazz: Class[DateTime]): DateTime = {
    new DateTime(in.readLong(true))
  }
}
