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

package com.cloudera.sparkts.api.java;

import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.UniformDateTimeIndex;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag$;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public final class JavaTimeSeriesFactory {
    private static final JavaTimeSeries$ JAVA_TIME_SERIES = JavaTimeSeries$.MODULE$;

    private JavaTimeSeriesFactory() {}

    public static <K> JavaTimeSeries<K> javaTimeSeriesFromIrregularSamples(
            List<Tuple2<ZonedDateTime, double[]>> samples,
            K[] keys,
            ZoneId zone) {
        return JAVA_TIME_SERIES.javaTimeSeriesFromIrregularSamples(
                JavaConversions.asScalaBuffer(samples),
                keys,
                zone != null ? zone : ZoneId.systemDefault(),
                ClassTag$.MODULE$.<K>apply(keys[0].getClass()));
    }

    /**
     * This function should only be called when you can safely make the assumption that the time
     * samples are uniform (monotonously increasing) across time.
     */
    public static <K> JavaTimeSeries<K> javaTimeSeriesFromUniformSamples(
            List<double[]> samples,
            UniformDateTimeIndex index,
            K[] keys) {
        return JAVA_TIME_SERIES.javaTimeSeriesFromUniformSamples(
                JavaConversions.asScalaBuffer(samples),
                index,
                keys,
                ClassTag$.MODULE$.<K>apply(keys[0].getClass()));
    }

    public static <K> JavaTimeSeries<K> javaTimeSeriesFromVectors(
            Iterable<Vector> samples,
            DateTimeIndex index,
            K[] keys) {
        return JAVA_TIME_SERIES.javaTimeSeriesFromVectors(
                JavaConversions.iterableAsScalaIterable(samples),
                index,
                keys,
                ClassTag$.MODULE$.<K>apply(keys[0].getClass()));
    }
}
