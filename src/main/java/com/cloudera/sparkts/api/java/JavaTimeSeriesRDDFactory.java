package com.cloudera.sparkts.api.java;

import com.cloudera.sparkts.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple3;
import scala.reflect.ClassTag$;

import java.lang.reflect.Array;

public final class JavaTimeSeriesRDDFactory {
    private static final JavaTimeSeriesRDD$ JAVA_TIME_SERIES_RDD = JavaTimeSeriesRDD$.MODULE$;

    /**
     * Instantiates a JavaTimeSeriesRDD.
     *
     * @param index DateTimeIndex
     * @param seriesRDD JavaPairRDD of time series
     */
    public static <K> JavaTimeSeriesRDD<K> javaTimeSeriesRDD(
        DateTimeIndex index,
        JavaPairRDD<K, Vector> seriesRDD) {
        return JAVA_TIME_SERIES_RDD.javaTimeSeriesRDD(
                index,
                seriesRDD,
                ClassTag$.MODULE$.<K>apply(seriesRDD.first()._1().getClass()));
    }

    /**
     * Instantiates a JavaTimeSeriesRDD.
     *
     * @param targetIndex DateTimeIndex to conform all the indices to.
     * @param seriesRDD JavaRDD of time series, each with their own DateTimeIndex.
     */
    public static <K> JavaTimeSeriesRDD<K> javaTimeSeriesRDD(
        UniformDateTimeIndex targetIndex,
        JavaRDD<Tuple3<K, UniformDateTimeIndex, Vector>> seriesRDD) {
        return JAVA_TIME_SERIES_RDD.javaTimeSeriesRDD(
                targetIndex,
                seriesRDD,
                ClassTag$.MODULE$.<K>apply(seriesRDD.first()._1().getClass()));
    }

    /**
     * Instantiates a JavaTimeSeriesRDD from a JavaRDD of JavaTimeSeries.
     *
     * @param targetIndex DateTimeIndex to conform all the indices to.
     * @param seriesRDD JavaRDD of time series, each with their own DateTimeIndex.
     */
    public static <K> JavaTimeSeriesRDD<K> javaTimeSeriesRDD(
        DateTimeIndex targetIndex,
        JavaRDD<JavaTimeSeries<K>> seriesRDD) {
        java.lang.reflect.Array.class.cast(null);
        return JAVA_TIME_SERIES_RDD.javaTimeSeriesRDD(
                targetIndex,
                seriesRDD,
                ClassTag$.MODULE$.<K>apply(((K[]) seriesRDD.first().keys())[0].getClass()));
    }

    /**
     * Instantiates a JavaTimeSeriesRDD from a DataFrame of observations.
     *
     * @param targetIndex DateTimeIndex to conform all the series to.
     * @param df The DataFrame.
     * @param tsCol The Timestamp column telling when the observation occurred.
     * @param keyCol The string column labeling which string key the observation belongs to..
     * @param valueCol The observed value..
     */
    public static JavaTimeSeriesRDD<String> javaTimeSeriesRDDFromObservations(
            DateTimeIndex targetIndex,
            DataFrame df,
            String tsCol,
            String keyCol,
            String valueCol) {
        return JAVA_TIME_SERIES_RDD.javaTimeSeriesRDDFromObservations(
                targetIndex,
                df,
                tsCol,
                keyCol,
                valueCol);
    }

    /**
     * Loads a JavaTimeSeriesRDD from a directory containing a set of CSV files and a date-time index.
     */
    public static JavaTimeSeriesRDD<String> javaTimeSeriesRDDFromCsv(String path, JavaSparkContext sc) {
        return JAVA_TIME_SERIES_RDD.javaTimeSeriesRDDFromCsv(path, sc);
    }

    /**
     * Creates a TimeSeriesRDD from rows in a binary format that Python can write to.
     * Not a public API. For use only by the Python API.
     */
    public static JavaTimeSeriesRDD<String> javaTimeSeriesRDDFromPython(DateTimeIndex index,
        JavaRDD<byte[]> pyRdd) {
        return JAVA_TIME_SERIES_RDD.javaTimeSeriesRDDFromPython(index, pyRdd);
    }
}
