package com.github.skyrylyuk.sensoranalyzer;

import com.github.skyrylyuk.sensoranalyzer.util.StoreUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Project
 * Created by skyrylyuk on 2020-03-05.
 */

@SpringBootTest(classes = {ApplicationConfig.class, StoreUtil.class, Analyzer.class})
class AnalyzerWorkerTest implements java.io.Serializable {

    @Autowired
    Analyzer analyzer;

    @Autowired
    SparkConf conf;

    @Autowired
    SparkSession session;

    @BeforeAll
    static void init() {
        Logger.getLogger("org").setLevel(Level.ERROR);
    }


    @DisplayName("Test validation first phase of transformation")
    @Test
    void shouldExecutePhaseOne() throws IOException {

        final Dataset<Row> locations = session.createDataFrame(List.of(
                RowFactory.create("Sensor 0", 100, "temperature", "Room 0"),
                RowFactory.create("Sensor 0", 200, "battery", "Room 0"),
                RowFactory.create("Sensor 0", 300, "presence", "Room 0"),
                RowFactory.create("Sensor 1", 100, "temperature", "Room 0")
        ), StoreStruct.LOCATION_SCHEMA);

        final Dataset<Row> values = session.createDataFrame(List.of(
                RowFactory.create("Sensor 0", 100, Timestamp.valueOf("2018-03-23 11:57:56"), 65.0057662358275),
                RowFactory.create("Sensor 0", 200, Timestamp.valueOf("2018-03-23 11:57:56"), 100.0),
                RowFactory.create("Sensor 0", 300, Timestamp.valueOf("2018-03-23 11:57:56"), 0.0),
                RowFactory.create("Sensor 0", 100, Timestamp.valueOf("2018-03-23 11:58:06"), 65.23612240137348),
                RowFactory.create("Sensor 0", 100, Timestamp.valueOf("2018-03-23 11:58:11"), 65.39024481526586),
                RowFactory.create("Sensor 0", 100, Timestamp.valueOf("2018-03-23 11:58:21"), 65.39706079852459),
                RowFactory.create("Sensor 0", 200, Timestamp.valueOf("2018-03-23 11:59:56"), 19.66046443066763),
                RowFactory.create("Sensor 0", 100, Timestamp.valueOf("2018-03-23 12:33:46"), 64.47439669660929),
                RowFactory.create("Sensor 0", 100, Timestamp.valueOf("2018-03-23 12:35:06"), 65.29845202862242),
                RowFactory.create("Sensor 0", 300, Timestamp.valueOf("2018-03-23 12:47:56"), 0.0)
        ), StoreStruct.VALUE_SCHEMA);


        final Dataset<Row> actualDataset = analyzer.executePhase1(values, locations);
        assertThat(actualDataset, notNullValue());

        assertThat(Arrays.asList(actualDataset.schema().names()),
                contains("TimeSlotStart", "Location", "TempMin", "TempMax", "TempAvg", "TempCnt", "Presence", "PresenceCnt")
        );

        final List<Row> rows = actualDataset.collectAsList();
        assertThat(rows, hasSize(3));


        assertThat(rows.get(0).getAs("Location"), is("Room 0"));
        assertThat(Double.parseDouble(rows.get(0).getAs("TempMin")), closeTo(65.01, 0.01));
        assertThat(Double.parseDouble(rows.get(0).getAs("TempMax")), closeTo(65.40, 0.01));
        assertThat(Double.parseDouble(rows.get(0).getAs("TempAvg")), closeTo(65.26, 0.01));
        assertThat(rows.get(0).getAs("TempCnt"), is(4L));
        assertThat(rows.get(0).getAs("Presence"), is(true));
        assertThat(rows.get(0).getAs("PresenceCnt"), is(1L));

        assertThat(rows.get(1).getAs("Location"), is("Room 0"));
        assertThat(Double.parseDouble(rows.get(1).getAs("TempMin")), closeTo(64.47, 0.01));
        assertThat(Double.parseDouble(rows.get(1).getAs("TempMax")), closeTo(65.30, 0.01));
        assertThat(Double.parseDouble(rows.get(1).getAs("TempAvg")), closeTo(64.89, 0.01));
        assertThat(rows.get(1).getAs("TempCnt"), is(2L));
        assertThat(rows.get(1).getAs("Presence"), is(false));
        assertThat(rows.get(1).getAs("PresenceCnt"), is(0L));

        assertThat(rows.get(2).getAs("Location"), is("Room 0"));
        assertThat(rows.get(2).getAs("TempMin"), emptyString());
        assertThat(rows.get(2).getAs("TempMax"), emptyString());
        assertThat(rows.get(2).getAs("TempAvg"), emptyString());
        assertThat(rows.get(2).getAs("TempCnt"), is(0L));
        assertThat(rows.get(2).getAs("Presence"), is(true));
        assertThat(rows.get(2).getAs("PresenceCnt"), is(1L));
    }


    @DisplayName("Test validation second phase of transformation")
    @Test
    void shouldExecutePhaseTwo() {

        final List<Row> list = List.of(
                RowFactory.create(Timestamp.valueOf("2018-03-23 11:00:00"), "Room 0", 62.23, 65.75, 63.99, 2L, true, 1L),
                RowFactory.create(Timestamp.valueOf("2018-03-23 11:25:00"), "Room 0", 69.01, 75.40, 72.21, 3L, true, 1L),
                RowFactory.create(Timestamp.valueOf("2018-03-23 11:45:00"), "Room 0", 65.01, 65.40, 65.26, 4L, true, 1L),
                RowFactory.create(Timestamp.valueOf("2018-03-23 12:30:00"), "Room 0", 64.47, 65.30, 64.89, 2L, false, 0L),
                RowFactory.create(Timestamp.valueOf("2018-03-23 12:45:00"), "Room 0", null, null, null, 0L, true, 1L)
        );
        final JavaRDD<Row> rdd = JavaSparkContext.fromSparkContext(session.sparkContext()).parallelize(list);


        JavaRDD actualDataset = analyzer.executePhase2(rdd);

        assertThat(actualDataset.count(), is(2L));

        final List<Row> rows = actualDataset.collect();


        assertThat(rows.get(0).getTimestamp(0), is(Timestamp.valueOf("2018-03-23 11:00:00")));
        assertThat(rows.get(0).getString(1), is("Room 0"));
        assertThat(Double.parseDouble(rows.get(0).getString(2)), closeTo(65.42, 0.01));
        assertThat(Double.parseDouble(rows.get(0).getString(3)), closeTo(68.85, 0.01));
        assertThat(Double.parseDouble(rows.get(0).getString(4)), closeTo(67.15, 0.01));
        assertThat(rows.get(0).getInt(5), is(9));
        assertThat(rows.get(0).getBoolean(6), is(true));
        assertThat(rows.get(0).getInt(7), is(3));

        assertThat(rows.get(1).getTimestamp(0), is(Timestamp.valueOf("2018-03-23 12:00:00")));
        assertThat(rows.get(1).getString(1), is("Room 0"));
        assertThat(Double.parseDouble(rows.get(1).getString(2)), closeTo(64.47, 0.01));
        assertThat(Double.parseDouble(rows.get(1).getString(3)), closeTo(65.30, 0.01));
        assertThat(Double.parseDouble(rows.get(1).getString(4)), closeTo(64.89, 0.01));
        assertThat(rows.get(1).getInt(5), is(2));
        assertThat(rows.get(1).getBoolean(6), is(true));
        assertThat(rows.get(1).getInt(7), is(1));
    }
}


