package com.github.skyrylyuk.sensoranalyzer.util;

import com.github.skyrylyuk.sensoranalyzer.Analyzer;
import com.github.skyrylyuk.sensoranalyzer.ApplicationConfig;
import com.github.skyrylyuk.sensoranalyzer.StoreStruct;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.FileNotFoundException;

import static org.junit.jupiter.api.Assertions.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Project
 * Created by skyrylyuk on 2020-03-06.
 */

@SpringBootTest(classes = {ApplicationConfig.class, StoreUtil.class})
class StoreUtilTest {

    @Autowired
    StoreUtil store;

    @DisplayName("The 'readCSV' should read file return dataframe locations correct length")
    @Test
    void shouldReadDataSource1() throws FileNotFoundException {
        final Dataset<Row> dataset = store.readCSV("classpath:sample/test_ds_1.csv", StoreStruct.LOCATION_SCHEMA);


        assertThat(dataset.count(), is(6L));
        assertThat(dataset.schema().size(), is(4));
    }

    @DisplayName("The 'readCSV' should read file return dataframe values correct length")
    @Test
    void shouldReadDataSource2() throws FileNotFoundException {
        final Dataset<Row> dataset = store.readCSV("classpath:sample/test_ds_2.csv", StoreStruct.VALUE_SCHEMA);

        assertThat(dataset.count(), is(4L));
        assertThat(dataset.schema().size(), is(4));
    }
}