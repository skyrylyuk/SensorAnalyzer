package com.github.skyrylyuk.sensoranalyzer.util;

import com.github.skyrylyuk.sensoranalyzer.StoreStruct;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Project
 * Created by skyrylyuk on 2020-03-05.
 */
@Component
public class StoreUtil {

    private static final int MIN_PARTITIONS_COUNT = 1;
    private final SparkSession session;

    @Autowired
    public StoreUtil(SparkSession session) {
        this.session = session;
    }

    public Dataset<Row> readCSV(String path, StructType schema) throws FileNotFoundException {
        final URL url = ResourceUtils.getURL(path);
        return session.read()
                .schema(schema)
                .csv(url.getPath());
    }

    public void writeJSON(String path, Dataset<Row> df) {
        df.coalesce(1).write()
                .mode(SaveMode.Overwrite)
                .json(path);
    }

    public JavaRDD<Row> readJSON(String path){
        return session.read().json(path).toJavaRDD();
    }

    public String readSQL(String path) throws IOException {
        File file = ResourceUtils.getFile("classpath:" + path);

        final byte[] bytes = Files.readAllBytes(file.toPath());
        return new String(bytes);
    }

    public <T> Dataset<Row> readList(List<T> list, Class<T> clazz){
        final List<Row> l = list.stream().map(RowFactory::create).collect(Collectors.toList());
        return session.createDataFrame(l, StoreStruct.TIMESTAMP_SCHEMA);
    }

}
