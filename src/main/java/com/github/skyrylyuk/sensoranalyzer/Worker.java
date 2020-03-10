package com.github.skyrylyuk.sensoranalyzer;

import com.github.skyrylyuk.sensoranalyzer.util.DateUtil;
import com.github.skyrylyuk.sensoranalyzer.util.StoreUtil;
import org.apache.commons.cli.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

//import org.apache.spark.sql.expressions;

/**
 * Project
 * Created by skyrylyuk on 2020-03-02.
 */

@Service
public class Worker implements ApplicationRunner {
    private Logger log = LoggerFactory.getLogger(Worker.class);

    @Value("${store.input.values}")
    private String valuesUri;
    @Value("${store.input.locations}")
    private String locationsUri;
    @Value("${store.output.result1}")
    private String intermediateStorePath;
    @Value("${store.output.result2}")
    private String totalStorePath;

    private final Analyzer analyzer;
    private final StoreUtil store;

    @Autowired
    public Worker(Analyzer analyzer, StoreUtil store) {
        this.analyzer = analyzer;
        this.store = store;
    }

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {

        System.out.println("args = " + applicationArguments);

        final Map<String, String> arguments = Arrays.stream(applicationArguments.getSourceArgs())
                .map(s -> s.replace("--", "").split("="))
                .collect(Collectors.toMap(e -> e[0], e -> e[1]));

        LocalDate start = LocalDate.parse(arguments.getOrDefault("start", "2018-03-23"));
        LocalDate end = LocalDate.parse(arguments.getOrDefault("start", "2018-03-23"));

        final Dataset<Row> dates = store.readList(DateUtil.getDatesBetween(start, end), Timestamp.class);
        log.info("==> dates has size {}", dates.count());

        final Dataset<Row> values = store.readCSV(valuesUri, StoreStruct.VALUE_SCHEMA);
        log.info("==> values has size {}", values.count());
        final Dataset<Row> locations= store.readCSV(locationsUri, StoreStruct.LOCATION_SCHEMA);
        log.info("==> locations has size {}", locations.count());

        final Dataset<Row> phase1Results = analyzer.executePhase1(dates, values, locations);
        log.info("==> results {}", phase1Results.count());

        values.show(25, false);
        phase1Results.show(25, false);

        final String path = Path.of(intermediateStorePath).toUri().getPath();

        store.writeJSON(path, phase1Results);

        final JavaRDD<Row> rdd = store.readJSON(path);
        log.info("==> rdd size {}", rdd.collect().size());
        final JavaRDD<String> resRDD = analyzer.executePhase2(rdd);
        log.info("==> res size {}", resRDD.collect().size());

        final String pathTotal = Path.of(totalStorePath).toUri().getPath();


        resRDD.saveAsTextFile(pathTotal);

        log.info("================================================");
    }

}
