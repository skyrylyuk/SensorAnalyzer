package com.github.skyrylyuk.sensoranalyzer;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Project
 * Created by skyrylyuk on 2020-03-02.
 */

@Configuration
public class ApplicationConfig {

    @Value("${app.name:jigsaw}")
    private String appName;

    @Value("${spark.home}")
    private String sparkHome;

    @Value("${master.uri:local}")
    private String masterUri;

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
                .setAppName(appName)
                .setSparkHome(sparkHome)
                .setMaster(masterUri);
    }

    @Bean
    public SparkSession javaSparkSession(SparkConf conf) {
        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }
}
