package com.github.skyrylyuk.sensoranalyzer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SensorAnalyzerApplication {

    @Autowired
	Worker worker;

	public static void main(String[] args) {
		SpringApplication.run(SensorAnalyzerApplication.class, args);
	}

}
