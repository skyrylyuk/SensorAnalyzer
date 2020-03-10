package com.github.skyrylyuk.sensoranalyzer;

import com.github.skyrylyuk.sensoranalyzer.util.StoreUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

import static com.github.skyrylyuk.sensoranalyzer.util.DoubleUtil.average;
import static com.github.skyrylyuk.sensoranalyzer.util.DoubleUtil.formatDouble;
import static com.github.skyrylyuk.sensoranalyzer.util.TimestampUtil.nulling;

/**
 * Project
 * Created by skyrylyuk on 2020-03-02.
 */

@Service
public class Analyzer {

    private final StoreUtil storeUtil;

    private final SparkSession session;

    private static final Gson GSON = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'hh:mm:ss").create();


    @Autowired
    public Analyzer(StoreUtil storeUtil, SparkSession session) {
        this.storeUtil = storeUtil;
        this.session = session;
    }

    Dataset<Row> executePhase1(Dataset<Row> dates, Dataset<Row> values, Dataset<Row> locations) throws IOException {
        dates.createOrReplaceTempView("dates");
        values.createOrReplaceTempView("values");
        locations.createOrReplaceTempView("locations");

        final String sql = storeUtil.readSQL("query/spark_sql_query_second.sql");

        return session.sql(sql);
    }

    JavaRDD<String> executePhase2(JavaRDD<Row> rdd) {
        return rdd.keyBy(grouper)
                .groupByKey()
                .values()
                .map(reducer)
                .sortBy(sorter, true, 1)
                .map(toJson);
    }

    private final static Function<Row, String> grouper = (Function<Row, String>) x -> x.getString(x.fieldIndex("TimeSlotStart")).substring(0, 13) + x.getString(x.fieldIndex("Location"));

    private final static Function<Iterable<Row>, Result> reducer = (Function<Iterable<Row>, Result>) iterable -> {
        Aggregator agg = null;
        if (iterable != null && iterable.iterator().hasNext()) {
            final Row first = iterable.iterator().next();

            final int timeSlotStartIndex = first.fieldIndex("TimeSlotStart");

            final int locationIndex = first.fieldIndex("Location");

            final int tempMinIndex = first.fieldIndex("TempMin");
            final int tempMaxIndex = first.fieldIndex("TempMax");
            final int tempAvgIndex = first.fieldIndex("TempAvg");
            final int tempCntIndex = first.fieldIndex("TempCnt");

            final int presenceIndex = first.fieldIndex("Presence");
            final int presenceCntIndex = first.fieldIndex("PresenceCnt");

            for (Row row : iterable) {
                if (!row.isNullAt(timeSlotStartIndex)) {
                    agg = new Aggregator();
                    final String timeSlotStart = row.getString(timeSlotStartIndex);
                    agg.timestamp = Timestamp.valueOf(LocalDateTime.parse(timeSlotStart));
                }

                if (!row.isNullAt(tempMinIndex)) {
                    agg.location = row.getString(locationIndex);
                }

                if (!row.isNullAt(tempMinIndex)) {
                    agg.tempMinCounter.incrementAndGet();
                    agg.tempMinValues.add(Double.parseDouble(row.getString(tempMinIndex)));
                }
                if (!row.isNullAt(tempMaxIndex)) {
                    agg.tempMaxCounter.incrementAndGet();
                    agg.tempMaxValues.add(Double.parseDouble(row.getString(tempMaxIndex)));
                }
                if (!row.isNullAt(tempAvgIndex)) {
                    agg.tempAvgCounter.incrementAndGet();
                    agg.tempAvgValues.add(Double.parseDouble(row.getString(tempAvgIndex)));
                }


                if (!row.isNullAt(tempCntIndex)) {
                    agg.tempCnt = agg.tempCnt != null ? agg.tempCnt + row.getLong(tempCntIndex) : row.getLong(tempCntIndex);
                }

                if (!row.isNullAt(presenceIndex)) {
                    agg.presence = agg.presence || row.getBoolean(presenceIndex);
                }
                if (!row.isNullAt(presenceCntIndex)) {
                    agg.presenceCnt += row.getLong(presenceCntIndex);
                }
            }
        }


        if (agg != null) {

            return new Result(
                    nulling(agg.timestamp),
                    agg.location,
                    formatDouble(average(agg.tempMinValues, agg.tempMinCounter)),
                    formatDouble(average(agg.tempMaxValues, agg.tempMaxCounter)),
                    formatDouble(average(agg.tempAvgValues, agg.tempAvgCounter)),
                    agg.tempCnt,
                    agg.presence,
                    agg.presenceCnt
            );
        } else {
            return null;
        }
    };

    private final static Function<Result, Timestamp> sorter = (Function<Result, Timestamp>) v -> v.timestamp;

    private final static Function<Result, String> toJson = (Function<Result, String>) row -> GSON.toJson(row);

    static class Aggregator {
        Timestamp timestamp;
        String location;
        AtomicLong tempMinCounter = new AtomicLong();
        DoubleAdder tempMinValues = new DoubleAdder();
        AtomicLong tempMaxCounter = new AtomicLong();
        DoubleAdder tempMaxValues = new DoubleAdder();
        AtomicLong tempAvgCounter = new AtomicLong();
        DoubleAdder tempAvgValues = new DoubleAdder();
        Long tempCnt;
        boolean presence;
        long presenceCnt;
    }


    static class Result implements Serializable {
        Timestamp timestamp;
        String location;
        String tempMinValues;
        String tempMaxValues;
        String tempAvgValues;
        Long tempCnt;
        boolean presence;
        long presenceCnt;

        public Result(Timestamp timestamp,
                      String location,
                      String tempMinValues, String tempMaxValues, String tempAvgValues, Long tempCnt,
                      boolean presence, long presenceCnt) {
            this.timestamp = timestamp;
            this.location = location;
            this.tempMinValues = tempMinValues;
            this.tempMaxValues = tempMaxValues;
            this.tempAvgValues = tempAvgValues;
            this.tempCnt = tempCnt;
            this.presence = presence;
            this.presenceCnt = presenceCnt;
        }
    }


}
