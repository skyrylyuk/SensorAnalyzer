package com.github.skyrylyuk.sensoranalyzer;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Project
 * Created by skyrylyuk on 2020-03-07.
 */
public class StoreStruct {

    public static final StructType LOCATION_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("location_sensor_id", DataTypes.StringType, false),
            DataTypes.createStructField("location_channel_id", DataTypes.IntegerType, false),
            DataTypes.createStructField("channel_type", DataTypes.StringType, true),
            DataTypes.createStructField("location_id", DataTypes.StringType, true)
    });

    public static final StructType VALUE_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("sensor_id", DataTypes.StringType, false),
            DataTypes.createStructField("channel_id", DataTypes.IntegerType, false),
            DataTypes.createStructField("create_time", DataTypes.TimestampType, true),
            DataTypes.createStructField("value", DataTypes.DoubleType, true)
    });

    public static final StructType TIMESTAMP_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("start_time_slot", DataTypes.TimestampType, true),
    });

    public static final StructType RESULT_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("TimeSlotStart", DataTypes.StringType, false),
            DataTypes.createStructField("Location", DataTypes.StringType, false),
            DataTypes.createStructField("TempMin", DataTypes.DoubleType, false),
            DataTypes.createStructField("TempMax", DataTypes.DoubleType, false),
            DataTypes.createStructField("TempAvg", DataTypes.DoubleType, false),
            DataTypes.createStructField("TempCnt", DataTypes.LongType, true),
            DataTypes.createStructField("Presence", DataTypes.BooleanType, true),
            DataTypes.createStructField("PresenceCnt", DataTypes.LongType, true)
    });

}
