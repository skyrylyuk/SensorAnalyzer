package com.github.skyrylyuk.sensoranalyzer.util;

import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Project
 * Created by skyrylyuk on 2020-03-06.
 */
public class TimestampUtil {
    public static Timestamp nulling(Timestamp t){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(t);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);

        return Timestamp.from(calendar.toInstant());
    }
}
