package com.github.skyrylyuk.sensoranalyzer.util;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Project
 * Created by skyrylyuk on 3/9/20.
 */
public class DateUtil {

    public static final int MINUTES_INTERVAL = 15;

    public static List<Timestamp> getDatesBetween(LocalDate startDate, LocalDate endDate) {

        if (startDate != null && !startDate.isAfter(endDate)) {
            LocalDateTime s = LocalDateTime.of(startDate, LocalTime.MIDNIGHT);
            System.out.println("s = " + s);
            final LocalDateTime e = LocalDateTime.of(endDate.plusDays(1), LocalTime.MIDNIGHT);
            System.out.println("e = " + e);

            List<Timestamp> times = new ArrayList<>();
            while (s.isBefore(e) ){
                times.add(Timestamp.valueOf(s));
                s = s.plusMinutes(MINUTES_INTERVAL);
            }
            return times;
        } else {
            return Collections.emptyList();
        }
    }

}
