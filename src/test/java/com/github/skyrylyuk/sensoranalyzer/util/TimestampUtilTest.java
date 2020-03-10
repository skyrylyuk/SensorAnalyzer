package com.github.skyrylyuk.sensoranalyzer.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Project
 * Created by skyrylyuk on 2020-03-07.
 */
class TimestampUtilTest {

    @DisplayName("The 'nulling' should reset values of minutes, seconds and millis to zero")
    @Test
    void shouldNullingMinutesSecondMillis() {
        final Timestamp input = Timestamp.valueOf(LocalDateTime.parse("2007-12-03T10:15:30"));
        final Timestamp expect = Timestamp.valueOf(LocalDateTime.parse("2007-12-03T10:00:00"));

        final Timestamp actual = TimestampUtil.nulling(input);

        assertThat(actual, is(expect));
    }

    @DisplayName("The 'nulling' should do nothing if value already has zero value in minutes, seconds and millis")
    @Test
    void shouldNullingCorrectHandlingAlreadyNulled() {
        final Timestamp input = Timestamp.valueOf(LocalDateTime.parse("2007-12-03T10:00:00"));
        final Timestamp expect = Timestamp.valueOf(LocalDateTime.parse("2007-12-03T10:00:00"));

        final Timestamp actual = TimestampUtil.nulling(input);

        assertThat(actual, is(expect));
    }

    @DisplayName("The 'nulling' should allays round value to down")
    @Test
    void shouldNullingRoundToDown() {
        final Timestamp input = Timestamp.valueOf(LocalDateTime.parse("2007-12-03T10:55:00"));
        final Timestamp expect = Timestamp.valueOf(LocalDateTime.parse("2007-12-03T10:00:00"));

        final Timestamp actual = TimestampUtil.nulling(input);

        assertThat(actual, is(expect));
    }

    @DisplayName("The 'nulling' should not change date for 'Midnight' (00:00:00)")
    @Test
    void shouldNullingHandlingOfMidnight() {
        final Timestamp input = Timestamp.valueOf(LocalDateTime.parse("2007-12-03T00:00:00"));
        final Timestamp expect = Timestamp.valueOf(LocalDateTime.parse("2007-12-03T00:00:00"));

        final Timestamp actual = TimestampUtil.nulling(input);

        assertThat(actual, is(expect));
    }
}