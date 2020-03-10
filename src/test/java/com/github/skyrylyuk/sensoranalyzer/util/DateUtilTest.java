package com.github.skyrylyuk.sensoranalyzer.util;

import com.google.common.collect.Iterables;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Project
 * Created by skyrylyuk on 3/9/20.
 */
class DateUtilTest {

    @DisplayName("The 'nulling' should reset values of minutes, seconds and millis to zero")
    @Test
    void shouldGenerateRangeTimeIntervals1() {
        LocalDate start = LocalDate.parse("2018-09-01");
        LocalDate end = LocalDate.parse("2018-09-03");

        final List<LocalDateTime> datesBetween = DateUtil.getDatesBetween(start, end);
        assertThat(datesBetween, hasSize(288));
        assertThat(Iterables.getFirst(datesBetween, null), equalTo(LocalDateTime.parse("2018-09-01T00:00:00.00")));
        assertThat(Iterables.getLast(datesBetween), equalTo(LocalDateTime.parse("2018-09-03T23:45:00.00")));
    }
}