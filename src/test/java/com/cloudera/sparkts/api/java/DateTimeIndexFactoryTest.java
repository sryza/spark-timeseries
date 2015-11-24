package com.cloudera.sparkts.api.java;

import com.cloudera.sparkts.BusinessDayFrequency;
import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.DayFrequency;
import java.time.*;

import org.threeten.extra.Interval;
import scala.runtime.RichInt;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class DateTimeIndexFactoryTest {
    @Test
    public void testToFromString() {
        DateTimeIndex uniformIndex = DateTimeIndexFactory.uniform(
          ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, ZoneId.systemDefault()),
          5,
          new BusinessDayFrequency(2, DayOfWeek.MONDAY.getValue()));
        String uniformStr = uniformIndex.toString();
        assertEquals(DateTimeIndexFactory.fromString(uniformStr), uniformIndex);

        DateTimeIndex irregularIndex = DateTimeIndexFactory.irregular(new ZonedDateTime[]{
          ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, ZoneId.systemDefault()),
          ZonedDateTime.of(1990, 4, 12, 0, 0, 0, 0, ZoneId.systemDefault()),
          ZonedDateTime.of(1990, 4, 13, 0, 0, 0, 0, ZoneId.systemDefault())
        });
        String irregularStr = irregularIndex.toString();
        assertEquals(DateTimeIndexFactory.fromString(irregularStr), irregularIndex);
    }

    @Test
    public void testUniform() {
        DateTimeIndex index = DateTimeIndexFactory.uniform(
          ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, ZoneId.of("Z")),
          5,
          new DayFrequency(2));
        assertEquals(index.size(), 5);
        assertEquals(index.first(), ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, ZoneId.of("Z")));
        assertEquals(index.last(), ZonedDateTime.of(2015, 4, 18, 0, 0, 0, 0, ZoneId.of("Z")));

        verifySliceUniform(index.slice(ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.of("Z")),
          ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, ZoneId.of("Z"))));
        verifySliceUniform(index.slice(Interval.of(
          ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.of("Z")).toInstant(),
          ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, ZoneId.of("Z")).toInstant())));
        verifySliceUniform(index.islice(2, 4));
        verifySliceUniform(index.islice(new RichInt(2).until(4)));
        verifySliceUniform(index.islice(new RichInt(2).to(3)));
    }

    private void verifySliceUniform(DateTimeIndex index) {
        assertEquals(index.size(), 2);
        assertEquals(index.first(), ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.of("Z")));
        assertEquals(index.last(), ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, ZoneId.of("Z")));
    }

    @Test
    public void testIrregular() {
        DateTimeIndex index = DateTimeIndexFactory.irregular(new ZonedDateTime[]{
                ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.of("Z")),
                ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, ZoneId.of("Z")),
                ZonedDateTime.of(2015, 4, 17, 0, 0, 0, 0, ZoneId.of("Z")),
                ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, ZoneId.of("Z")),
                ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, ZoneId.of("Z"))
        });
        assertEquals(index.size(), 5);
        assertEquals(index.first(), ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.of("Z")));
        assertEquals(index.last(), ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, ZoneId.of("Z")));

        verifySliceIrregular(index.slice(ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, ZoneId.of("Z")),
                ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, ZoneId.of("Z"))));
        verifySliceIrregular(index.slice(
                Interval.of(
                        ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, ZoneId.of("Z")).toInstant(),
                        ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, ZoneId.of("Z")).toInstant())));
        verifySliceIrregular(index.islice(1, 4));
        verifySliceIrregular(index.islice(new RichInt(1).until(4)));
        verifySliceIrregular(index.islice(new RichInt(1).to(3)));

        // TODO: test bounds that aren't members of the index
    }

    private void verifySliceIrregular(DateTimeIndex index) {
        assertEquals(index.size(), 3);
        assertEquals(index.first(), ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, ZoneId.of("Z")));
        assertEquals(index.last(), ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, ZoneId.of("Z")));
    }
}
