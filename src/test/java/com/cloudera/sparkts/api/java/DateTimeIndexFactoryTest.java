package com.cloudera.sparkts.api.java;

import com.cloudera.sparkts.BusinessDayFrequency;
import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.DayFrequency;
import java.time.*;

import org.threeten.extra.Interval;
import scala.runtime.RichInt;
import org.junit.Test;

import static com.cloudera.sparkts.api.java.DateTimeIndexFactory.*;
import static org.junit.Assert.assertEquals;

public class DateTimeIndexFactoryTest {
    
    private static ZoneId UTC = ZoneId.of("Z");
    
    @Test
    public void testToFromString() {
        DateTimeIndex uniformIndex = DateTimeIndexFactory.uniform(
          ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, ZoneId.systemDefault()),
          5,
          new BusinessDayFrequency(2, DayOfWeek.MONDAY.getValue()));
        String uniformStr = uniformIndex.toString();
        assertEquals(fromString(uniformStr), uniformIndex);

        DateTimeIndex irregularIndex = DateTimeIndexFactory.irregular(new ZonedDateTime[]{
          ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, ZoneId.systemDefault()),
          ZonedDateTime.of(1990, 4, 12, 0, 0, 0, 0, ZoneId.systemDefault()),
          ZonedDateTime.of(1990, 4, 13, 0, 0, 0, 0, ZoneId.systemDefault())
        });
        String irregularStr = irregularIndex.toString();
        assertEquals(fromString(irregularStr), irregularIndex);

        DateTimeIndex hybridIndex = hybrid(new DateTimeIndex[] {
                uniformIndex, irregularIndex});
        String hybridStr = hybridIndex.toString();
        assertEquals(fromString(hybridStr), hybridIndex);
    }

    @Test
    public void testUniform() {
        DateTimeIndex index = DateTimeIndexFactory.uniform(
          ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, UTC),
          5,
          new DayFrequency(2));
        assertEquals(index.size(), 5);
        assertEquals(index.first(), ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, UTC));
        assertEquals(index.last(), ZonedDateTime.of(2015, 4, 18, 0, 0, 0, 0, UTC));

        verifySliceUniform(index.slice(ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC),
          ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC)));
        verifySliceUniform(index.slice(Interval.of(
          ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC).toInstant(),
          ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC).toInstant())));
        verifySliceUniform(index.islice(2, 4));
        verifySliceUniform(index.islice(new RichInt(2).until(4)));
        verifySliceUniform(index.islice(new RichInt(2).to(3)));
    }

    private void verifySliceUniform(DateTimeIndex index) {
        assertEquals(index.size(), 2);
        assertEquals(index.first(), ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC));
        assertEquals(index.last(), ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC));
    }

    @Test
    public void testIrregular() {
        DateTimeIndex index = DateTimeIndexFactory.irregular(new ZonedDateTime[]{
                ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC),
                ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, UTC),
                ZonedDateTime.of(2015, 4, 17, 0, 0, 0, 0, UTC),
                ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, UTC),
                ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, UTC)
        });
        assertEquals(index.size(), 5);
        assertEquals(index.first(), ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC));
        assertEquals(index.last(), ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, UTC));

        verifySliceIrregular(index.slice(ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, UTC),
                ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, UTC)));
        verifySliceIrregular(index.slice(
                Interval.of(
                        ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, UTC).toInstant(),
                        ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, UTC).toInstant())));
        verifySliceIrregular(index.islice(1, 4));
        verifySliceIrregular(index.islice(new RichInt(1).until(4)));
        verifySliceIrregular(index.islice(new RichInt(1).to(3)));

        // TODO: test bounds that aren't members of the index
    }

    private void verifySliceIrregular(DateTimeIndex index) {
        assertEquals(index.size(), 3);
        assertEquals(index.first(), ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, UTC));
        assertEquals(index.last(), ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, UTC));
    }

    @Test
    public void testHybrid() {
        DateTimeIndex index1 = uniform(zonedDateTime("2015-04-10", UTC), 5,
                new DayFrequency(2), UTC);
        DateTimeIndex index2 = irregular(new ZonedDateTime[]{
                zonedDateTime("2015-04-19", UTC),
                zonedDateTime("2015-04-20", UTC),
                zonedDateTime("2015-04-21", UTC),
                zonedDateTime("2015-04-25", UTC),
                zonedDateTime("2015-04-28", UTC)
        }, UTC);
        DateTimeIndex index3 = uniform(zonedDateTime("2015-05-10", UTC), 5,
                new DayFrequency(2), UTC);

        DateTimeIndex index = hybrid(new DateTimeIndex[]{index1, index2, index3}, UTC);

        assertEquals(index.size(), 15);
        assertEquals(index.first(), zonedDateTime("2015-04-10", UTC));
        assertEquals(index.last(), zonedDateTime("2015-05-18", UTC));

        verifySlice1(index.slice(zonedDateTime("2015-04-14", UTC),
                zonedDateTime("2015-04-16", UTC)));
        verifySlice1(index.slice(Interval.of(
                zonedDateTime("2015-04-14", UTC).toInstant(),
                zonedDateTime("2015-04-16", UTC).toInstant())));
        verifySlice1(index.islice(2, 4));
        verifySlice1(index.islice(new RichInt(2).until(4)));
        verifySlice1(index.islice(new RichInt(2).to(3)));

        verifySlice2(index.slice(zonedDateTime("2015-04-20", UTC),
                zonedDateTime("2015-04-25", UTC)));
        verifySlice2(index.slice(Interval.of(
                zonedDateTime("2015-04-20", UTC).toInstant(),
                zonedDateTime("2015-04-25", UTC).toInstant())));
        verifySlice2(index.islice(6, 9));
        verifySlice2(index.islice(new RichInt(6).until(9)));
        verifySlice2(index.islice(new RichInt(6).to(8)));

        verifySlice3(index.slice(zonedDateTime("2015-04-16", UTC),
                zonedDateTime("2015-05-16", UTC)));
        verifySlice3(index.slice(Interval.of(
                zonedDateTime("2015-04-16", UTC).toInstant(),
                zonedDateTime("2015-05-16", UTC).toInstant())));
        verifySlice3(index.islice(3, 14));
        verifySlice3(index.islice(new RichInt(3).until(14)));
        verifySlice3(index.islice(new RichInt(3).to(13)));

        assertEquals(index.dateTimeAtLoc(0), zonedDateTime("2015-04-10", UTC));
        assertEquals(index.dateTimeAtLoc(4), zonedDateTime("2015-04-18", UTC));
        assertEquals(index.dateTimeAtLoc(5), zonedDateTime("2015-04-19", UTC));
        assertEquals(index.dateTimeAtLoc(7), zonedDateTime("2015-04-21", UTC));
        assertEquals(index.dateTimeAtLoc(9), zonedDateTime("2015-04-28", UTC));
        assertEquals(index.dateTimeAtLoc(10), zonedDateTime("2015-05-10", UTC));
        assertEquals(index.dateTimeAtLoc(14), zonedDateTime("2015-05-18", UTC));

        assertEquals(index.locAtDateTime(zonedDateTime("2015-04-10", UTC)), 0);
        assertEquals(index.locAtDateTime(zonedDateTime("2015-04-18", UTC)), 4);
        assertEquals(index.locAtDateTime(zonedDateTime("2015-04-19", UTC)), 5);
        assertEquals(index.locAtDateTime(zonedDateTime("2015-04-21", UTC)), 7);
        assertEquals(index.locAtDateTime(zonedDateTime("2015-04-28", UTC)), 9);
        assertEquals(index.locAtDateTime(zonedDateTime("2015-05-10", UTC)), 10);
        assertEquals(index.locAtDateTime(zonedDateTime("2015-05-18", UTC)), 14);
    }

    private void verifySlice1(DateTimeIndex index) {
        assertEquals(index.size(), 2);
        assertEquals(index.first(), zonedDateTime("2015-04-14", UTC));
        assertEquals(index.last(), zonedDateTime("2015-04-16", UTC));
    }

    private void verifySlice2(DateTimeIndex index) {
        assertEquals(index.size(), 3);
        assertEquals(index.first(), zonedDateTime("2015-04-20", UTC));
        assertEquals(index.last(), zonedDateTime("2015-04-25", UTC));
    }

    private void verifySlice3(DateTimeIndex index) {
        assertEquals(index.size(), 11);
        assertEquals(index.first(), zonedDateTime("2015-04-16", UTC));
        assertEquals(index.last(), zonedDateTime("2015-05-16", UTC));
    }
    
    private static ZonedDateTime zonedDateTime(String dt, ZoneId zone) {
        String[] tokens = dt.split("-");
        return ZonedDateTime.of(
                Integer.parseInt(tokens[0]),
                Integer.parseInt(tokens[1]),
                Integer.parseInt(tokens[2]),
                0, 0, 0, 0, zone
        );
    }
}
