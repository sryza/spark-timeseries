package com.cloudera.sparkts.api.java;

import com.cloudera.sparkts.BusinessDayFrequency;
import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.DayFrequency;
import com.github.nscala_time.time.RichReadableInstant;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import scala.runtime.RichInt;

import static org.junit.Assert.assertEquals;

public class DateTimeIndexFactoryTest {
    @Test
    public void testToFromString() {
        DateTimeIndex uniformIndex = DateTimeIndexFactory.uniform(new DateTime("1990-04-10"),
                5, new BusinessDayFrequency(2, DateTimeConstants.MONDAY));
        String uniformStr = uniformIndex.toString();
        assertEquals(DateTimeIndexFactory.fromString(uniformStr), uniformIndex);

        DateTimeIndex irregularIndex = DateTimeIndexFactory.irregular(new DateTime[]{
                new DateTime("1990-04-10"), new DateTime("1990-04-12"), new DateTime("1990-04-13")});
        String irregularStr = irregularIndex.toString();
        assertEquals(DateTimeIndexFactory.fromString(irregularStr), irregularIndex);
    }

    @Test
    public void testUniform() {
        DateTimeIndex index = DateTimeIndexFactory.uniform(
                new DateTime("2015-04-10", DateTimeZone.UTC), 5, new DayFrequency(2));
        assertEquals(index.size(), 5);
        assertEquals(index.first(), new DateTime("2015-04-10", DateTimeZone.UTC));
        assertEquals(index.last(), new DateTime("2015-04-18", DateTimeZone.UTC));

        verifySliceUniform(index.slice(new DateTime("2015-04-14", DateTimeZone.UTC), new DateTime("2015-04-16", DateTimeZone.UTC)));
        verifySliceUniform(index.slice(new RichReadableInstant(new DateTime("2015-04-14", DateTimeZone.UTC))
                .to(new DateTime("2015-04-16", DateTimeZone.UTC))));
        verifySliceUniform(index.islice(2, 4));
        verifySliceUniform(index.islice(new RichInt(2).until(4)));
        verifySliceUniform(index.islice(new RichInt(2).to(3)));
    }

    private void verifySliceUniform(DateTimeIndex index) {
        assertEquals(index.size(), 2);
        assertEquals(index.first(), new DateTime("2015-04-14", DateTimeZone.UTC));
        assertEquals(index.last(), new DateTime("2015-04-16", DateTimeZone.UTC));
    }

    @Test
    public void testIrregular() {
        DateTimeIndex index = DateTimeIndexFactory.irregular(new DateTime[]{
                new DateTime("2015-04-14", DateTimeZone.UTC),
                new DateTime("2015-04-15", DateTimeZone.UTC),
                new DateTime("2015-04-17", DateTimeZone.UTC),
                new DateTime("2015-04-22", DateTimeZone.UTC),
                new DateTime("2015-04-25", DateTimeZone.UTC)
        });
        assertEquals(index.size(), 5);
        assertEquals(index.first(), new DateTime("2015-04-14", DateTimeZone.UTC));
        assertEquals(index.last(), new DateTime("2015-04-25", DateTimeZone.UTC));

        verifySliceIrregular(index.slice(new DateTime("2015-04-15", DateTimeZone.UTC), new DateTime("2015-04-22", DateTimeZone.UTC)));
        verifySliceIrregular(index.slice(new RichReadableInstant(new DateTime("2015-04-15", DateTimeZone.UTC))
                .to(new DateTime("2015-04-22", DateTimeZone.UTC))));
        verifySliceIrregular(index.islice(1, 4));
        verifySliceIrregular(index.islice(new RichInt(1).until(4)));
        verifySliceIrregular(index.islice(new RichInt(1).to(3)));

        // TODO: test bounds that aren't members of the index
    }

    private void verifySliceIrregular(DateTimeIndex index) {
        assertEquals(index.size(), 3);
        assertEquals(index.first(), new DateTime("2015-04-15", DateTimeZone.UTC));
        assertEquals(index.last(), new DateTime("2015-04-22", DateTimeZone.UTC));
    }
}
