package com.github.leegphillips.mongex.dataLayer.dao;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(MockitoJUnitRunner.class)
public class TimeFrameTest {
    @Test
    public void generalTestCases() {
        assertNotEquals(TimeFrame.EIGHT_HOURS, TimeFrame.FOUR_HOURS);
        assertEquals(TimeFrame.EIGHT_HOURS, TimeFrame.get(TimeFrame.EIGHT_HOURS.getLabel()));
        assertNotEquals(TimeFrame.EIGHT_HOURS, TimeFrame.get(TimeFrame.FIFTEEN_MINUTES.getLabel()));
        assertEquals(TimeFrame.EIGHT_HOURS, TimeFrame.EIGHT_HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNonExisting() {
        TimeFrame.get("13h");
    }

    @Test(expected = NullPointerException.class)
    public void cannotCreateWithNull() {
        TimeFrame.get(null);
    }
}
