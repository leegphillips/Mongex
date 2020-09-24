package org.github.leegphillips.mongex;

import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TickFacadeTest {

    @Mock
    CSVRecord record;

    @Test
    public void happyPath() {
        when(record.get(0)).thenReturn("43");
        when(record.get(1)).thenReturn("43");
        when(record.get(2)).thenReturn("43");

        new TickFacade(record);
    }

    @Test(expected = NullPointerException.class)
    public void cannotInstantiateWithNull() {
        new TickFacade(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bidCannotBeGreaterThanAsk() {
        when(record.get(0)).thenReturn("43");
        when(record.get(1)).thenReturn("43");
        when(record.get(2)).thenReturn("44");

        new TickFacade(record);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bidCannotBeNegative() {
        when(record.get(0)).thenReturn("43");
        when(record.get(1)).thenReturn("43");
        when(record.get(2)).thenReturn("-44");

        new TickFacade(record);
    }

    @Test(expected = IllegalArgumentException.class)
    public void askCannotBeNegative() {
        when(record.get(0)).thenReturn("43");
        when(record.get(1)).thenReturn("-43");
        when(record.get(2)).thenReturn("-44");

        new TickFacade(record);
    }

    @Test
    public void trimsBidAndAsk() {
        when(record.get(0)).thenReturn("43");
        when(record.get(1)).thenReturn("43.01  ");
        when(record.get(2)).thenReturn("43.00  ");

        new TickFacade(record);
    }

    @Test(expected = NumberFormatException.class)
    public void timestampMustBeNumerical() {
        when(record.get(0)).thenReturn("43GH");

        new TickFacade(record);
    }

    @Test
    public void timestampCanHandleSpaces() {
        when(record.get(0)).thenReturn("43 45");
        when(record.get(1)).thenReturn("43");
        when(record.get(2)).thenReturn("42");

        new TickFacade(record);
    }

    @Test
    public void gettersWiredCorrectly() {
        long timestamp = 3456;

        when(record.get(0)).thenReturn(Long.toString(timestamp));
        when(record.get(1)).thenReturn(BigDecimal.TEN.toString());
        when(record.get(2)).thenReturn(BigDecimal.ONE.toString());

        TickFacade tick = new TickFacade(record);

        assertEquals(timestamp, tick.getTimestamp());
        assertEquals(BigDecimal.TEN, tick.getAsk());
        assertEquals(BigDecimal.ONE, tick.getBid());
        assertEquals(new BigDecimal("5.5000"), tick.getMid());
    }

}
