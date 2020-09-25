package org.github.leegphillips.mongex;

import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CandleFactoryTest {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmssSSS");
    CandleFactory factory = new CandleFactory();

    @Mock
    CSVRecord record1;

    @Mock
    CSVRecord record2;

    @Mock
    CSVRecord record3;

    String pair = "AUDJPY";
    String tickSize = "5M";

    @Test(expected = NullPointerException.class)
    public void nullRecordsThrows() {
        factory.create(null, pair, tickSize, LocalDateTime.now());
    }

    @Test(expected = NullPointerException.class)
    public void nullPairThrows() {
        factory.create(singletonList(record1), null, tickSize, LocalDateTime.now());
    }

    @Test(expected = NullPointerException.class)
    public void nullTickSizeThrows() {
        factory.create(singletonList(record1), pair, null, LocalDateTime.now());
    }

    @Test(expected = IllegalArgumentException.class)
    public void recordsCannotBeEmpty() {
        factory.create(EMPTY_LIST, pair, tickSize, LocalDateTime.now());
    }

    @Test(expected = IllegalArgumentException.class)
    public void pairCannotBeEmpty() {
        factory.create(singletonList(record1), "", tickSize, LocalDateTime.now());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tickSizeCannotBeEmpty() {
        factory.create(singletonList(record1), pair, "", LocalDateTime.now());
    }

    @Test
    public void tickCannotBeAfterCandleCeiling() {

    }

    @Test
    public void singleTickCandle() {
        String timestamp = "20071202 170024000";
        when(record1.get(0)).thenReturn(timestamp);
        when(record1.get(1)).thenReturn(BigDecimal.TEN.toString());
        when(record1.get(2)).thenReturn(BigDecimal.ONE.toString());
        Candle candle = factory.create(singletonList(record1), pair, tickSize, LocalDateTime.now());
        System.out.println();
    }


    @Test
    public void twoTickCandle() {
        String timestamp = "20071202 170024000";
        when(record1.get(0)).thenReturn(timestamp);
        when(record1.get(1)).thenReturn(BigDecimal.TEN.toString());
        when(record1.get(2)).thenReturn(BigDecimal.ONE.toString());
        String timestamp2 = "20071202 170025000";
        when(record2.get(0)).thenReturn(timestamp2);
        when(record2.get(1)).thenReturn(BigDecimal.TEN.add(BigDecimal.ONE).toString());
        when(record2.get(2)).thenReturn(BigDecimal.ONE.add(BigDecimal.ONE).toString());
        Candle candle = factory.create(Arrays.asList(record1, record2), pair, tickSize, LocalDateTime.now());
        System.out.println();
    }

    private void behave(CSVRecord record, String timestamp, BigDecimal ask, BigDecimal bid) {
        when(record.get(0)).thenReturn(timestamp);
        when(record.get(1)).thenReturn(ask.toString());
        when(record.get(2)).thenReturn(ask.toString());
    }
}
