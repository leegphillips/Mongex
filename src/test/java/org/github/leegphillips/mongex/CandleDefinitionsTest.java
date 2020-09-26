package org.github.leegphillips.mongex;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class CandleDefinitionsTest {

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");

    @Test
    public void all() {
        assertEquals(CandleDefinitions.FIVE_MINUTES.getFloor(LocalDateTime.parse("20200924 123100", formatter)),
                LocalDateTime.parse("20200924 123000", formatter));

        assertEquals(CandleDefinitions.FIVE_MINUTES.getFloor(LocalDateTime.parse("20200924 123000", formatter)),
                LocalDateTime.parse("20200924 123000", formatter));

        assertEquals(CandleDefinitions.FIVE_MINUTES.getFloor(LocalDateTime.parse("20200924 123005", formatter)),
                LocalDateTime.parse("20200924 123000", formatter));

        assertEquals(CandleDefinitions.FIVE_MINUTES.getFloor(LocalDateTime.parse("20200924 123605", formatter)),
                LocalDateTime.parse("20200924 123500", formatter));

        assertEquals(CandleDefinitions.FIVE_MINUTES.getFloor(LocalDateTime.parse("20200924 133605", formatter)),
                LocalDateTime.parse("20200924 133500", formatter));
    }
}
