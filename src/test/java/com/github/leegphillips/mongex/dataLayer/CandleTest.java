package com.github.leegphillips.mongex.dataLayer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@SuppressWarnings("ConstantConditions")
@RunWith(MockitoJUnitRunner.class)
public class CandleTest {

    CurrencyPair pair = new CurrencyPair("AUDJPY");

    @Mock
    Tick tick1;

    @Mock
    Tick tick2;

    @Mock
    Tick tick3;

    List<Tick> batch = asList(tick1, tick2, tick3);

    @Test(expected = NullPointerException.class)
    public void cannotCreatWithNullTicks() {
        Candle.create(null, pair, TimeFrame.FIVE_MINUTES, LocalDateTime.now(), emptyList());
    }

    @Test(expected = NullPointerException.class)
    public void cannotCreatWithNullPair() {
        Candle.create(batch, null, TimeFrame.FIVE_MINUTES, LocalDateTime.now(), emptyList());
    }

    @Test(expected = NullPointerException.class)
    public void cannotCreatWithNullSize() {
        Candle.create(batch, pair, null, LocalDateTime.now(), emptyList());
    }

    @Test(expected = NullPointerException.class)
    public void cannotCreatWithNullBatchCeiling() {
        Candle.create(batch, pair, TimeFrame.FIVE_MINUTES, null, emptyList());
    }

    @Test(expected = IllegalStateException.class)
    public void cannotCreateEmptyCandle() {
        Candle.create(emptyList(), pair, TimeFrame.FIVE_MINUTES, LocalDateTime.now(), emptyList());
    }

    @Test(expected = IllegalStateException.class)
    public void tickCannotBeAfterCandle() {
        LocalDateTime time = LocalDateTime.now();
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, BigDecimal.ONE, time.plusHours(1));
        Candle.create(singletonList(tick1), pair, duration, time, emptyList());
    }

    @Test
    public void singleTickCandle() {
        LocalDateTime time = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, time);
        Candle candle = Candle.create(singletonList(tick1), pair, duration, time, emptyList());
//        verifyCandle(candle, duration, pair, time, mid, mid, mid, mid, 1, 0, 0, 0);
    }

    @Test
    public void singleTickCandleWithError() {
        LocalDateTime time = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, time);
        when(tick1.isError()).thenReturn(true);
        Candle candle = Candle.create(singletonList(tick1), pair, duration, time, emptyList());
//        verifyCandle(candle, duration, pair, time, mid, mid, mid, mid, 1, 0, 1, 0);
    }

    @Test
    public void doubleTickCandleRising() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        LocalDateTime later = base.plusHours(1);
        BigDecimal up = mid.add(BigDecimal.ONE);
        behave(tick2, up, later);
        Candle candle = Candle.create(asList(tick1, tick2), pair, duration, later, emptyList());
//        verifyCandle(candle, duration, pair, later, mid, up, mid, up, 2, 0, 0, 0);
    }

    @Test
    public void doubleTickCandleRisingWithError() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        when(tick1.isError()).thenReturn(true);
        LocalDateTime later = base.plusHours(1);
        BigDecimal up = mid.add(BigDecimal.ONE);
        behave(tick2, up, later);
        when(tick2.isError()).thenReturn(true);
        Candle candle = Candle.create(asList(tick1, tick2), pair, duration, later, emptyList());
//        verifyCandle(candle, duration, pair, later, mid, up, mid, up, 2, 0, 2, 0);
    }

    @Test
    public void doubleTickCandleFalling() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        LocalDateTime later = base.plusHours(1);
        BigDecimal down = mid.subtract(new BigDecimal("0.5"));
        behave(tick2, down, later);
        Candle candle = Candle.create(asList(tick1, tick2), pair, duration, later, emptyList());
//        verifyCandle(candle, duration, pair, later, mid, mid, down, down, 2, 0, 0, 0);
    }

    @Test
    public void tripleTickCandleStraightLine() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        LocalDateTime later = base.plusHours(1);
        behave(tick2, mid, later);
        LocalDateTime evenLater = later.plusHours(1);
        behave(tick3, mid, evenLater);
        Candle candle = Candle.create(asList(tick1, tick2, tick3), pair, duration, evenLater, emptyList());
//        verifyCandle(candle, duration, pair, evenLater, mid, mid, mid, mid, 3, 0, 0, 0);
    }

    @Test
    public void tripleTickCandleAscending() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        LocalDateTime later = base.plusHours(1);
        BigDecimal mid2 = mid.add(BigDecimal.ONE);
        behave(tick2, mid2, later);
        LocalDateTime evenLater = later.plusHours(1);
        BigDecimal mid3 = mid2.add(BigDecimal.ONE);
        behave(tick3, mid3, evenLater);
        Candle candle = Candle.create(asList(tick1, tick2, tick3), pair, duration, evenLater, emptyList());
//        verifyCandle(candle, duration, pair, evenLater, mid, mid3, mid, mid3, 3, 0, 0, 0);
    }

    @Test
    public void tripleTickCandleDescending() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        LocalDateTime later = base.plusHours(1);
        BigDecimal mid2 = mid.subtract(new BigDecimal("0.5"));
        behave(tick2, mid2, later);
        LocalDateTime evenLater = later.plusHours(1);
        BigDecimal mid3 = mid2.subtract(new BigDecimal("0.25"));
        behave(tick3, mid3, evenLater);
        Candle candle = Candle.create(asList(tick1, tick2, tick3), pair, duration, evenLater, emptyList());
//        verifyCandle(candle, duration, pair, evenLater, mid, mid, mid3, mid3, 3, 0, 0, 0);
    }

    @Test
    public void tripleTickCandleMidMax() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        LocalDateTime later = base.plusHours(1);
        BigDecimal mid2 = mid.add(new BigDecimal("0.5"));
        behave(tick2, mid2, later);
        LocalDateTime evenLater = later.plusHours(1);
        BigDecimal mid3 = mid.add(new BigDecimal("0.25"));
        behave(tick3, mid3, evenLater);
        Candle candle = Candle.create(asList(tick1, tick2, tick3), pair, duration, evenLater, emptyList());
//        verifyCandle(candle, duration, pair, evenLater, mid, mid2, mid, mid3, 3, 0, 0, 0);
    }

    @Test
    public void tripleTickCandleMidMin() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        LocalDateTime later = base.plusHours(1);
        BigDecimal mid2 = mid.subtract(new BigDecimal("0.5"));
        behave(tick2, mid2, later);
        LocalDateTime evenLater = later.plusHours(1);
        BigDecimal mid3 = mid.add(new BigDecimal("0.25"));
        behave(tick3, mid3, evenLater);
        Candle candle = Candle.create(asList(tick1, tick2, tick3), pair, duration, evenLater, emptyList());
//        verifyCandle(candle, duration, pair, evenLater, mid, mid3, mid2, mid3, 3, 0, 0, 0);
    }

    @Test
    public void tripleTickCandleMidMinWithTwoErrors() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        when(tick1.isError()).thenReturn(true);
        LocalDateTime later = base.plusHours(1);
        BigDecimal mid2 = mid.subtract(new BigDecimal("0.5"));
        behave(tick2, mid2, later);
        LocalDateTime evenLater = later.plusHours(1);
        BigDecimal mid3 = mid.add(new BigDecimal("0.25"));
        behave(tick3, mid3, evenLater);
        when(tick3.isError()).thenReturn(true);
        Candle candle = Candle.create(asList(tick1, tick2, tick3), pair, duration, evenLater, emptyList());
//        verifyCandle(candle, duration, pair, evenLater, mid, mid3, mid2, mid3, 3, 0, 2, 0);
    }

    @Test
    public void tripleTickCandleWithTwoInversions() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        when(tick1.isInverted()).thenReturn(true);
        LocalDateTime later = base.plusHours(1);
        BigDecimal mid2 = mid.subtract(new BigDecimal("0.5"));
        behave(tick2, mid2, later);
        LocalDateTime evenLater = later.plusHours(1);
        BigDecimal mid3 = mid.add(new BigDecimal("0.25"));
        behave(tick3, mid3, evenLater);
        when(tick3.isInverted()).thenReturn(true);
        Candle candle = Candle.create(asList(tick1, tick2, tick3), pair, duration, evenLater, emptyList());
//        verifyCandle(candle, duration, pair, evenLater, mid, mid3, mid2, mid3, 3, 0, 0, 2);
    }

    @Test
    public void tripleAllSameTimestamp() {
        LocalDateTime base = LocalDateTime.now();
        BigDecimal mid = BigDecimal.ONE;
        TimeFrame duration = TimeFrame.FIVE_MINUTES;
        behave(tick1, mid, base);
        BigDecimal mid2 = mid.subtract(new BigDecimal("0.5"));
        behave(tick2, mid2, base);
        BigDecimal mid3 = mid.add(new BigDecimal("0.25"));
        behave(tick3, mid3, base);
        Candle candle = Candle.create(asList(tick1, tick2, tick3), pair, duration, base, emptyList());
//        verifyCandle(candle, duration, pair, base, mid, mid3, mid2, mid, 3, 2, 0, 0);
    }

    private void behave(Tick tick, BigDecimal mid, LocalDateTime time) {
        when(tick.getMid()).thenReturn(mid);
        when(tick.getTimestamp()).thenReturn(time);
        when(tick.isError()).thenReturn(false);
    }

    private void verifyCandle(Candle candle, TimeFrame duration, CurrencyPair pair, LocalDateTime timestamp,
                              BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, int tickCount,
                              long duplicates, int errorCount, int inversionCount) {
        assertEquals(duration, candle.getTimeFrame());
        assertEquals(pair, candle.getPair());
        assertEquals(timestamp, candle.getTimestamp());
        assertEquals(open, candle.getOpen());
        assertEquals(high, candle.getHigh());
        assertEquals(low, candle.getLow());
        assertEquals(close, candle.getClose());
        assertEquals(tickCount, candle.getTickCount());
        assertEquals(duplicates, candle.getDuplicatesCount());
        assertEquals(errorCount, candle.getErrorCount());
        assertEquals(inversionCount, candle.getInversionCount());
    }
}
