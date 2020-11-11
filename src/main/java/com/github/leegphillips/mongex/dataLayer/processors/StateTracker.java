package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.dao.Tick;
import com.github.leegphillips.mongex.dataLayer.ma.MovingAverage;
import com.github.leegphillips.mongex.dataLayer.ma.SimpleMovingAverage;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.github.leegphillips.mongex.dataLayer.dao.State.END;
import static com.github.leegphillips.mongex.dataLayer.utils.Constants.MA_SIZES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class StateTracker extends WrappedBlockingQueue<State> implements Runnable {
    private final WrappedBlockingQueue<Tick> input;
    private final List<SimpleMovingAverage> sMAs = Arrays.stream(MA_SIZES).mapToObj(SimpleMovingAverage::new).collect(toList());

    public StateTracker(WrappedBlockingQueue<Tick> input) {
        this.input = input;
    }

    @Override
    public void run() {
        Tick tick = input.take();
        while (tick != Tick.POISON) {
            BigDecimal mid = tick.getMid();
            sMAs.parallelStream().forEach(ma -> ma.add(mid));
            Map<Integer, BigDecimal> values = sMAs.parallelStream().collect(toMap(MovingAverage::getSize, SimpleMovingAverage::getValue, (o1, o2) -> o1, TreeMap::new));
            put(new State(tick.getPair(), tick.getTimestamp(), values));
            tick = input.take();
        }
        put(END);
    }
}
