package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.Tick;
import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.ma.MovingAverage;
import com.github.leegphillips.mongex.dataLayer.ma.SimpleMovingAverage;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static com.github.leegphillips.mongex.dataLayer.dao.State.END;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class TickMATracker extends WrappedBlockingQueue<State> implements Runnable {
    private static final int QUEUE_SIZE = 4096;

    public static final int[] MA_SIZES = new int[]{1, 2, 8, 34, 144, 610, 2584};

    private final BlockingQueue<Tick> input;
    private final List<SimpleMovingAverage> sMAs = Arrays.stream(MA_SIZES).mapToObj(SimpleMovingAverage::new).collect(toList());

    public TickMATracker(BlockingQueue<Tick> input) {
        super(QUEUE_SIZE);
        this.input = input;
    }

    @Override
    public void run() {
        try {
            Tick tick = input.take();
            while (tick != Tick.POISON) {
                BigDecimal mid = tick.getMid();
                sMAs.parallelStream().forEach(ma -> ma.add(mid));
                put(new State(tick.getPair(), tick.getTimestamp(), sMAs.parallelStream().collect(toMap(MovingAverage::getSize, SimpleMovingAverage::getValue))));
                tick = input.take();
            }
            put(END);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
