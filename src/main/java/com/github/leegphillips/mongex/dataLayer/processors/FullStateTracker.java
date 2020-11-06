package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.Utils;
import com.github.leegphillips.mongex.dataLayer.dao.State;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.github.leegphillips.mongex.dataLayer.processors.TickMATracker.MA_SIZES;
import static com.github.leegphillips.mongex.dataLayer.utils.Constants.CLOSE;
import static com.github.leegphillips.mongex.dataLayer.utils.Constants.END;
import static java.util.stream.Collectors.toMap;

public class FullStateTracker extends ArrayBlockingQueue<Map<CurrencyPair, State>> implements Runnable {
    private static final int QUEUE_SIZE = 4096;

    private final BlockingQueue<List<State>> input;

    public FullStateTracker(BlockingQueue<List<State>> input) {
        super(QUEUE_SIZE);
        this.input = input;
    }

    @Override
    public void run() {
        Map<CurrencyPair, State> full = Utils.getAllCurrencies()
                .collect(toMap(pair -> pair, pair -> new State(pair, LocalDateTime.MIN, Arrays.stream(MA_SIZES)
                        .boxed()
                        .collect(toMap(entry -> entry, entry -> BigDecimal.ZERO))), (o1, o2) -> o1, TreeMap::new));
        try {
            List<State> update = input.take();
            while (update != CLOSE) {
                update.forEach(state -> full.replace(state.getPair(), state));

                put(new TreeMap<>(full));

                update = input.take();
            }
            put(END);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
