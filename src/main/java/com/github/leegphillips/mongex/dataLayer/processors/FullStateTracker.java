package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.Utils;
import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.github.leegphillips.mongex.dataLayer.utils.Constants.*;
import static java.util.stream.Collectors.toMap;

public class FullStateTracker extends WrappedBlockingQueue<Map<CurrencyPair, State>> implements Runnable {

    private final WrappedBlockingQueue<List<State>> input;

    public FullStateTracker(WrappedBlockingQueue<List<State>> input) {
        this.input = input;
    }

    @Override
    public void run() {
        Map<CurrencyPair, State> full = Utils.getAllCurrencies()
                .collect(toMap(pair -> pair, pair -> new State(pair, LocalDateTime.MIN, Arrays.stream(MA_SIZES)
                        .boxed()
                        .collect(toMap(entry -> entry, entry -> BigDecimal.ZERO))), (o1, o2) -> o1, TreeMap::new));
        List<State> update = input.take();
        while (update != CLOSE) {
            update.forEach(state -> full.replace(state.getPair(), state));

            put(new TreeMap<>(full));

            update = input.take();
        }
        put(END);
    }
}
