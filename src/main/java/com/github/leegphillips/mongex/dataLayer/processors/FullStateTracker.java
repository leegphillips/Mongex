package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.utils.Utils;
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

    private final String pairMatch;
    private final WrappedBlockingQueue<List<State>> input;

    public FullStateTracker(String pairMatch, WrappedBlockingQueue<List<State>> input) {
        this.pairMatch = pairMatch;
        this.input = input;
    }

    @Override
    public void run() {
        Map<CurrencyPair, State> full = Utils.getAllCurrencies()
                .filter(pair -> pairMatch == null ? true : pair.getLabel().contains(pairMatch))
                .collect(toMap(pair -> pair, pair -> new State(pair, LocalDateTime.MIN, Arrays.stream(MA_SIZES)
                        .boxed()
                        .collect(toMap(entry -> entry, entry -> BigDecimal.ZERO, (o1, o2) -> o1, TreeMap::new))), (o1, o2) -> o1, TreeMap::new));

        List<State> update = input.take();
        while (update != CLOSE) {
            update.forEach(state -> full.replace(state.getPair(), state));

            put(new TreeMap<>(full));

            update = input.take();
        }
        put(END);
    }
}
