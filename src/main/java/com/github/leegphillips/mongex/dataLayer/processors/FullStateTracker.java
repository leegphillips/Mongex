package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.utils.Utils;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.github.leegphillips.mongex.dataLayer.dao.State.UNSTARTED;
import static com.github.leegphillips.mongex.dataLayer.utils.Constants.CLOSE;
import static com.github.leegphillips.mongex.dataLayer.utils.Constants.END;
import static java.util.stream.Collectors.toMap;

public class FullStateTracker extends WrappedBlockingQueue<Map<CurrencyPair, State>> implements Runnable {

    private final WrappedBlockingQueue<List<State>> input;

    public FullStateTracker(WrappedBlockingQueue<List<State>> input) {
        this.input = input;
    }

    @Override
    public void run() {
        Map<CurrencyPair, State> full = Utils.getAllCurrencies()
                .collect(toMap(pair -> pair, pair -> UNSTARTED, (o1, o2) -> o1, TreeMap::new));

        List<State> update = input.take();
        while (update != CLOSE) {
            update.forEach(state -> full.replace(state.getPair(), state));

            put(new TreeMap<>(full));

            update = input.take();
        }
        put(END);
    }
}
