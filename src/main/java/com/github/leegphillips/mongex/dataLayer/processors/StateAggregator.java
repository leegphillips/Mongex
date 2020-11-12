package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.leegphillips.mongex.dataLayer.utils.Constants.CLOSE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class StateAggregator extends WrappedBlockingQueue<List<State>> implements Runnable {

    private final Map<CurrencyPair, WrappedBlockingQueue<State>> inputs;

    public StateAggregator(Map<CurrencyPair, WrappedBlockingQueue<State>> inputs) {
        this.inputs = new HashMap<>(inputs);
    }

    @Override
    public void run() {
        Map<CurrencyPair, State> nexts = inputs.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().take()));

        while (nexts.size() > 0) {
            State first = nexts.values().stream()
                    .min(Comparator.comparing(State::getTimestamp))
                    .orElseThrow(IllegalStateException::new);

            List<State> same = nexts.values().stream()
                    .filter(state -> state.getTimestamp().isEqual(first.getTimestamp()))
                    .collect(toList());

            same.stream()
                    .filter(state -> nexts.containsKey(state.getPair()))
                    .forEach(state -> nexts.put(state.getPair(), inputs.get(state.getPair()).take()));

            put(same);

            // moved from start to end
            List<CurrencyPair> finished = nexts.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(State.END))
                    .map(Map.Entry::getKey)
                    .collect(toList());

            finished.forEach(inputs::remove);
            finished.forEach(nexts::remove);
        }
        put(CLOSE);
    }
}
