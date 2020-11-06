package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import static com.github.leegphillips.mongex.dataLayer.utils.Constants.CLOSE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class StateAggregator extends ArrayBlockingQueue<List<State>> implements Runnable {

    private static final int QUEUE_SIZE = 4096;

    private final Map<CurrencyPair, WrappedBlockingQueue<State>> inputs;

    public StateAggregator(Map<CurrencyPair, WrappedBlockingQueue<State>> inputs) {
        super(QUEUE_SIZE);
        this.inputs = new HashMap<>(inputs);
    }

    @Override
    public void run() {
        try {
            Map<CurrencyPair, State> nexts = inputs.entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().take()));

            while (nexts.size() > 0) {
                List<CurrencyPair> finished = nexts.entrySet().stream()
                        .filter(entry -> entry.getValue().equals(State.END))
                        .map(Map.Entry::getKey)
                        .collect(toList());

                finished.forEach(inputs::remove);
                finished.forEach(nexts::remove);

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
            }
            put(CLOSE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
