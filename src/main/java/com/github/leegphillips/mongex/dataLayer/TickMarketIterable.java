package com.github.leegphillips.mongex.dataLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Function;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toMap;

public class TickMarketIterable extends ArrayBlockingQueue<Tick> {
    private final static Logger LOG = LoggerFactory.getLogger(TickMarketIterable.class);

    private final static int SIZE = 256;

    public TickMarketIterable() {
        super(SIZE);
        new Thread(new Worker(), getClass().getSimpleName()).start();
    }

    public static void main(String[] args) throws InterruptedException {
        TickMarketIterable ticks = new TickMarketIterable();
        Tick tick = ticks.take();
        while (tick != Tick.POISON) {
            tick = ticks.take();
        }
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            try {
                Map<CurrencyPair, TickHistoryIterable> histories = Utils.getAllCurrencies()
                        .map(TickHistoryIterable::new)
                        .collect(toMap(TickHistoryIterable::getPair, Function.identity()));

                List<Tick> nexts = new ArrayList<>();
                for (TickHistoryIterable history : histories.values()) {
                    nexts.add(history.take());
                }

                while (nexts.size() > 0) {
                    Tick next = nexts.stream()
                            .sorted(comparing(Tick::getTimestamp))
                            .findFirst()
                            .orElseThrow(IllegalStateException::new);

                    nexts.remove(next);

                    TickHistoryIterable ticks = histories.get(next.getPair());
                    Tick replacement = ticks.take();
                    if (replacement == Tick.POISON) {
                        histories.remove(ticks);
                    } else {
                        nexts.add(replacement);
                    }

                    LOG.trace(next.toString());
                    put(next);
                }

                put(Tick.POISON);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
