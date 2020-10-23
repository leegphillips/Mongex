package com.github.leegphillips.mongex.dataLayer;

import java.io.Closeable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class TickMarketIterable implements Iterable<Tick>, Closeable {

    private final List<TickHistoryIterable> histories;
    private final Map<CurrencyPair, Iterator<Tick>> iterators;
    private final List<Tick> nexts;

    public TickMarketIterable() {
        histories = Utils.getAllCurrencies()
                        .map(TickHistoryIterable::new)
                        .collect(toList());
        iterators = histories.stream()
                        .collect(toMap(TickHistoryIterable::getPair, TickHistoryIterable::iterator));
        nexts = iterators.values()
                        .stream()
                        .filter(Iterator::hasNext)
                        .map(Iterator::next)
                        .collect(toList());
    }

    @Override
    public void close() {
        histories.stream().forEach(TickHistoryIterable::close);
    }

    @Override
    public Iterator<Tick> iterator() {
        return new Iterator<Tick>() {
            @Override
            public boolean hasNext() {
                return nexts.size() > 0;
            }

            @Override
            public Tick next() {
                Tick next = nexts.stream()
                                .sorted(Comparator.comparing(Tick::getTimestamp))
                                .findFirst()
                                .orElseThrow(IllegalStateException::new);

                nexts.remove(next);

                Iterator<Tick> iterator = iterators.entrySet().stream()
                        .filter(entry -> entry.getKey().equals(next.getPair()))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new)
                        .getValue();

                if (iterator.hasNext())
                    nexts.add(iterator.next());

                return next;
            }
        };
    }

    public static void main(String[] args) {
        new TickMarketIterable().iterator().forEachRemaining(tick -> System.out.println(tick));
    }
}
