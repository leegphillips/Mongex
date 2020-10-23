package com.github.leegphillips.mongex.dataLayer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public class TickHistoryIterable implements Iterable<Tick>, Closeable {
    private static final Properties PROPERTIES = PropertiesSingleton.getInstance();

    private static final File[] FILES = new File(PROPERTIES.getProperty(PropertiesSingleton.SOURCE_DIR)).listFiles();

    private final CurrencyPair pair;
    private final List<TickFileReader> readers;
    private final List<Iterator<Tick>> iterators;

    public TickHistoryIterable(CurrencyPair pair) {
        this.pair = pair;
        this.readers = stream(FILES)
                            .filter(file -> file.getName().contains(pair.getLabel()))
                            .sorted(Comparator.naturalOrder())
                            .map(TickFileReader::new)
                            .collect(toList());
        this.iterators = readers.stream()
                            .map(TickFileReader::iterator)
                            .collect(toList());
    }

    @Override
    public Iterator<Tick> iterator() {
        return new Iterator<Tick>() {
            @Override
            public boolean hasNext() {
                return iterators.stream().anyMatch(iter -> iter.hasNext());
            }

            @Override
            public Tick next() {
                return iterators.stream()
                        .filter(iter -> iter.hasNext())
                        .findFirst()
                        .orElseThrow(IllegalStateException::new)
                        .next();
            }
        };
    }

    public static void main(String[] args) {
        new TickHistoryIterable(Utils.getAllCurrencies().findFirst().get())
                .iterator()
                .forEachRemaining(tick -> System.out.println(tick));
    }

    @Override
    public void close() {
        readers.stream().forEach(TickFileReader::close);
    }

    public CurrencyPair getPair() {
        return pair;
    }
}
