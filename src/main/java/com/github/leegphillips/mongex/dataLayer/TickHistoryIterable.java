package com.github.leegphillips.mongex.dataLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static java.util.stream.Collectors.toList;

public class TickHistoryIterable extends ArrayBlockingQueue<Tick> {
    private final static Logger LOG = LoggerFactory.getLogger(TickHistoryIterable.class);

    private final static int SIZE = 256;

    private final CurrencyPair pair;

    public TickHistoryIterable(CurrencyPair pair) {
        super(SIZE);
        this.pair = pair;
        new Thread(new Worker(), getClass().getSimpleName() + ":" + pair.getLabel()).start();
    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            try {
                List<TickFileReader> readers = Arrays.stream(Utils.getFiles())
                        .filter(file -> file.getName().contains(pair.getLabel()))
                        .sorted(Comparator.naturalOrder())
                        .map(TickFileReader::new)
                        .collect(toList());

                for (TickFileReader reader : readers) {
                    Tick tick = reader.readTick();
                    while (tick != null) {
                        LOG.trace(tick.toString());
                        put(tick);
                        tick = reader.readTick();
                    }
                    reader.close();
                }
                put(Tick.POISON);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        TickHistoryIterable eurusd = new TickHistoryIterable(new CurrencyPair("EURUSD"));
        Tick tick = eurusd.take();
        while (tick != Tick.POISON) {
            tick = eurusd.take();
        }
    }

    public CurrencyPair getPair() {
        return pair;
    }
}
