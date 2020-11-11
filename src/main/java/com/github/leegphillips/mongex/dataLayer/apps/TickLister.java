package com.github.leegphillips.mongex.dataLayer.apps;

import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.dao.Tick;
import com.github.leegphillips.mongex.dataLayer.dao.TimeFrame;
import com.github.leegphillips.mongex.dataLayer.processors.TickReader;
import com.github.leegphillips.mongex.dataLayer.processors.TickTimeFrameFilter;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TickLister implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TickLister.class);

    private static final ExecutorService SERVICE = Executors.newCachedThreadPool();

    private final CurrencyPair pair;
    private final TimeFrame tf;

    public static void main(String[] args) {
        CurrencyPair pair = CurrencyPair.get(args[0]);
        TimeFrame tf = TimeFrame.get(args[1]);

        new TickLister(pair, tf).run();
    }

    public TickLister(CurrencyPair pair, TimeFrame tf) {
        this.pair = pair;
        this.tf = tf;
    }

    @Override
    public void run() {
        TickReader reader = new TickReader(pair);
        SERVICE.execute(reader);

        TickTimeFrameFilter filter = new TickTimeFrameFilter(tf, reader);
        SERVICE.execute(filter);

        SERVICE.execute(new Output(filter));
    }

    private class Output implements Runnable {

        private final WrappedBlockingQueue<Tick> input;

        private Output(WrappedBlockingQueue<Tick> input) {
            this.input = input;
        }

        @Override
        public void run() {
            Tick tick = input.take();
            while (tick != Tick.POISON) {
                LOG.info(tick.toString());
                tick = input.take();
            }
            SERVICE.shutdown();
        }
    }
}
