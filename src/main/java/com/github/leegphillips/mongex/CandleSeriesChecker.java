package com.github.leegphillips.mongex;

import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.atomic.AtomicInteger;

public class CandleSeriesChecker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CandleSeriesChecker.class);

    private final FindIterable<Document> pairSeries;
    private final TimeFrame timeframe;
    private final AtomicInteger counter;

    public CandleSeriesChecker(FindIterable<Document> pairSeries, TimeFrame timeframe, AtomicInteger counter) {
        this.pairSeries = pairSeries;
        this.timeframe = timeframe;
        this.counter = counter;
    }

    @Override
    public void run() {
        Candle prev = null;
        for (Document doc : pairSeries) {
            Candle current = Candle.create(doc);
            if (prev != null && !current.getTimestamp().isEqual(timeframe.next(prev.getTimestamp()))) {
                MDC.put("prev", prev.toString());
                MDC.put("current", current.toString());
                LOG.error("Illegal gap");
                break;
            }
            System.out.println(current);
            prev = current;
        }
        counter.decrementAndGet();
    }
}
