package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.dao.Classification;
import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map;

import static com.github.leegphillips.mongex.dataLayer.utils.Constants.END;

public class Classifier extends WrappedBlockingQueue<Classification> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Classifier.class);

    private final CurrencyPair pair;
    private final WrappedBlockingQueue<Map<CurrencyPair, State>> input;

    public Classifier(CurrencyPair pair, WrappedBlockingQueue<Map<CurrencyPair, State>> input) {
        this.pair = pair;
        this.input = input;
    }

    @Override
    public void run() {
        Map<CurrencyPair, State> current = input.take();
        Map<CurrencyPair, State> next = input.take();
        int i = 0;
        while (next != END) {
            BigDecimal currentValue = current.get(pair).getValues().get(1);
            if (!currentValue.equals(BigDecimal.ZERO)) {

                BigDecimal nextValue = next.get(pair).getValues().get(1);

                int diff = currentValue.compareTo(nextValue);
                if (diff != 0) {
                    put(new Classification(diff < 0, new ArrayList<>(current.values())));
                } else {
                    LOG.trace("Not different " + i + " " + current.get(pair).getTimestamp());
                }
                LOG.trace(current.get(pair).getTimestamp() + " " + currentValue.toPlainString() + " " + nextValue.toPlainString());
            }
            current = next;
            next = input.take();
            i++;
        }
        put(Classification.CLOSE);
    }
}

