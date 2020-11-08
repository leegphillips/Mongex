package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.dao.Classification;
import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map;

import static com.github.leegphillips.mongex.dataLayer.utils.Constants.CLOSE;

public class Classifier extends WrappedBlockingQueue<Classification> implements Runnable {

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
        while (next != CLOSE) {
            BigDecimal currentValue = current.get(pair).getValues().get(1);
            if (!currentValue.equals(BigDecimal.ZERO)) {

                BigDecimal nextValue = next.get(pair).getValues().get(1);

                // TODO only publish if there is a change
                put(new Classification(currentValue.compareTo(nextValue) < 0, new ArrayList<>(current.values())));
            }
            current = next;
            next = input.take();
        }
        put(Classification.CLOSE);
    }
}

