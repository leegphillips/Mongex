package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.ma.MovingAverage;
import com.github.leegphillips.mongex.dataLayer.ma.SimpleMovingAverage;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class StreamState {
    private static final int[] MA_SIZES = {1, 2, 8, 34, 144, 610, 2584};

    private final CurrencyPair pair;
    private final List<SimpleMovingAverage> movingAverages;

    public StreamState(CurrencyPair pair) {
        this.pair = pair;
        this.movingAverages = stream(MA_SIZES).mapToObj(SimpleMovingAverage::new).collect(toList());
    }

    public CurrencyPair getPair() {
        return pair;
    }

    public void update(Delta delta) {
        movingAverages.stream().forEach(ma -> ma.add(delta.getValue()));
    }

    @Override
    public String toString() {
        return "{" +
                pair.getLabel() +
                ", " + movingAverages +
                '}';
    }

    public Map<Integer, BigDecimal> getSnapshot() {
        return movingAverages.stream().collect(toMap(MovingAverage::getSize, MovingAverage::getValue));
    }
}
