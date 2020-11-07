package com.github.leegphillips.mongex.dataLayer.utils;

import com.github.leegphillips.mongex.dataLayer.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.dao.State;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Constants {
    public static final int[] MA_SIZES = new int[]{1, 2, 8, 34, 144, 610, 2584};

    public static final int QUEUE_SIZE = 4096;

    public static final List<State> CLOSE = new ArrayList<>();
    public static final Map<CurrencyPair, State> END = Collections.EMPTY_MAP;

}
