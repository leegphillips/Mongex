package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.ma.MALoader;

public class App {
    public static void main(String[] args) {
        CandleLoader.main(args);
        FullCandleSeriesChecker.main(args);
        MALoader.main(args);
    }
}
